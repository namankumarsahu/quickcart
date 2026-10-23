// Package orders creates orders atomically and enforces the order lifecycle.
package orders

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"

	"github.com/namankumarsahu/quickcart/internal/inventory"
)

var (
	ErrInvalidRequest    = errors.New("orders: invalid request")
	ErrInvalidKey        = errors.New("orders: idempotency key must be 1-255 characters")
	ErrKeyReused         = errors.New("orders: idempotency key reused with a different request")
	ErrIllegalTransition = errors.New("orders: illegal status transition")
	ErrStaleTransition   = errors.New("orders: order is no longer in the expected status")
	ErrOrderNotFound     = errors.New("orders: order not found")
)

const (
	maxItemsPerOrder   = 50
	maxQuantityPerItem = 50
	maxKeyLength       = 255
	maxIDLength        = 64
	responseCreated    = 201 // stored with the response, mirrors HTTP "Created"
)

// dbtx is what the helpers need; both a pool and a transaction satisfy it.
type dbtx interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// DB is what Service needs: single statements plus the ability to open a
// transaction. *pgxpool.Pool satisfies it.
type DB interface {
	Begin(ctx context.Context) (pgx.Tx, error)
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Item is one order line. There is deliberately NO price field: prices are
// never accepted from the client.
type Item struct {
	SKU      string `json:"sku"`
	Quantity int    `json:"quantity"`
}

// CreateOrderRequest is everything a client may send. There is no status and
// no price: the server decides both.
type CreateOrderRequest struct {
	StoreID string
	Items   []Item
}

// CreateResult is what CreateOrder returns, and what is stored for replays.
type CreateResult struct {
	OrderID    string
	StatusCode int    // stored response status (201 for a created order)
	Body       []byte // stored response body (JSON)
	Replayed   bool   // true when this came from an earlier identical request
}

type orderView struct {
	OrderID string `json:"order_id"`
	StoreID string `json:"store_id"`
	Status  Status `json:"status"`
	Items   []Item `json:"items"`
}

// Service creates orders and applies status transitions.
type Service struct {
	db DB
}

// NewService returns a Service backed by db.
func NewService(db DB) *Service { return &Service{db: db} }

const (
	// Claim the idempotency key FIRST, inside the order's own transaction.
	// If another transaction is inserting the same key right now, this
	// statement WAITS for it to commit or roll back, then either inserts
	// (it rolled back) or does nothing (it committed).
	claimKeySQL = `
INSERT INTO idempotency_keys (key, request_hash) VALUES ($1, $2)
ON CONFLICT (key) DO NOTHING`

	lookupKeySQL = `
SELECT request_hash,
       COALESCE(response_status, 0),
       COALESCE(response_body::text, ''),
       COALESCE(order_id::text, '')
  FROM idempotency_keys WHERE key = $1`

	finalizeKeySQL = `
UPDATE idempotency_keys
   SET response_status = $2, response_body = $3, order_id = $4
 WHERE key = $1`
)

// CreateOrder reserves ALL items or none, exactly once per idempotency key.
//
//  1. Validate input (no database yet).
//  2. BEGIN. Claim the key. Zero rows claimed => replay the stored response.
//  3. Insert the order and items, reserve stock in sorted order, mark the
//     order reserved, store the response, COMMIT.
//
// Failures roll back everything, including the key claim, so a failed
// attempt leaves no trace and the client may retry with the same key.
func (s *Service) CreateOrder(ctx context.Context, key string, req CreateOrderRequest) (CreateResult, error) {
	if key == "" || len(key) > maxKeyLength {
		return CreateResult{}, ErrInvalidKey
	}
	norm, err := normalize(req)
	if err != nil {
		return CreateResult{}, err
	}
	hash := requestHash(norm)

	tx, err := s.db.Begin(ctx)
	if err != nil {
		return CreateResult{}, fmt.Errorf("begin create-order transaction: %w", err)
	}
	defer rollbackQuietly(tx)

	claimed, err := tx.Exec(ctx, claimKeySQL, key, hash)
	if err != nil {
		return CreateResult{}, fmt.Errorf("claim idempotency key: %w", err)
	}
	if claimed.RowsAffected() == 0 {
		return replay(ctx, tx, key, hash)
	}

	orderID, body, err := createOrderRows(ctx, tx, norm)
	if err != nil {
		return CreateResult{}, err
	}
	if _, err := tx.Exec(ctx, finalizeKeySQL, key, responseCreated, string(body), orderID); err != nil {
		return CreateResult{}, fmt.Errorf("store idempotent response: %w", err)
	}
	if err := tx.Commit(ctx); err != nil {
		return CreateResult{}, fmt.Errorf("commit create-order transaction: %w", err)
	}
	return CreateResult{OrderID: orderID, StatusCode: responseCreated, Body: body}, nil
}

// createOrderRows does the actual work inside the caller's transaction:
// insert order + items, reserve every item, move the order to "reserved".
func createOrderRows(ctx context.Context, tx dbtx, req CreateOrderRequest) (string, []byte, error) {
	var orderID string
	err := tx.QueryRow(ctx,
		`INSERT INTO orders (store_id) VALUES ($1) RETURNING id::text`, req.StoreID).Scan(&orderID)
	if err != nil {
		return "", nil, fmt.Errorf("insert order: %w", err)
	}
	for _, it := range req.Items {
		if _, err := tx.Exec(ctx,
			`INSERT INTO order_items (order_id, sku, quantity) VALUES ($1, $2, $3)`,
			orderID, it.SKU, it.Quantity); err != nil {
			return "", nil, fmt.Errorf("insert order item %q: %w", it.SKU, err)
		}
	}

	// Each Reserve takes a row lock that is held until COMMIT/ROLLBACK. If two
	// orders lock the same rows in different orders they can wait on each
	// other forever (deadlock). Sorting gives every order the same locking
	// order, so a cycle is impossible.
	inv := inventory.NewStore(tx)
	for _, it := range sortedByLockOrder(req.Items) { // BREAK-IT A: change to req.Items
		if err := inv.Reserve(ctx, req.StoreID, it.SKU, it.Quantity); err != nil {
			return "", nil, fmt.Errorf("reserve %q: %w", it.SKU, err)
		}
	}

	if err := transition(ctx, tx, orderID, StatusCreated, StatusReserved); err != nil {
		return "", nil, err
	}
	body, err := json.Marshal(orderView{
		OrderID: orderID, StoreID: req.StoreID, Status: StatusReserved, Items: req.Items,
	})
	if err != nil {
		return "", nil, fmt.Errorf("encode order response: %w", err)
	}
	return orderID, body, nil
}

// replay returns the stored response for an already-seen key, or ErrKeyReused
// if the new request differs from the one that first used the key.
func replay(ctx context.Context, tx dbtx, key, hash string) (CreateResult, error) {
	var storedHash, body, orderID string
	var status int
	err := tx.QueryRow(ctx, lookupKeySQL, key).Scan(&storedHash, &status, &body, &orderID)
	if err != nil {
		return CreateResult{}, fmt.Errorf("look up idempotency key: %w", err)
	}
	if storedHash != hash {
		return CreateResult{}, ErrKeyReused
	}
	if status == 0 {
		// Cannot happen: a claimed key is only visible after the claiming
		// transaction committed, and that transaction stores the response.
		return CreateResult{}, errors.New("orders: idempotency key has no stored response")
	}
	return CreateResult{OrderID: orderID, StatusCode: status, Body: []byte(body), Replayed: true}, nil
}

// Transition moves an order from -> to. The UPDATE is conditional on the
// current status, so a stale caller (someone else already moved the order)
// changes zero rows and gets ErrStaleTransition instead of overwriting.
func (s *Service) Transition(ctx context.Context, orderID string, from, to Status) error {
	return transition(ctx, s.db, orderID, from, to)
}

func transition(ctx context.Context, db dbtx, orderID string, from, to Status) error {
	if !CanTransition(from, to) {
		return fmt.Errorf("order %s: %s -> %s: %w", orderID, from, to, ErrIllegalTransition)
	}
	tag, err := db.Exec(ctx,
		`UPDATE orders SET status = $3, updated_at = now() WHERE id = $1 AND status = $2`,
		orderID, string(from), string(to))
	if err != nil {
		return fmt.Errorf("order %s: %s -> %s: %w", orderID, from, to, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	var exists bool
	if err := db.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM orders WHERE id = $1)`, orderID).Scan(&exists); err != nil {
		return fmt.Errorf("order %s: check existence: %w", orderID, err)
	}
	if !exists {
		return fmt.Errorf("order %s: %w", orderID, ErrOrderNotFound)
	}
	return fmt.Errorf("order %s: expected %s: %w", orderID, from, ErrStaleTransition)
}

// normalize validates a request and returns a cleaned copy. It never mutates
// the caller's slice (goroutines may share it). Duplicate SKUs are REJECTED,
// not merged: a duplicate usually means a client bug, and silently adding the
// quantities would hide it.
func normalize(req CreateOrderRequest) (CreateOrderRequest, error) {
	store := strings.TrimSpace(req.StoreID)
	if store == "" || len(store) > maxIDLength {
		return CreateOrderRequest{}, fmt.Errorf("%w: store_id must be 1-%d characters", ErrInvalidRequest, maxIDLength)
	}
	if len(req.Items) == 0 || len(req.Items) > maxItemsPerOrder {
		return CreateOrderRequest{}, fmt.Errorf("%w: items must contain 1-%d lines", ErrInvalidRequest, maxItemsPerOrder)
	}
	seen := make(map[string]struct{}, len(req.Items))
	items := make([]Item, 0, len(req.Items))
	for _, it := range req.Items {
		sku := strings.TrimSpace(it.SKU)
		if sku == "" || len(sku) > maxIDLength {
			return CreateOrderRequest{}, fmt.Errorf("%w: sku must be 1-%d characters", ErrInvalidRequest, maxIDLength)
		}
		if it.Quantity < 1 || it.Quantity > maxQuantityPerItem {
			return CreateOrderRequest{}, fmt.Errorf("%w: quantity for %q must be 1-%d", ErrInvalidRequest, sku, maxQuantityPerItem)
		}
		if _, dup := seen[sku]; dup {
			return CreateOrderRequest{}, fmt.Errorf("%w: duplicate sku %q", ErrInvalidRequest, sku)
		}
		seen[sku] = struct{}{}
		items = append(items, Item{SKU: sku, Quantity: it.Quantity})
	}
	return CreateOrderRequest{StoreID: store, Items: items}, nil
}

// sortedByLockOrder returns a copy sorted by SKU. All items of one order
// share a store, so sorting by SKU is sorting by (store_id, sku). Plain byte
// order is fine: locks only need ONE consistent order across transactions,
// it does not have to match Postgres's collation.
func sortedByLockOrder(items []Item) []Item {
	out := append([]Item(nil), items...)
	sort.Slice(out, func(i, j int) bool { return out[i].SKU < out[j].SKU })
	return out
}

// requestHash fingerprints the normalised request. Items are sorted first, so
// the same basket listed in a different order counts as the same request.
func requestHash(req CreateOrderRequest) string {
	var b strings.Builder
	b.WriteString(strconv.Quote(req.StoreID))
	b.WriteByte('\n')
	for _, it := range sortedByLockOrder(req.Items) {
		b.WriteString(strconv.Quote(it.SKU))
		b.WriteByte('=')
		b.WriteString(strconv.Itoa(it.Quantity))
		b.WriteByte('\n')
	}
	sum := sha256.Sum256([]byte(b.String()))
	return hex.EncodeToString(sum[:])
}

// rollbackQuietly is deferred after Begin. After a successful Commit the
// rollback returns pgx.ErrTxClosed, which is expected and safe to ignore.
// It uses its own context so cleanup still works if the caller's ctx expired.
func rollbackQuietly(tx pgx.Tx) {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = tx.Rollback(ctx)
}
