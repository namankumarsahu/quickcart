// Package inventory owns stock counts per dark store. Available stock is
// on_hand - reserved; reservations never touch on_hand.
package inventory

import (
	"context"
	"errors"
	"fmt"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
)

// Sentinel errors let callers branch with errors.Is. They are immutable
// values, not mutable global state.
var (
	ErrInsufficientStock    = errors.New("inventory: insufficient stock")
	ErrUnknownSKU           = errors.New("inventory: unknown sku")
	ErrInsufficientReserved = errors.New("inventory: release exceeds reserved quantity")
	ErrInvalidQuantity      = errors.New("inventory: quantity must be positive")
)

// DB is the small slice of pgx that Store needs. Both *pgxpool.Pool and
// pgx.Tx satisfy it, so Milestone 2 can run the same code inside a transaction.
type DB interface {
	Exec(ctx context.Context, sql string, args ...any) (pgconn.CommandTag, error)
	QueryRow(ctx context.Context, sql string, args ...any) pgx.Row
}

// Item is one inventory row.
type Item struct {
	StoreID  string
	SKU      string
	OnHand   int
	Reserved int
}

// Available is the quantity that can still be reserved.
func (i Item) Available() int { return i.OnHand - i.Reserved }

// Store reads and writes inventory rows.
type Store struct {
	db DB
}

// NewStore returns a Store backed by db (a pool or a transaction).
func NewStore(db DB) *Store { return &Store{db: db} }

const reserveSQL = `
UPDATE inventory
   SET reserved = reserved + $3, updated_at = now()
 WHERE store_id = $1 AND sku = $2 AND on_hand - reserved >= $3`

const releaseSQL = `
UPDATE inventory
   SET reserved = reserved - $3, updated_at = now()
 WHERE store_id = $1 AND sku = $2 AND reserved >= $3`

const existsSQL = `
SELECT EXISTS (SELECT 1 FROM inventory WHERE store_id = $1 AND sku = $2)`

// Reserve holds qty units for (storeID, sku). The check and the write happen
// in ONE statement, so no other buyer can slip in between them.
func (s *Store) Reserve(ctx context.Context, storeID, sku string, qty int) error {
	if qty <= 0 {
		return fmt.Errorf("reserve %s/%s qty=%d: %w", storeID, sku, qty, ErrInvalidQuantity)
	}
	tag, err := s.db.Exec(ctx, reserveSQL, storeID, sku, qty)
	if err != nil {
		return fmt.Errorf("reserve %s/%s: %w", storeID, sku, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	// Zero rows: either the SKU doesn't exist or there isn't enough stock.
	reason, err := s.missReason(ctx, storeID, sku, ErrInsufficientStock)
	if err != nil {
		return fmt.Errorf("reserve %s/%s: %w", storeID, sku, err)
	}
	return fmt.Errorf("reserve %s/%s qty=%d: %w", storeID, sku, qty, reason)
}

// Release gives back qty previously reserved units (cancellation or expiry).
func (s *Store) Release(ctx context.Context, storeID, sku string, qty int) error {
	if qty <= 0 {
		return fmt.Errorf("release %s/%s qty=%d: %w", storeID, sku, qty, ErrInvalidQuantity)
	}
	tag, err := s.db.Exec(ctx, releaseSQL, storeID, sku, qty)
	if err != nil {
		return fmt.Errorf("release %s/%s: %w", storeID, sku, err)
	}
	if tag.RowsAffected() == 1 {
		return nil
	}
	reason, err := s.missReason(ctx, storeID, sku, ErrInsufficientReserved)
	if err != nil {
		return fmt.Errorf("release %s/%s: %w", storeID, sku, err)
	}
	return fmt.Errorf("release %s/%s qty=%d: %w", storeID, sku, qty, reason)
}

// AddStock increases on_hand (creating the row if needed). Used for seeding
// and, later, goods-received events.
func (s *Store) AddStock(ctx context.Context, storeID, sku string, qty int) error {
	if qty <= 0 {
		return fmt.Errorf("add stock %s/%s qty=%d: %w", storeID, sku, qty, ErrInvalidQuantity)
	}
	const q = `
INSERT INTO inventory (store_id, sku, on_hand) VALUES ($1, $2, $3)
ON CONFLICT (store_id, sku)
DO UPDATE SET on_hand = inventory.on_hand + EXCLUDED.on_hand, updated_at = now()`
	if _, err := s.db.Exec(ctx, q, storeID, sku, qty); err != nil {
		return fmt.Errorf("add stock %s/%s: %w", storeID, sku, err)
	}
	return nil
}

// Get returns the current row, or ErrUnknownSKU.
func (s *Store) Get(ctx context.Context, storeID, sku string) (Item, error) {
	it := Item{StoreID: storeID, SKU: sku}
	const q = `SELECT on_hand, reserved FROM inventory WHERE store_id = $1 AND sku = $2`
	err := s.db.QueryRow(ctx, q, storeID, sku).Scan(&it.OnHand, &it.Reserved)
	if errors.Is(err, pgx.ErrNoRows) {
		return Item{}, fmt.Errorf("get %s/%s: %w", storeID, sku, ErrUnknownSKU)
	}
	if err != nil {
		return Item{}, fmt.Errorf("get %s/%s: %w", storeID, sku, err)
	}
	return it, nil
}

// missReason runs only after an UPDATE touched zero rows. It returns
// ErrUnknownSKU if the row doesn't exist, otherwise ifExists.
func (s *Store) missReason(ctx context.Context, storeID, sku string, ifExists error) (error, error) {
	var exists bool
	if err := s.db.QueryRow(ctx, existsSQL, storeID, sku).Scan(&exists); err != nil {
		return nil, fmt.Errorf("check sku existence: %w", err)
	}
	if !exists {
		return ErrUnknownSKU, nil
	}
	return ifExists, nil
}
