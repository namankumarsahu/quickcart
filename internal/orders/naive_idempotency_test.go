//go:build naive

// Break-it exercise B. Compiled only with `-tags naive`.
// Internal test package so it can reuse the unexported helpers.
package orders

import (
	"context"
	"sync"
	"testing"
	"time"

	"github.com/namankumarsahu/quickcart/internal/inventory"
	"github.com/namankumarsahu/quickcart/internal/testdb"
)

// naiveCreateOrder is WRONG on purpose: it CHECKS whether the key exists,
// creates the order, and only then records the key. Between the check and the
// record, other requests do the same check and also see "not found".
func naiveCreateOrder(ctx context.Context, db DB, key string, req CreateOrderRequest) (CreateResult, error) {
	norm, err := normalize(req)
	if err != nil {
		return CreateResult{}, err
	}
	hash := requestHash(norm)

	tx, err := db.Begin(ctx)
	if err != nil {
		return CreateResult{}, err
	}
	defer rollbackQuietly(tx)

	var exists bool
	if err := tx.QueryRow(ctx,
		`SELECT EXISTS (SELECT 1 FROM idempotency_keys WHERE key = $1)`, key).Scan(&exists); err != nil {
		return CreateResult{}, err
	}
	if exists {
		return replay(ctx, tx, key, hash)
	}

	orderID, body, err := createOrderRows(ctx, tx, norm)
	if err != nil {
		return CreateResult{}, err
	}
	// "Insert if missing". DO NOTHING silently swallows the collision, so the
	// losers still commit their own order.
	if _, err := tx.Exec(ctx, `
INSERT INTO idempotency_keys (key, request_hash, response_status, response_body, order_id)
VALUES ($1, $2, $3, $4, $5) ON CONFLICT (key) DO NOTHING`,
		key, hash, responseCreated, string(body), orderID); err != nil {
		return CreateResult{}, err
	}
	if err := tx.Commit(ctx); err != nil {
		return CreateResult{}, err
	}
	return CreateResult{OrderID: orderID, StatusCode: responseCreated, Body: body}, nil
}

func TestCreateOrder_SameKeyConcurrent_Naive(t *testing.T) {
	pool := testdb.Pool(t)
	testdb.Reset(t, pool)
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	const store = "store-blr-01"
	if err := inventory.NewStore(pool).AddStock(ctx, store, "milk", 100); err != nil {
		t.Fatalf("seed: %v", err)
	}
	req := CreateOrderRequest{StoreID: store, Items: []Item{{SKU: "milk", Quantity: 1}}}

	const callers = 20
	errs := make([]error, callers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < callers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			_, errs[i] = naiveCreateOrder(ctx, pool, "same-key", req)
		}(i)
	}
	close(start)
	wg.Wait()

	for i, err := range errs {
		if err != nil {
			t.Errorf("caller %d: %v", i, err)
		}
	}
	var orders, reserved int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM orders`).Scan(&orders); err != nil {
		t.Fatalf("count orders: %v", err)
	}
	if err := pool.QueryRow(ctx, `SELECT reserved FROM inventory WHERE store_id=$1 AND sku='milk'`, store).Scan(&reserved); err != nil {
		t.Fatalf("read reserved: %v", err)
	}
	if orders != 1 {
		t.Errorf("orders created = %d, want 1 (duplicates!)", orders)
	}
	if reserved != 1 {
		t.Errorf("units reserved = %d, want 1 (stock leaked)", reserved)
	}
}
