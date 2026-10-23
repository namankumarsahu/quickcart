package orders_test

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/namankumarsahu/quickcart/internal/inventory"
	"github.com/namankumarsahu/quickcart/internal/orders"
	"github.com/namankumarsahu/quickcart/internal/testdb"
)

const store = "store-blr-01"

type env struct {
	pool *pgxpool.Pool
	svc  *orders.Service
	inv  *inventory.Store
}

func newEnv(t *testing.T) env {
	t.Helper()
	pool := testdb.Pool(t)
	testdb.Reset(t, pool)
	return env{pool: pool, svc: orders.NewService(pool), inv: inventory.NewStore(pool)}
}

func testCtx(t *testing.T) context.Context {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	t.Cleanup(cancel)
	return ctx
}

func (e env) addStock(t *testing.T, sku string, qty int) {
	t.Helper()
	if err := e.inv.AddStock(context.Background(), store, sku, qty); err != nil {
		t.Fatalf("add stock %s: %v", sku, err)
	}
}

func (e env) reserved(t *testing.T, sku string) int {
	t.Helper()
	item, err := e.inv.Get(context.Background(), store, sku)
	if err != nil {
		t.Fatalf("get %s: %v", sku, err)
	}
	return item.Reserved
}

func (e env) count(t *testing.T, table string) int {
	t.Helper()
	switch table {
	case "orders", "order_items", "idempotency_keys":
	default:
		t.Fatalf("count: unexpected table %q", table)
	}
	var n int
	if err := e.pool.QueryRow(context.Background(), "SELECT count(*) FROM "+table).Scan(&n); err != nil {
		t.Fatalf("count %s: %v", table, err)
	}
	return n
}

func (e env) status(t *testing.T, orderID string) string {
	t.Helper()
	var s string
	if err := e.pool.QueryRow(context.Background(),
		`SELECT status FROM orders WHERE id = $1`, orderID).Scan(&s); err != nil {
		t.Fatalf("read status: %v", err)
	}
	return s
}

func oneItem(sku string, qty int) orders.CreateOrderRequest {
	return orders.CreateOrderRequest{StoreID: store, Items: []orders.Item{{SKU: sku, Quantity: qty}}}
}

type callResult struct {
	res orders.CreateResult
	err error
}

// runConcurrent releases n goroutines at the same instant. Each writes only
// its own slot, so there is no data race and no mutex.
func runConcurrent[T any](n int, fn func(i int) T) []T {
	results := make([]T, n)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < n; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start
			results[i] = fn(i)
		}(i)
	}
	close(start)
	wg.Wait()
	return results
}

// 1. Happy path
func TestCreateOrder_HappyPath(t *testing.T) {
	e := newEnv(t)
	e.addStock(t, "milk", 10)
	e.addStock(t, "bread", 5)

	req := orders.CreateOrderRequest{StoreID: store, Items: []orders.Item{
		{SKU: "milk", Quantity: 3}, {SKU: "bread", Quantity: 2},
	}}
	res, err := e.svc.CreateOrder(testCtx(t), "key-happy", req)
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}
	if res.Replayed || res.StatusCode != 201 || res.OrderID == "" {
		t.Fatalf("result = %+v, want fresh 201 with an order id", res)
	}
	var body struct {
		OrderID string `json:"order_id"`
		Status  string `json:"status"`
	}
	if err := json.Unmarshal(res.Body, &body); err != nil {
		t.Fatalf("response body is not JSON: %v", err)
	}
	if body.OrderID != res.OrderID || body.Status != "reserved" {
		t.Errorf("body = %+v, want order %s reserved", body, res.OrderID)
	}
	if got := e.status(t, res.OrderID); got != "reserved" {
		t.Errorf("stored status = %q, want reserved", got)
	}
	if got := e.reserved(t, "milk"); got != 3 {
		t.Errorf("milk reserved = %d, want 3", got)
	}
	if got := e.reserved(t, "bread"); got != 2 {
		t.Errorf("bread reserved = %d, want 2", got)
	}
	if got := e.count(t, "order_items"); got != 2 {
		t.Errorf("order_items = %d, want 2", got)
	}
}

// 2. Out of stock
func TestCreateOrder_OutOfStock(t *testing.T) {
	e := newEnv(t)
	e.addStock(t, "milk", 2)

	_, err := e.svc.CreateOrder(testCtx(t), "key-oos", oneItem("milk", 3))
	if !errors.Is(err, inventory.ErrInsufficientStock) {
		t.Fatalf("error = %v, want ErrInsufficientStock", err)
	}
	for _, table := range []string{"orders", "order_items", "idempotency_keys"} {
		if n := e.count(t, table); n != 0 {
			t.Errorf("%s rows = %d, want 0 (everything must roll back)", table, n)
		}
	}
	if got := e.reserved(t, "milk"); got != 0 {
		t.Errorf("milk reserved = %d, want 0", got)
	}
}

// 3. Partial failure: item "a-" is reserved first (sorted order), then the
// later item fails; the first reservation must be rolled back.
func TestCreateOrder_PartialFailureRollsBack(t *testing.T) {
	tests := []struct {
		name    string
		bad     orders.Item
		wantErr error
	}{
		{"second item out of stock", orders.Item{SKU: "z-scarce", Quantity: 2}, inventory.ErrInsufficientStock},
		{"second item unknown", orders.Item{SKU: "z-missing", Quantity: 1}, inventory.ErrUnknownSKU},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			e := newEnv(t)
			e.addStock(t, "a-plenty", 10)
			e.addStock(t, "z-scarce", 1)

			req := orders.CreateOrderRequest{StoreID: store, Items: []orders.Item{
				{SKU: "a-plenty", Quantity: 4}, tc.bad,
			}}
			_, err := e.svc.CreateOrder(testCtx(t), "key-partial", req)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if got := e.reserved(t, "a-plenty"); got != 0 {
				t.Errorf("a-plenty reserved = %d, want 0 (rolled back)", got)
			}
			if n := e.count(t, "orders"); n != 0 {
				t.Errorf("orders = %d, want 0", n)
			}
		})
	}
}

// 4. Same key twice, one after the other
func TestCreateOrder_SameKeySequential(t *testing.T) {
	e := newEnv(t)
	e.addStock(t, "milk", 10)
	ctx := testCtx(t)

	first, err := e.svc.CreateOrder(ctx, "key-seq", oneItem("milk", 3))
	if err != nil {
		t.Fatalf("first call: %v", err)
	}
	second, err := e.svc.CreateOrder(ctx, "key-seq", oneItem("milk", 3))
	if err != nil {
		t.Fatalf("second call: %v", err)
	}
	if first.Replayed || !second.Replayed {
		t.Errorf("Replayed = %v then %v, want false then true", first.Replayed, second.Replayed)
	}
	if first.OrderID != second.OrderID || string(first.Body) != string(second.Body) {
		t.Errorf("replay differs from original:\n first  %+v\n second %+v", first, second)
	}
	if n := e.count(t, "orders"); n != 1 {
		t.Errorf("orders = %d, want 1", n)
	}
	if got := e.reserved(t, "milk"); got != 3 {
		t.Errorf("milk reserved = %d, want 3 (not 6)", got)
	}
}

// 5. Same key, 20 goroutines at once
func TestCreateOrder_SameKeyConcurrent(t *testing.T) {
	e := newEnv(t)
	e.addStock(t, "milk", 10)
	ctx := testCtx(t)

	results := runConcurrent(20, func(int) callResult {
		res, err := e.svc.CreateOrder(ctx, "key-concurrent", oneItem("milk", 2))
		return callResult{res, err}
	})

	ids := map[string]int{}
	fresh := 0
	for i, r := range results {
		if r.err != nil {
			t.Errorf("caller %d: %v", i, r.err)
			continue
		}
		ids[r.res.OrderID]++
		if !r.res.Replayed {
			fresh++
		}
	}
	if len(ids) != 1 {
		t.Errorf("distinct order ids = %d (%v), want 1", len(ids), ids)
	}
	if fresh != 1 {
		t.Errorf("callers that created the order = %d, want exactly 1", fresh)
	}
	if n := e.count(t, "orders"); n != 1 {
		t.Errorf("orders = %d, want 1", n)
	}
	if got := e.reserved(t, "milk"); got != 2 {
		t.Errorf("milk reserved = %d, want 2", got)
	}
}

// 6. Same key, different body
func TestCreateOrder_SameKeyDifferentBody(t *testing.T) {
	e := newEnv(t)
	e.addStock(t, "milk", 10)
	ctx := testCtx(t)

	if _, err := e.svc.CreateOrder(ctx, "key-diff", oneItem("milk", 3)); err != nil {
		t.Fatalf("first call: %v", err)
	}
	_, err := e.svc.CreateOrder(ctx, "key-diff", oneItem("milk", 4))
	if !errors.Is(err, orders.ErrKeyReused) {
		t.Fatalf("error = %v, want ErrKeyReused", err)
	}
	if n := e.count(t, "orders"); n != 1 {
		t.Errorf("orders = %d, want 1", n)
	}
	if got := e.reserved(t, "milk"); got != 3 {
		t.Errorf("milk reserved = %d, want 3", got)
	}
}

// 7. Opposite-order item lists: must not deadlock.
func TestCreateOrder_OppositeOrder_NoDeadlock(t *testing.T) {
	e := newEnv(t)
	skus := []string{"s1", "s2", "s3", "s4", "s5", "s6"}
	forward := make([]orders.Item, 0, len(skus))
	reverse := make([]orders.Item, 0, len(skus))
	for _, s := range skus {
		e.addStock(t, s, 1000)
		forward = append(forward, orders.Item{SKU: s, Quantity: 1})
	}
	for i := len(forward) - 1; i >= 0; i-- {
		reverse = append(reverse, forward[i])
	}

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Second) // test timeout
	defer cancel()

	const rounds, perRound = 3, 40
	for r := 0; r < rounds; r++ {
		results := runConcurrent(perRound, func(i int) callResult {
			list := forward
			if i%2 == 1 {
				list = reverse
			}
			res, err := e.svc.CreateOrder(ctx, fmt.Sprintf("opp-%d-%d", r, i),
				orders.CreateOrderRequest{StoreID: store, Items: list})
			return callResult{res, err}
		})
		for i, c := range results {
			if c.err != nil {
				t.Errorf("round %d caller %d: %v", r, i, c.err) // a deadlock shows up as SQLSTATE 40P01
			}
		}
	}
	for _, s := range skus {
		if got := e.reserved(t, s); got != rounds*perRound {
			t.Errorf("%s reserved = %d, want %d", s, got, rounds*perRound)
		}
	}
}

// 8. 50 orders for 1 unit each against 10 units: exactly 10 orders.
func TestCreateOrder_Concurrent_NoOversell(t *testing.T) {
	e := newEnv(t)
	e.addStock(t, "milk", 10)
	ctx := testCtx(t)

	results := runConcurrent(50, func(i int) callResult {
		res, err := e.svc.CreateOrder(ctx, fmt.Sprintf("buyer-%d", i), oneItem("milk", 1))
		return callResult{res, err}
	})

	ok, soldOut := 0, 0
	for i, r := range results {
		switch {
		case r.err == nil:
			ok++
		case errors.Is(r.err, inventory.ErrInsufficientStock):
			soldOut++
		default:
			t.Errorf("buyer %d: unexpected error: %v", i, r.err)
		}
	}
	if ok != 10 || soldOut != 40 {
		t.Errorf("orders created = %d, sold out = %d; want 10 and 40", ok, soldOut)
	}
	if n := e.count(t, "orders"); n != 10 {
		t.Errorf("orders rows = %d, want 10", n)
	}
	if n := e.count(t, "idempotency_keys"); n != 10 {
		t.Errorf("idempotency_keys rows = %d, want 10 (failed attempts leave no key)", n)
	}
	if got := e.reserved(t, "milk"); got != 10 {
		t.Errorf("milk reserved = %d, want 10", got)
	}
}

// 9. Transitions: illegal in Go, stale affects zero rows, DB trigger backs it up.
func TestTransition_IllegalAndStale(t *testing.T) {
	e := newEnv(t)
	e.addStock(t, "milk", 10)
	ctx := testCtx(t)

	res, err := e.svc.CreateOrder(ctx, "key-transition", oneItem("milk", 1))
	if err != nil {
		t.Fatalf("CreateOrder: %v", err)
	}

	if err := e.svc.Transition(ctx, res.OrderID, orders.StatusReserved, orders.StatusConfirmed); err != nil {
		t.Fatalf("reserved -> confirmed: %v", err)
	}
	// Stale: the order is already confirmed, so "from reserved" matches 0 rows.
	err = e.svc.Transition(ctx, res.OrderID, orders.StatusReserved, orders.StatusConfirmed)
	if !errors.Is(err, orders.ErrStaleTransition) {
		t.Errorf("stale transition error = %v, want ErrStaleTransition", err)
	}
	// Illegal: rejected in Go before touching the database.
	err = e.svc.Transition(ctx, res.OrderID, orders.StatusCreated, orders.StatusDelivered)
	if !errors.Is(err, orders.ErrIllegalTransition) {
		t.Errorf("illegal transition error = %v, want ErrIllegalTransition", err)
	}
	// Unknown order.
	err = e.svc.Transition(ctx, "00000000-0000-0000-0000-000000000000", orders.StatusCreated, orders.StatusReserved)
	if !errors.Is(err, orders.ErrOrderNotFound) {
		t.Errorf("unknown order error = %v, want ErrOrderNotFound", err)
	}
	if got := e.status(t, res.OrderID); got != "confirmed" {
		t.Errorf("status = %q, want confirmed", got)
	}

	// Bypass Go entirely: the database itself must refuse these.
	for _, bad := range []string{"delivered", "bogus"} {
		_, err := e.pool.Exec(ctx, `UPDATE orders SET status = $2 WHERE id = $1`, res.OrderID, bad)
		var pgErr *pgconn.PgError
		if !errors.As(err, &pgErr) || pgErr.Code != "23514" {
			t.Errorf("raw update to %q: error = %v, want SQLSTATE 23514", bad, err)
		}
	}
}

func TestCreateOrder_Validation(t *testing.T) {
	big := orders.CreateOrderRequest{StoreID: store}
	for i := 0; i < 51; i++ {
		big.Items = append(big.Items, orders.Item{SKU: fmt.Sprintf("sku-%d", i), Quantity: 1})
	}
	tests := []struct {
		name    string
		key     string
		req     orders.CreateOrderRequest
		wantErr error
	}{
		{"missing key", "", oneItem("milk", 1), orders.ErrInvalidKey},
		{"empty store", "k", orders.CreateOrderRequest{Items: []orders.Item{{SKU: "milk", Quantity: 1}}}, orders.ErrInvalidRequest},
		{"no items", "k", orders.CreateOrderRequest{StoreID: store}, orders.ErrInvalidRequest},
		{"too many items", "k", big, orders.ErrInvalidRequest},
		{"empty sku", "k", oneItem("  ", 1), orders.ErrInvalidRequest},
		{"zero quantity", "k", oneItem("milk", 0), orders.ErrInvalidRequest},
		{"negative quantity", "k", oneItem("milk", -1), orders.ErrInvalidRequest},
		{"quantity over max", "k", oneItem("milk", 51), orders.ErrInvalidRequest},
		{"duplicate sku", "k", orders.CreateOrderRequest{StoreID: store, Items: []orders.Item{
			{SKU: "milk", Quantity: 1}, {SKU: "milk", Quantity: 2}}}, orders.ErrInvalidRequest},
	}
	e := newEnv(t)
	e.addStock(t, "milk", 10)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			_, err := e.svc.CreateOrder(testCtx(t), tc.key, tc.req)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("error = %v, want %v", err, tc.wantErr)
			}
			if n := e.count(t, "orders"); n != 0 {
				t.Errorf("orders = %d, want 0", n)
			}
		})
	}
}
