package inventory_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/namankumarsahu/quickcart/internal/inventory"
	"github.com/namankumarsahu/quickcart/internal/testdb"
)

const (
	testStore = "store-blr-01"
	testSKU   = "milk-1l"
)

// Tests share one database and TRUNCATE between cases, so none of them call
// t.Parallel(). testdb.Pool skips locally if Postgres is down and FAILS when
// CI=true.
func newTestPool(t *testing.T) *pgxpool.Pool {
	t.Helper()
	return testdb.Pool(t)
}

// seed empties the table and inserts one SKU with the given on_hand.
func seed(t *testing.T, pool *pgxpool.Pool, onHand int) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE inventory`); err != nil {
		t.Fatalf("truncate: %v", err)
	}
	if err := inventory.NewStore(pool).AddStock(ctx, testStore, testSKU, onHand); err != nil {
		t.Fatalf("seed: %v", err)
	}
}

type outcome struct {
	ok           int
	insufficient int
	other        []error
}

// hammer runs fn from `workers` goroutines released at the same instant.
// Each goroutine writes only its own slot in errs, so there is no data race.
func hammer(workers int, fn func(ctx context.Context) error) outcome {
	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	errs := make([]error, workers)
	start := make(chan struct{})
	var wg sync.WaitGroup
	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func(i int) {
			defer wg.Done()
			<-start // barrier: everyone waits, then all go together
			errs[i] = fn(ctx)
		}(i)
	}
	close(start)
	wg.Wait()

	var out outcome
	for _, err := range errs {
		switch {
		case err == nil:
			out.ok++
		case errors.Is(err, inventory.ErrInsufficientStock):
			out.insufficient++
		default:
			out.other = append(out.other, err)
		}
	}
	return out
}

// assertTenOfFifty is shared by the atomic test and the naive break-it test,
// so the ONLY difference between them is the implementation under test.
func assertTenOfFifty(t *testing.T, pool *pgxpool.Pool, out outcome) {
	t.Helper()
	if len(out.other) > 0 {
		t.Errorf("unexpected errors (%d), first: %v", len(out.other), out.other[0])
	}
	if out.ok != 10 {
		t.Errorf("successful reservations = %d, want 10", out.ok)
	}
	if out.insufficient != 40 {
		t.Errorf("ErrInsufficientStock count = %d, want 40", out.insufficient)
	}
	item, err := inventory.NewStore(pool).Get(context.Background(), testStore, testSKU)
	if err != nil {
		t.Fatalf("read back row: %v", err)
	}
	if item.Reserved != 10 || item.OnHand != 10 {
		t.Errorf("row = on_hand %d, reserved %d; want on_hand 10, reserved 10", item.OnHand, item.Reserved)
	}
}

func TestReserve_Concurrent_Atomic(t *testing.T) {
	pool := newTestPool(t)
	seed(t, pool, 10)
	store := inventory.NewStore(pool)

	out := hammer(50, func(ctx context.Context) error {
		return store.Reserve(ctx, testStore, testSKU, 1)
	})
	assertTenOfFifty(t, pool, out)
}

func TestReserve(t *testing.T) {
	tests := []struct {
		name         string
		sku          string
		qty          int
		wantErr      error
		wantReserved int
	}{
		{"within stock", testSKU, 3, nil, 3},
		{"exactly all stock", testSKU, 10, nil, 10},
		{"more than available", testSKU, 11, inventory.ErrInsufficientStock, 0},
		{"unknown sku is not out-of-stock", "no-such-sku", 1, inventory.ErrUnknownSKU, 0},
		{"zero quantity", testSKU, 0, inventory.ErrInvalidQuantity, 0},
		{"negative quantity", testSKU, -2, inventory.ErrInvalidQuantity, 0},
	}

	pool := newTestPool(t)
	store := inventory.NewStore(pool)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seed(t, pool, 10)
			ctx := context.Background()

			err := store.Reserve(ctx, testStore, tc.sku, tc.qty)
			if !errors.Is(err, tc.wantErr) {
				t.Fatalf("Reserve error = %v, want %v", err, tc.wantErr)
			}
			item, err := store.Get(ctx, testStore, testSKU)
			if err != nil {
				t.Fatalf("Get: %v", err)
			}
			if item.Reserved != tc.wantReserved {
				t.Errorf("reserved = %d, want %d", item.Reserved, tc.wantReserved)
			}
		})
	}
}

func TestRelease(t *testing.T) {
	pool := newTestPool(t)
	seed(t, pool, 10)
	store := inventory.NewStore(pool)
	ctx := context.Background()

	if err := store.Reserve(ctx, testStore, testSKU, 4); err != nil {
		t.Fatalf("Reserve: %v", err)
	}
	if err := store.Release(ctx, testStore, testSKU, 3); err != nil {
		t.Fatalf("Release 3 of 4: %v", err)
	}
	item, err := store.Get(ctx, testStore, testSKU)
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if item.Reserved != 1 {
		t.Fatalf("reserved = %d, want 1", item.Reserved)
	}

	// Releasing more than is reserved must be rejected and change nothing.
	if err := store.Release(ctx, testStore, testSKU, 2); !errors.Is(err, inventory.ErrInsufficientReserved) {
		t.Errorf("over-release error = %v, want ErrInsufficientReserved", err)
	}
	if item, _ = store.Get(ctx, testStore, testSKU); item.Reserved != 1 {
		t.Errorf("reserved after rejected release = %d, want 1", item.Reserved)
	}

	if err := store.Release(ctx, testStore, "no-such-sku", 1); !errors.Is(err, inventory.ErrUnknownSKU) {
		t.Errorf("unknown sku error = %v, want ErrUnknownSKU", err)
	}
	if err := store.Release(ctx, testStore, testSKU, 0); !errors.Is(err, inventory.ErrInvalidQuantity) {
		t.Errorf("zero qty error = %v, want ErrInvalidQuantity", err)
	}
}

// The CHECK constraints are the last line of defence: even a buggy query
// cannot leave the table in an impossible state.
func TestDatabaseRejectsInvalidStates(t *testing.T) {
	tests := []struct {
		name string
		sql  string
	}{
		{"reserved above on_hand", `UPDATE inventory SET reserved = on_hand + 1`},
		{"negative reserved", `UPDATE inventory SET reserved = -1`},
		{"negative on_hand", `UPDATE inventory SET on_hand = -1`},
	}

	pool := newTestPool(t)
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			seed(t, pool, 10)
			_, err := pool.Exec(context.Background(), tc.sql)
			var pgErr *pgconn.PgError
			if !errors.As(err, &pgErr) || pgErr.Code != "23514" { // check_violation
				t.Fatalf("error = %v, want SQLSTATE 23514 (check_violation)", err)
			}
		})
	}
}
