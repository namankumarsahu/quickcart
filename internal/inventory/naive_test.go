//go:build naive

// Break-it exercise. This file is compiled only with `-tags naive`, so normal
// `go test` runs never see it. It contains the classic read-check-write bug.
package inventory_test

import (
	"context"
	"testing"

	"github.com/jackc/pgx/v5/pgxpool"

	"github.com/namankumarsahu/quickcart/internal/inventory"
)

// naiveReserve is WRONG on purpose:
//  1. SELECT reads reserved.
//  2. Go checks availability using that (possibly stale) value.
//  3. UPDATE writes an ABSOLUTE value computed from the stale read.
//
// Between steps 1 and 3 other goroutines change the row, so checks pass that
// should fail and increments get overwritten (lost update).
func naiveReserve(ctx context.Context, pool *pgxpool.Pool, storeID, sku string, qty int) error {
	var onHand, reserved int
	err := pool.QueryRow(ctx,
		`SELECT on_hand, reserved FROM inventory WHERE store_id = $1 AND sku = $2`,
		storeID, sku).Scan(&onHand, &reserved)
	if err != nil {
		return err
	}
	if onHand-reserved < qty {
		return inventory.ErrInsufficientStock
	}
	_, err = pool.Exec(ctx,
		`UPDATE inventory SET reserved = $3 WHERE store_id = $1 AND sku = $2`,
		storeID, sku, reserved+qty)
	return err
}

func TestReserve_Concurrent_Naive(t *testing.T) {
	pool := newTestPool(t)
	seed(t, pool, 10)

	out := hammer(50, func(ctx context.Context) error {
		return naiveReserve(ctx, pool, testStore, testSKU, 1)
	})
	assertTenOfFifty(t, pool, out) // expected to FAIL
}
