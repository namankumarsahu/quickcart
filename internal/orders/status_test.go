package orders_test

import (
	"fmt"
	"testing"

	"github.com/namankumarsahu/quickcart/internal/orders"
)

func TestCanTransition(t *testing.T) {
	all := []orders.Status{
		orders.StatusCreated, orders.StatusReserved, orders.StatusConfirmed,
		orders.StatusPacked, orders.StatusDelivered, orders.StatusCancelled, orders.StatusExpired,
	}
	// The ONLY legal moves. Every other pair of the 49 must be rejected.
	allowed := map[[2]orders.Status]bool{
		{orders.StatusCreated, orders.StatusReserved}:    true,
		{orders.StatusReserved, orders.StatusConfirmed}:  true,
		{orders.StatusConfirmed, orders.StatusPacked}:    true,
		{orders.StatusPacked, orders.StatusDelivered}:    true,
		{orders.StatusCreated, orders.StatusCancelled}:   true,
		{orders.StatusReserved, orders.StatusCancelled}:  true,
		{orders.StatusConfirmed, orders.StatusCancelled}: true,
		{orders.StatusReserved, orders.StatusExpired}:    true,
	}
	for _, from := range all {
		for _, to := range all {
			want := allowed[[2]orders.Status{from, to}]
			t.Run(fmt.Sprintf("%s->%s", from, to), func(t *testing.T) {
				if got := orders.CanTransition(from, to); got != want {
					t.Fatalf("CanTransition(%s, %s) = %v, want %v", from, to, got, want)
				}
			})
		}
	}
}

func TestStatusValid(t *testing.T) {
	if !orders.StatusDelivered.Valid() {
		t.Error("delivered (a terminal state) should be valid")
	}
	if orders.Status("bogus").Valid() {
		t.Error("bogus should not be valid")
	}
}
