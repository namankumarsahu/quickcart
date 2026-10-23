// Package testdb gives integration tests a connected pgx pool.
package testdb

import (
	"context"
	"fmt"
	"os"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgxpool"
)

// lockKey is an arbitrary constant. Every test takes this Postgres advisory
// lock, so tests from different packages (which `go test ./...` runs in
// parallel) cannot truncate each other's data.
const lockKey int64 = 7_243_001

// Pool connects to the compose Postgres (or DATABASE_URL) and holds the test
// lock until the test ends. If Postgres is unreachable the test is skipped
// locally, but FAILS when CI=true so a broken pipeline can never go green.
func Pool(t testing.TB) *pgxpool.Pool {
	t.Helper()
	dsn := os.Getenv("DATABASE_URL")
	if dsn == "" {
		dsn = "postgres://quickcart:quickcart@localhost:5433/quickcart?sslmode=disable"
	}
	cfg, err := pgxpool.ParseConfig(dsn)
	if err != nil {
		t.Fatalf("parse DATABASE_URL: %v", err)
	}
	cfg.MaxConns = 32 // one connection holds the lock; the rest run the test

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	pool, err := pgxpool.NewWithConfig(ctx, cfg)
	if err != nil {
		t.Fatalf("create pool: %v", err)
	}
	if err := pool.Ping(ctx); err != nil {
		pool.Close()
		msg := fmt.Sprintf("postgres not reachable: %v (run `docker compose up -d --wait`)", err)
		if os.Getenv("CI") == "true" {
			t.Fatal(msg)
		}
		t.Skip(msg)
	}
	t.Cleanup(pool.Close) // registered first, so it runs LAST

	lockCtx, lockCancel := context.WithTimeout(context.Background(), 2*time.Minute)
	defer lockCancel()
	conn, err := pool.Acquire(lockCtx)
	if err != nil {
		t.Fatalf("acquire lock connection: %v", err)
	}
	if _, err := conn.Exec(lockCtx, `SELECT pg_advisory_lock($1)`, lockKey); err != nil {
		conn.Release()
		t.Fatalf("take test lock: %v", err)
	}
	t.Cleanup(func() {
		// The lock belongs to the database session, and a released connection
		// stays open in the pool, so unlock explicitly BEFORE releasing it.
		_, _ = conn.Exec(context.Background(), `SELECT pg_advisory_unlock($1)`, lockKey)
		conn.Release()
	})
	return pool
}

// Reset empties every table. Safe because Pool holds the cross-package lock.
func Reset(t testing.TB, pool *pgxpool.Pool) {
	t.Helper()
	ctx := context.Background()
	if _, err := pool.Exec(ctx, `TRUNCATE order_items, orders, idempotency_keys, inventory`); err != nil {
		t.Fatalf("reset tables (did you apply migration 002?): %v", err)
	}
}
// // Package testdb gives integration tests a connected pgx pool.
// package testdb

// import (
// 	"context"
// 	"fmt"
// 	"os"
// 	"testing"
// 	"time"

// 	"github.com/jackc/pgx/v5/pgxpool"
// )

// // Pool connects to the compose Postgres (or DATABASE_URL). If Postgres is
// // unreachable the test is skipped locally, but FAILS when CI=true so a broken
// // pipeline can never go green by skipping.
// func Pool(t testing.TB) *pgxpool.Pool {
// 	t.Helper()
// 	dsn := os.Getenv("DATABASE_URL")
// 	if dsn == "" {
// 		dsn = "postgres://quickcart:quickcart@localhost:5433/quickcart?sslmode=disable"
// 	}
// 	cfg, err := pgxpool.ParseConfig(dsn)
// 	if err != nil {
// 		t.Fatalf("parse DATABASE_URL: %v", err)
// 	}
// 	cfg.MaxConns = 30 // enough that 20-50 goroutines can hold transactions at once

// 	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
// 	defer cancel()
// 	pool, err := pgxpool.NewWithConfig(ctx, cfg)
// 	if err != nil {
// 		t.Fatalf("create pool: %v", err)
// 	}
// 	if err := pool.Ping(ctx); err != nil {
// 		pool.Close()
// 		msg := fmt.Sprintf("postgres not reachable: %v (run `docker compose up -d --wait`)", err)
// 		if os.Getenv("CI") == "true" {
// 			t.Fatal(msg)
// 		}
// 		t.Skip(msg)
// 	}
// 	t.Cleanup(pool.Close)
// 	return pool
// }

// // Reset empties every table. Tests share one database, so they must not run
// // with t.Parallel().
// func Reset(t testing.TB, pool *pgxpool.Pool) {
// 	t.Helper()
// 	ctx := context.Background()
// 	if _, err := pool.Exec(ctx, `TRUNCATE order_items, orders, idempotency_keys, inventory`); err != nil {
// 		t.Fatalf("reset tables (did you apply migration 002?): %v", err)
// 	}
// }
