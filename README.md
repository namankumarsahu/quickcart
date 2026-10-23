# QuickCart (Milestones 1-2)

Fresh start:

```powershell
go mod tidy
docker compose down -v          # wipes the old volume so BOTH migrations run
docker compose up -d --wait
go test -count=1 -v ./...
go test -race -count=1 ./...    # needs CGO + gcc on Windows; CI runs it on Linux
```

Already have a running DB from Milestone 1 and want to keep it:

```powershell
Get-Content -Raw migrations\002_orders.sql | docker compose exec -T postgres psql -U quickcart -d quickcart -v ON_ERROR_STOP=1
```

Break-it exercises:

```powershell
# A: deadlock. In internal/orders/service.go change sortedByLockOrder(req.Items) to req.Items
go test -count=3 -run NoDeadlock -v ./internal/orders/
git checkout internal/orders/service.go     # restore the fix

# B: duplicate orders from SELECT-then-INSERT idempotency (should FAIL)
go test -count=1 -tags naive -run Naive -v ./internal/orders/
```

Simulate CI strictness: `$env:CI = "true"; go test ./...` (fails instead of skipping when Postgres is down).
