# Default recipe
default:
    @just --list

# Build all packages
build:
    go build ./...

# Run tests
test:
    go test ./...

# Run tests with verbose output
test-v:
    go test -v ./...

# Run tests with race detector
test-race:
    go test -race ./...

# Run tests with coverage
test-cover:
    go test -cover ./...

# Run tests with a coverage profile and enforce a minimum (excludes generated/examples)
test-cover-gate threshold="55":
    #!/usr/bin/env bash
    set -euo pipefail
    pkgs=$(go list ./... | grep -vE '/proto/|/examples' | paste -sd, -)
    go test -coverpkg="$pkgs" -coverprofile=cover.out ./...
    go tool cover -func=cover.out | tail -1
    pct=$(go tool cover -func=cover.out | awk '/^total:/ {gsub(/%/,"",$3); print $3}')
    awk -v p="$pct" -v t="{{threshold}}" 'BEGIN{ if (p+0 < t+0){ printf "coverage %.1f%% is below threshold %s%%\n", p, t; exit 1 } else { printf "coverage %.1f%% meets threshold %s%%\n", p, t } }'

# Run only the fast, hermetic smoke tests (no external services)
smoke:
    go test -run 'TestSmoke|TestRedisLifecycle|TestMetrics|TestMongoCodec|TestPostgresSqlmock' ./...

# Run benchmarks with allocation stats
bench:
    go test -run '^$' -bench=. -benchmem ./...

# Run the fuzz seed corpora as regular tests
fuzz-seed:
    go test -run 'Fuzz' ./...

# Fuzz a single target, e.g.: just fuzz FuzzComputeNextSchedule 30s
fuzz target time="30s":
    go test -run '^$' -fuzz '^{{target}}$' -fuzztime={{time}} ./...

# Start local backends for integration tests
compose-up:
    docker compose up -d --wait

# Stop local backends
compose-down:
    docker compose down -v

# Run the Docker-gated integration suite against compose backends
test-integration:
    REDIS_ADDR=localhost:6380 \
    MONGO_URI=mongodb://localhost:27018 \
    POSTGRES_DSN="postgres://scheduler_test:scheduler_test@localhost:5433/scheduler_test?sslmode=disable" \
        go test -tags=integration -race ./...

# Integration: Redis only
test-redis:
    REDIS_ADDR=localhost:6380 go test -tags=integration -race -run 'Redis' ./...

# Integration: PostgreSQL only
test-pg:
    POSTGRES_DSN="postgres://scheduler_test:scheduler_test@localhost:5433/scheduler_test?sslmode=disable" \
        go test -tags=integration -race -run 'Postgres' ./...

# Integration: MongoDB only
test-mongo:
    MONGO_URI=mongodb://localhost:27018 go test -tags=integration -race -run 'Mongo' ./...

# Format code
fmt:
    go fmt ./...

# Lint code
lint:
    golangci-lint run ./...

# Tidy dependencies
tidy:
    go mod tidy

# Run vulnerability check
vulncheck:
    go run golang.org/x/vuln/cmd/govulncheck@latest ./...

# Check for outdated dependencies
depcheck:
    go list -m -u all | grep '\[' || echo "All dependencies are up to date"

# Generate protobuf code
proto:
    buf generate

# Create and push a new release tag (bumps patch version)
release:
    ./scripts/release.sh
