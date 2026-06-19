# Event Scheduler

[![CI](https://github.com/rbaliyan/event-scheduler/actions/workflows/ci.yml/badge.svg)](https://github.com/rbaliyan/event-scheduler/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/rbaliyan/event-scheduler.svg)](https://pkg.go.dev/github.com/rbaliyan/event-scheduler)
[![Go Report Card](https://goreportcard.com/badge/github.com/rbaliyan/event-scheduler)](https://goreportcard.com/report/github.com/rbaliyan/event-scheduler)
[![Release](https://img.shields.io/github/v/release/rbaliyan/event-scheduler)](https://github.com/rbaliyan/event-scheduler/releases/latest)
[![License: MIT](https://img.shields.io/badge/License-MIT-blue.svg)](https://opensource.org/licenses/MIT)
[![OpenSSF Scorecard](https://api.scorecard.dev/projects/github.com/rbaliyan/event-scheduler/badge)](https://scorecard.dev/viewer/?uri=github.com/rbaliyan/event-scheduler)

A production-grade delayed and scheduled message delivery library for Go. Schedule messages for future delivery with support for Redis, PostgreSQL, and MongoDB backends.

## Features

- **Delayed Message Delivery**: Schedule messages for delivery at a specific time or after a delay
- **Recurring Messages**: Repeat delivery at a fixed interval or on a cron schedule, with optional MaxOccurrences and Until termination conditions
- **Multiple Storage Backends**: Redis, PostgreSQL, and MongoDB implementations
- **High Availability Safe**: Uses atomic operations and 2-phase commit for crash safety
- **Automatic Recovery**: Recovers messages stuck in processing state after scheduler crashes
- **Retry with Backoff**: Configurable retry count and backoff strategy for failed deliveries
- **Dead-Letter Queue**: Optional DLQ integration for messages that exceed max retries
- **OpenTelemetry Metrics**: Counters, histograms, and gauges for monitoring
- **Configurable Polling**: Adjustable poll intervals and batch sizes
- **Cancellation Support**: Cancel scheduled messages before delivery
- **Sentinel Errors**: Use `errors.Is(err, scheduler.ErrNotFound)` for programmatic error handling
- **gRPC + HTTP API**: Read-only gRPC service with HTTP/JSON gateway for operational tooling

## Installation

```bash
go get github.com/rbaliyan/event-scheduler
```

## Quick Start

### Redis Scheduler

```go
package main

import (
    "context"
    "encoding/json"
    "time"

    "github.com/google/uuid"
    scheduler "github.com/rbaliyan/event-scheduler"
    "github.com/rbaliyan/event/v3/transport/channel"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()

    // Create Redis client
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    // Create transport (use your preferred transport: channel, Redis, NATS, etc.)
    t := channel.New()

    // Create scheduler with your transport
    sched, err := scheduler.NewRedisScheduler(rdb, t,
        scheduler.WithPollInterval(100*time.Millisecond),
        scheduler.WithBatchSize(100),
    )
    if err != nil {
        panic(err)
    }

    // Start the scheduler (blocks until stopped)
    go sched.Start(ctx)

    // Schedule a message for delivery in 1 hour
    payload, _ := json.Marshal(map[string]string{"order_id": "12345"})
    id := uuid.New().String()
    err = sched.Schedule(ctx, scheduler.Message{
        ID:          id,
        EventName:   "orders.reminder",
        Payload:     payload,
        ScheduledAt: time.Now().Add(time.Hour),
    })
    if err != nil {
        panic(err)
    }

    // Cancel a scheduled message
    err = sched.Cancel(ctx, id)
    if err != nil {
        panic(err)
    }

    // Stop gracefully
    sched.Stop(ctx)
}
```

### MongoDB Scheduler

```go
package main

import (
    "context"
    "time"

    scheduler "github.com/rbaliyan/event-scheduler"
    "github.com/rbaliyan/event/v3/transport/channel"
    "go.mongodb.org/mongo-driver/v2/mongo"
    mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

func main() {
    ctx := context.Background()

    // Connect to MongoDB (mongo-driver v2: Connect takes only ClientOptions, no context)
    client, err := mongo.Connect(mongoopts.Client().ApplyURI("mongodb://localhost:27017"))
    if err != nil {
        panic(err)
    }
    db := client.Database("myapp")

    // Create transport (use your preferred transport: channel, Redis, NATS, etc.)
    t := channel.New()

    // Create scheduler with custom collection name
    sched, err := scheduler.NewMongoScheduler(db, t,
        scheduler.WithPollInterval(100*time.Millisecond),
        scheduler.WithCollection("scheduled_jobs"),
    )
    if err != nil {
        panic(err)
    }

    // Create indexes for optimal performance
    if err := sched.EnsureIndexes(ctx); err != nil {
        panic(err)
    }

    // Start the scheduler
    go sched.Start(ctx)

    // Schedule messages...
}
```

### PostgreSQL Scheduler

```go
package main

import (
    "context"
    "database/sql"
    "time"

    scheduler "github.com/rbaliyan/event-scheduler"
    "github.com/rbaliyan/event/v3/transport/channel"
    _ "github.com/lib/pq"
)

func main() {
    ctx := context.Background()

    // Connect to PostgreSQL
    db, err := sql.Open("postgres", "postgres://localhost/myapp?sslmode=disable")
    if err != nil {
        panic(err)
    }

    // Create transport (use your preferred transport: channel, Redis, NATS, etc.)
    t := channel.New()

    // Create scheduler with custom table name
    sched, err := scheduler.NewPostgresScheduler(db, t,
        scheduler.WithPollInterval(100*time.Millisecond),
        scheduler.WithTable("my_scheduled_jobs"),
    )
    if err != nil {
        panic(err)
    }

    // Auto-create the table and indexes
    if err := sched.EnsureTable(ctx); err != nil {
        panic(err)
    }

    // Start the scheduler
    go sched.Start(ctx)

    // Schedule messages...
}
```

## API Reference

### Scheduler Interface

All scheduler implementations provide the same interface:

```go
type Scheduler interface {
    // Schedule adds a message for future delivery
    Schedule(ctx context.Context, msg Message) error

    // Cancel cancels a scheduled message
    Cancel(ctx context.Context, id string) error

    // Get retrieves a scheduled message by ID
    Get(ctx context.Context, id string) (*Message, error)

    // List returns scheduled messages matching the filter
    List(ctx context.Context, filter Filter) ([]*Message, error)

    // Start begins the scheduler polling loop (blocking)
    Start(ctx context.Context) error

    // Stop gracefully stops the scheduler
    Stop(ctx context.Context) error
}
```

### Message Structure

```go
type Message struct {
    ID              string            // Unique identifier (auto-generated if empty)
    EventName       string            // Event/topic to publish when delivered
    Payload         []byte            // Message data (typically JSON)
    Metadata        map[string]string // Additional key-value pairs
    ScheduledAt     time.Time         // When to deliver the message
    CreatedAt       time.Time         // When the message was scheduled
    RetryCount      int               // Number of delivery attempts (managed internally)
    Recurrence      *Recurrence       // Optional: repeat delivery periodically
    OccurrenceCount int               // How many times this message has been delivered
}
```

### Configuration Options

```go
// Set poll interval (default: 100ms)
scheduler.WithPollInterval(50 * time.Millisecond)

// Set batch size (default: 100)
scheduler.WithBatchSize(500)

// Set key prefix for Redis storage keys (default: "scheduler:")
scheduler.WithKeyPrefix("myapp:scheduler:")

// Set table name for PostgreSQL (default: "scheduled_messages")
scheduler.WithTable("my_scheduled_jobs")

// Set collection name for MongoDB (default: "scheduled_messages")
scheduler.WithCollection("my_scheduled_jobs")

// Set retry backoff strategy
scheduler.WithBackoff(&backoff.Exponential{
    Initial:    time.Second,
    Multiplier: 2.0,
    Max:        5 * time.Minute,
    Jitter:     0.1,
})

// Set maximum retry attempts (default: 0 = infinite)
scheduler.WithMaxRetries(5)

// Set dead-letter queue for failed messages (see Retry & DLQ section for the
// DLQFunc adapter needed to wire an event-dlq Manager)
scheduler.WithDLQ(dlqAdapter)

// Enable OpenTelemetry metrics
metrics, _ := scheduler.NewMetrics()
scheduler.WithMetrics(metrics)
```

### Error Handling

```go
// Check for not-found errors programmatically
_, err := sched.Get(ctx, "missing-id")
if errors.Is(err, scheduler.ErrNotFound) {
    // Message does not exist
}
```

### Filtering Scheduled Messages

```go
// List messages scheduled for the next hour
messages, err := sched.List(ctx, scheduler.Filter{
    EventName: "orders.reminder",
    Before:    time.Now().Add(time.Hour),
    Limit:     100,
})

// List all messages after a specific time
messages, err := sched.List(ctx, scheduler.Filter{
    After: time.Now(),
})
```

## Recurring Messages

Set `Recurrence` on a `Message` to repeat delivery periodically. After each successful delivery the scheduler reschedules the message automatically. The message is deleted when a termination condition is reached or when `Cancel` is called.

### Fixed interval

```go
err = sched.Schedule(ctx, scheduler.Message{
    EventName:   "reports.weekly",
    Payload:     payload,
    ScheduledAt: nextMonday,
    Recurrence: &scheduler.Recurrence{
        Type:           scheduler.RecurrenceInterval,
        Interval:       7 * 24 * time.Hour, // every week
        MaxOccurrences: 4,                   // stop after 4 deliveries
    },
})
```

### Cron schedule

```go
err = sched.Schedule(ctx, scheduler.Message{
    EventName:   "reminders.daily",
    Payload:     payload,
    ScheduledAt: time.Now(),
    Recurrence: &scheduler.Recurrence{
        Type:  scheduler.RecurrenceCron,
        Cron:  "0 9 * * 1-5", // 09:00 Mon-Fri
        Until: endOfQuarter,   // stop after this date
    },
})
```

### Termination conditions

| Field | Description |
|-------|-------------|
| `MaxOccurrences` | Delete after this many deliveries (0 = unlimited) |
| `Until` | Delete when the next fire time would be after this timestamp (zero = no end) |

If neither condition is set the message recurs indefinitely — call `Cancel(ctx, id)` to stop it.

### Recurrence across backends

All three backends (Redis, PostgreSQL, MongoDB) support recurring messages with identical behaviour. For PostgreSQL, call `MigrateAddRecurrence(ctx)` on existing tables that pre-date this feature.

## gRPC and HTTP API

The scheduler includes a read-only gRPC service with an HTTP/JSON gateway for operational tooling, dashboards, and monitoring.

### In-Process Setup (Recommended)

When the scheduler and HTTP server run in the same process, use `NewInProcessHandler` to avoid network overhead:

```go
import (
    "net/http"

    scheduler "github.com/rbaliyan/event-scheduler"
    "github.com/rbaliyan/event-scheduler/service"
    "github.com/rbaliyan/event-scheduler/gateway"
)

// Create the gRPC service from an existing scheduler
svc, err := service.New(sched)
if err != nil {
    panic(err)
}

// Create an HTTP handler (no gRPC network hop)
handler, err := gateway.NewInProcessHandler(ctx, svc)
if err != nil {
    panic(err)
}

// Mount on your HTTP router
http.Handle("/v1/", handler)
http.ListenAndServe(":8080", nil)
```

### Remote Proxy Setup

When the gRPC service runs separately, use `NewHandler` to proxy HTTP requests to the gRPC backend:

```go
handler, err := gateway.NewHandler(ctx, "scheduler-service:50051",
    gateway.WithTLS(nil),  // Use system TLS defaults
)
if err != nil {
    panic(err)
}
defer handler.Close()

http.Handle("/v1/", handler)
```

### HTTP Endpoints

| Method | Path | Description |
|--------|------|-------------|
| GET | `/v1/messages/{id}` | Get a scheduled message by ID |
| GET | `/v1/messages?event_name=...&limit=...&offset=...&before=...&after=...` | List messages with optional filters and pagination |
| GET | `/v1/health` | Scheduler health status |

Returned messages include recurrence details (`recurrence`, `occurrence_count`)
for recurring schedules. `total_count` reports the number of messages in the
response (`== len(messages)`), not a grand total across all pages.

### gRPC Service

The service can also be used directly with gRPC:

```go
import "google.golang.org/grpc"

svc, _ := service.New(sched)

grpcServer := grpc.NewServer()
svc.Register(grpcServer)
```

### Gateway Options

```go
// TLS with system defaults
gateway.WithTLS(nil)

// TLS with custom config
gateway.WithTLS(&tls.Config{MinVersion: tls.VersionTLS13})

// Insecure (development only)
gateway.WithInsecure()

// Additional gRPC dial options
gateway.WithDialOptions(grpc.WithAuthority("custom"))

// Custom ServeMux options
gateway.WithMuxOptions(runtime.WithErrorHandler(customHandler))
```

## Retry, Backoff, and Dead-Letter Queue

All three backends support identical retry behavior:

1. On publish failure, `RetryCount` is incremented
2. If `BackoffStrategy` is set, `ScheduledAt` is updated to `now + backoff delay`
3. If `MaxRetries > 0` and retries are exhausted, the message is sent to the DLQ (if configured) and discarded

`WithMaxRetries(n)` allows up to `n` retries after the initial attempt (so `n+1`
total delivery attempts). After the retries are exhausted, the message is sent to
the DLQ (if configured) and removed.

```go
import "github.com/rbaliyan/event/v3/backoff"

// t is your transport.Transport (e.g. channel.New(), redis transport, etc.)
sched, err := scheduler.NewRedisScheduler(rdb, t,
    scheduler.WithBackoff(&backoff.Exponential{
        Initial:    time.Second,
        Multiplier: 2.0,
        Max:        5 * time.Minute,
    }),
    scheduler.WithMaxRetries(5),
    scheduler.WithDLQ(dlq),
)
```

The `DeadLetterQueue` interface is small:

```go
type DeadLetterQueue interface {
    Store(ctx context.Context, params DLQStoreParams) error
}
```

A `dlq.Manager` from `github.com/rbaliyan/event-dlq` does **not** satisfy this
interface directly — its `Store` takes a `dlq.StoreParams`, not a
`scheduler.DLQStoreParams`. Adapt it with the `DLQFunc` helper:

```go
mgr, err := dlq.NewManager(dlqStore, republisher) // returns (*dlq.Manager, error)
if err != nil {
    return err
}

dlqAdapter := scheduler.DLQFunc(func(ctx context.Context, p scheduler.DLQStoreParams) error {
    return mgr.Store(ctx, dlq.StoreParams{
        EventName:  p.EventName,
        OriginalID: p.OriginalID,
        Payload:    p.Payload,
        Metadata:   p.Metadata,
        Err:        p.Err,
        RetryCount: p.RetryCount,
        Source:     p.Source,
    })
})

sched, err := scheduler.NewRedisScheduler(rdb, t,
    scheduler.WithMaxRetries(5),
    scheduler.WithDLQ(dlqAdapter),
)
```

## OpenTelemetry Metrics

```go
metrics, err := scheduler.NewMetrics(
    scheduler.WithNamespace("orders"),
    scheduler.WithMeterProvider(provider),
)
// t is your transport.Transport (e.g. channel.New(), redis transport, etc.)
sched, err := scheduler.NewRedisScheduler(rdb, t,
    scheduler.WithMetrics(metrics),
)
// Gauge callbacks (pending/stuck) are wired automatically when Start() is called.
```

Available metrics:
- `scheduled_messages_total` - counter of messages scheduled
- `scheduled_messages_delivered_total` - counter of successful deliveries
- `scheduled_messages_failed_total` - counter of failed deliveries
- `scheduled_messages_cancelled_total` - counter of cancelled messages
- `scheduled_messages_recovered_total` - counter of stuck messages recovered
- `scheduled_messages_dlq_total` - counter of messages sent to DLQ
- `scheduled_messages_rescheduled_total` - counter of recurring message reschedules
- `scheduled_messages_pending` - gauge of pending messages
- `scheduled_messages_stuck` - gauge of stuck messages
- `schedule_delivery_delay_seconds` - histogram of delivery delay
- `schedule_processing_duration_seconds` - histogram of processing time

## Store Implementations

### Redis

- Uses sorted sets with scheduled time as score
- Supports single node, Sentinel, and Cluster modes
- Atomic operations via Lua scripts for HA safety
- Automatic recovery of stuck messages

### MongoDB

- Uses a `scheduled_messages` collection
- Atomic claim via `FindOneAndUpdate`
- Indexes on `scheduled_at`, `event_name`, and `status`
- Automatic recovery of stuck messages

### PostgreSQL

- 2-phase claim using a `status`/`claimed_at` column, with `FOR UPDATE SKIP
  LOCKED` so concurrent instances claim disjoint batches; the transport publish
  happens outside any row lock
- Composite index on `(status, scheduled_at)` for efficient claiming
- Automatic recovery of stuck messages
- Use `EnsureTable()` to auto-create schema
- Use `MigrateAddRetryCount()` to add retry_count to tables created before v0.5.0
- Use `MigrateAddRecurrence()` to add recurrence columns to tables created before v0.7.0
- Use `MigrateAddProcessingState()` to add the `status`/`claimed_at` columns to tables created before 2-phase claim support

## High Availability

All implementations support running multiple scheduler instances for high availability:

1. **Atomic Claiming**: Messages are atomically claimed before processing
2. **2-Phase Processing**: Claim -> Publish -> Delete ensures at-least-once delivery
3. **Stuck Message Recovery**: Messages stuck in "processing" state are automatically recovered after a configurable timeout (default: 5 minutes)
4. **Graceful Shutdown**: `Stop()` waits for in-flight processing to complete; safe to call multiple times

```go
// Configure stuck message recovery timeout via constructor option
// t is your transport.Transport (e.g. channel.New(), redis transport, etc.)
sched, err := scheduler.NewRedisScheduler(rdb, t,
    scheduler.WithStuckDuration(10 * time.Minute),
)
```

## Testing

The default (untagged) suite is fully hermetic — no Docker required. Redis is
exercised through an in-memory [`miniredis`](https://github.com/alicebob/miniredis)
server, PostgreSQL through [`go-sqlmock`](https://github.com/DATA-DOG/go-sqlmock),
MongoDB codec logic through pure round-trips, and metrics through an OpenTelemetry
`ManualReader`. Integration tests against real backends are gated behind the
`integration` build tag.

```bash
# Unit tests (hermetic, fast) + race detector
go test -race ./...

# Coverage with the CI gate (>= 70%)
just test-cover-gate

# Benchmarks with allocation stats (pure logic + miniredis-backed backends)
just bench                 # go test -run '^$' -bench=. -benchmem ./...
go test -bench=. -benchmem -memprofile=mem.out -cpuprofile=cpu.out ./...  # add profiles
# CI compares each PR's benchmarks against the merge-base with benchstat and
# fails on a >=2x regression (.github/workflows/ci.yml: bench-regression job).

# Fuzzing: run the seed corpora, or fuzz a single target
just fuzz-seed
just fuzz FuzzComputeNextSchedule 30s
# Fuzz targets: message JSON round-trip, recurrence validation (validator vs
# nextCronTime), computeNextSchedule liveness, recurrence decode, the MongoDB
# decode boundary, retry/DLQ decision, proto message conversion, and the
# gateway ListRequest->Filter decode. A nightly workflow
# (.github/workflows/fuzz-nightly.yml) runs a longer -race campaign per target.
#
# Triage a nightly failure: download the failing-input artifact, drop it under
# testdata/fuzz/<Target>/, then `go test -run <Target>/<hash>` reproduces it.

# Smoke tests only (constructor guards, in-process round-trips)
just smoke
```

### Integration tests (real Redis, MongoDB, PostgreSQL)

```bash
just compose-up           # start backends via docker-compose
just test-integration     # run the -tags=integration suite (race-enabled)
just test-pg              # or a single backend: test-pg / test-mongo / test-redis
just compose-down

# Or point at your own services:
REDIS_ADDR=redis:6379 \
MONGO_URI=mongodb://mongo:27017 \
POSTGRES_DSN="postgres://user:pass@pg:5432/test?sslmode=disable" \
  go test -tags integration -race -count=1 -timeout 120s ./...
```

CI runs the hermetic suite + coverage gate, smoke, fuzz (seeds + short run),
benchmarks, and the full integration suite against service containers on every PR.

## Best Practices

1. **Use unique message IDs** for deduplication and cancellation
2. **Set appropriate poll intervals** (100ms-1s for most cases)
3. **Handle idempotency** in your message handlers
4. **Monitor scheduler lag** (time between scheduled and actual delivery)
5. **Run multiple instances** for high availability
6. **Create database indexes** before deploying to production
7. **Configure MaxRetries and DLQ** to prevent infinite retry loops
8. **Use `errors.Is(err, scheduler.ErrNotFound)`** instead of string matching

## License

MIT License
