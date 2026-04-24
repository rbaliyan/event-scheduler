# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
go test ./...          # Run all tests
go test -run TestName  # Run a specific test
go build ./...         # Build all packages
go mod tidy            # Clean up dependencies
buf generate           # Regenerate proto (or: just proto)
```

## Project Overview

Event Scheduler (`github.com/rbaliyan/event-scheduler`) is a production-grade delayed and scheduled message delivery library for Go. It provides storage backends for scheduling messages to be delivered at a future time, with retry/backoff support and optional dead-letter queue integration.

## Architecture

### Core Components

**Scheduler Interface (scheduler.go)** - Defines the `Scheduler` interface and common types:
- `Message`: Scheduled message with ID, payload, metadata, delivery time, retry count, and optional `*Recurrence`
- `Recurrence`: Periodic re-delivery config — `Type` (`RecurrenceInterval`/`RecurrenceCron`), `Interval`, `Cron`, `MaxOccurrences`, `Until`
- `Filter`: Criteria for listing scheduled messages
- `BackoffStrategy`: Interface for retry backoff (compatible with `event/v3/backoff`)
- `DeadLetterQueue`: Interface for DLQ storage (compatible with `event-dlq` Manager)
- Option functions: `WithPollInterval`, `WithBatchSize`, `WithKeyPrefix`, `WithTable`, `WithCollection`, `WithMetrics`, `WithBackoff`, `WithMaxRetries`, `WithDLQ`, `WithAdaptivePolling`, `WithMinPollInterval`, `WithMaxPollInterval`, `WithStuckDuration`, `WithLogger`, `WithNotifyListener`

**Redis Scheduler (redis.go)** - Production-ready Redis implementation:
- Uses sorted sets with scheduled time as score
- Lua scripts for atomic claim operations
- 2-phase processing: claim -> publish -> delete
- Automatic recovery of stuck messages
- Supports single node, Sentinel, and Cluster modes
- Full retry/backoff/maxRetries/DLQ support

**MongoDB Scheduler (mongodb.go)** - MongoDB implementation:
- Uses `scheduled_messages` collection (configurable via `WithCollection`)
- `FindOneAndUpdate` for atomic claiming
- Status field tracks pending/processing state
- Automatic stuck message recovery
- Indexes for efficient querying
- Full retry/backoff/maxRetries/DLQ support

**PostgreSQL Scheduler (postgres.go)** - PostgreSQL implementation:
- Uses `scheduled_messages` table (configurable via `WithTable`)
- Uses `FOR UPDATE SKIP LOCKED` for concurrent safety
- Transactional batch processing
- JSONB metadata storage
- `EnsureTable()` for auto-creating the schema
- `MigrateAddRetryCount()` for upgrading existing tables
- Full retry/backoff/maxRetries/DLQ support

**gRPC Service (service/)** - Read-only gRPC API for operational tooling:
- `service.New(scheduler) (*Service, error)` creates the gRPC server
- RPCs: `Get` (by ID), `List` (with filter), `Health` (scheduler status)
- `HealthStatus` proto enum: `HEALTHY`, `DEGRADED`, `UNHEALTHY`
- `ListResponse` includes `total_count` for truncation detection
- Error mapping: `ErrNotFound` -> `NotFound`, `context.Canceled` -> `Canceled`, etc.
- `Register(server)` registers the service with a gRPC server

**HTTP Gateway (gateway/)** - gRPC-Gateway HTTP/JSON handler:
- `NewHandler(ctx, grpcAddr, ...Option) (*Handler, error)` - remote proxy to gRPC backend
- `NewInProcessHandler(ctx, svc, ...Option) (*Handler, error)` - direct in-process calls (no network)
- HTTP routes: `GET /v1/messages/{id}`, `GET /v1/messages`, `GET /v1/health`
- Options: `WithTLS`, `WithInsecure`, `WithDialOptions`, `WithMuxOptions`
- `Handler.Close()` releases resources (idempotent, safe to call multiple times)

**Proto Definition (proto/scheduler/v1/scheduler.proto)**:
- Read-only SchedulerService (intentionally omits Schedule/Cancel)
- Uses `google.api.http` annotations for REST mapping
- Generated with `buf generate` (buf.gen.yaml, buf.yaml)

**Metrics (metrics.go)** - OpenTelemetry metrics:
- Counters: scheduled, delivered, failed, cancelled, recovered, DLQ sent
- Gauges: pending messages, stuck messages (via observable callbacks)
- Histograms: delivery delay, processing duration
- Optional namespace prefix for multi-instance deployments

### Data Flow

```
Schedule() -> Validate recurrence (if set), store message with scheduled time
    |
Start() -> Polling loop (at PollInterval)
    |
processDue() -> Find messages where scheduled_at <= now
    |
    +-> Claim message (atomic operation)
    |
    +-> Publish to transport
    |       |
    |       +-> Success: computeNextSchedule()
    |       |       |
    |       |       +-> terminal=true (one-shot or termination reached):
    |       |       |       Delete from storage
    |       |       |
    |       |       +-> terminal=false (recurring, next fire computed):
    |       |               Reschedule at nextAt with updated occurrence_count
    |       |
    |       +-> Failure: Check retry logic
    |               |
    |               +-> RetryCount < MaxRetries: Increment count, apply backoff, reschedule
    |               |
    |               +-> RetryCount >= MaxRetries: Send to DLQ (if configured), discard
    |
    +-> recoverStuck() -> Reset messages stuck in "processing" state
```

### Retry and Backoff

All three backends have identical retry behavior:
1. On publish failure, `RetryCount` is incremented
2. If `MaxRetries > 0` and `RetryCount >= MaxRetries`, message is discarded (sent to DLQ if configured)
3. If `BackoffStrategy` is set, `ScheduledAt` is updated to `now + backoff.NextDelay(retryCount)`
4. Backoff state is reset at the start of each processing cycle
5. Without backoff, failed messages are retried on the next poll

### Adaptive Polling

All three backends support adaptive poll interval adjustment to optimize for both low latency and reduced database load:

```go
scheduler := NewRedisScheduler(client, transport,
    WithAdaptivePolling(true),        // Enable adaptive polling
    WithPollInterval(100*time.Millisecond),  // Initial/base interval
    WithMinPollInterval(10*time.Millisecond), // Minimum interval (high activity)
    WithMaxPollInterval(5*time.Second),       // Maximum interval (low activity)
)
```

**Behavior:**
- When messages are found and processed, the interval decreases (more activity expected)
- When no messages are found, the interval increases (less activity expected)
- The interval stays within the configured min/max bounds

**Adjustment Algorithm:**
- Messages found: `interval = interval * 3/4` (decrease by 25%)
- No messages: `interval = interval * 3/2` (increase by 50%)

This reduces database load during idle periods while maintaining low latency during bursts of activity.

### Dead-Letter Queue

The `DeadLetterQueue` interface provides loose coupling to DLQ implementations:
```go
type DeadLetterQueue interface {
    Store(ctx context.Context, params DLQStoreParams) error
}
```
Use `DLQFunc` to adapt a `dlq.Manager` from `github.com/rbaliyan/event-dlq` without a direct import:
```go
mgr, _ := dlq.NewManager(store, republisher)
scheduler.WithDLQ(scheduler.DLQFunc(func(ctx context.Context, p scheduler.DLQStoreParams) error {
    return mgr.Store(ctx, dlq.StoreParams{
        EventName: p.EventName, OriginalID: p.OriginalID,
        Payload: p.Payload, Metadata: p.Metadata,
        Err: p.Err, RetryCount: p.RetryCount, Source: p.Source,
    })
}))
```

### High Availability Pattern

All implementations use a 2-phase approach for crash safety:

1. **Claim Phase**: Atomically mark message as "processing"
2. **Publish Phase**: Send to transport
3. **Delete Phase**: Remove from storage

If scheduler crashes after step 1, the `recoverStuck()` function moves messages back to "pending" after `stuckDuration` (default: 5 minutes).

**Delivery semantics**: At-least-once. Messages may be delivered more than once in crash/recovery scenarios.

**Graceful shutdown**: The `Start()` polling loop runs `processDue()` synchronously. When `Stop()` is called, it signals the stop channel and waits. The current `processDue()` call completes before the loop exits.

### Key Design Patterns

- **Interface-Based Design**: All implementations satisfy the `Scheduler` interface; `BackoffStrategy` and `DeadLetterQueue` are interfaces for loose coupling
- **Functional Options**: Configuration via `Option` functions
- **Graceful Shutdown**: Stop channel signals polling loop to exit; in-flight processing completes
- **Atomic Operations**: Lua scripts (Redis) / FindOneAndUpdate (MongoDB) / FOR UPDATE SKIP LOCKED (PostgreSQL)

### Health Check

All scheduler implementations implement the `HealthChecker` interface for monitoring and readiness probes:

```go
type HealthChecker interface {
    Health(ctx context.Context) *HealthCheckResult
}

type HealthCheckResult struct {
    Status          HealthStatus   // healthy, degraded, unhealthy
    Message         string
    Latency         time.Duration
    PendingMessages int64
    StuckMessages   int64
    Details         map[string]any
    CheckedAt       time.Time
}
```

The health check verifies:
- Database connectivity (Redis ping, MongoDB ping, PostgreSQL ping)
- Pending message count
- Stuck message count (Redis/MongoDB only)

Returns `HealthStatusHealthy` if database is responsive and no stuck messages.
Returns `HealthStatusDegraded` if stuck messages exist.
Returns `HealthStatusUnhealthy` if database is not responsive.

### Default Configuration

- Poll Interval: 100ms
- Min Poll Interval: 10ms (for adaptive polling)
- Max Poll Interval: 5s (for adaptive polling)
- Adaptive Polling: disabled
- Batch Size: 100
- Key Prefix: "scheduler:" (Redis)
- Table: "scheduled_messages" (PostgreSQL)
- Collection: "scheduled_messages" (MongoDB)
- Stuck Duration: 5 minutes
- Max Retries: 0 (infinite)
- Backoff: nil (immediate retry)
- DLQ: nil (discard on max retries)

### Transport Integration

The scheduler publishes messages using the `transport.Transport` interface from `github.com/rbaliyan/event/v3/transport`. When a message is due:

1. Scheduler creates a `message.Message` with the payload and metadata
2. Adds `scheduled_message_id` and `scheduled_at` to metadata
3. Calls `transport.Publish(ctx, eventName, msg)`

### Storage Schemas

**MongoDB Collection** (default: `scheduled_messages`, configurable via `WithCollection`)
```
{
    "_id": string,
    "event_name": string,
    "payload": Binary,
    "metadata": object,
    "scheduled_at": ISODate,
    "created_at": ISODate,
    "status": string (pending/processing),
    "claimed_at": ISODate (optional),
    "retry_count": int (optional, default 0),
    "recurrence_type": string (optional, "interval"|"cron"),
    "recurrence_value": string (optional, duration string or cron expr),
    "max_occurrences": int (optional),
    "until": ISODate (optional),
    "occurrence_count": int (optional, default 0)
}
```

**PostgreSQL Table** (default: `scheduled_messages`, configurable via `WithTable`)
```sql
CREATE TABLE scheduled_messages (
    id               VARCHAR(36) PRIMARY KEY,
    event_name       VARCHAR(255) NOT NULL,
    payload          BYTEA NOT NULL,
    metadata         JSONB,
    scheduled_at     TIMESTAMP NOT NULL,
    created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
    retry_count      INTEGER NOT NULL DEFAULT 0,
    recurrence_type  VARCHAR(10),
    recurrence_value TEXT,
    max_occurrences  INTEGER NOT NULL DEFAULT 0,
    until            TIMESTAMP,
    occurrence_count INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_scheduled_due ON scheduled_messages(scheduled_at);
```

Use `EnsureTable()` to auto-create. Migrations for existing tables:
- `MigrateAddRetryCount()` — adds retry_count (tables pre-v0.5.0)
- `MigrateAddRecurrence()` — adds the 5 recurrence columns (tables pre-v0.7.0)

**Redis Keys**
- `{prefix}messages`: Sorted set, score=unix timestamp, member=msgID
- `{prefix}processing`: Sorted set, score=claimed_unix, member=msgID
- `{prefix}index`: Hash, field=msgID, value=JSON (includes recurrence fields)
- `{prefix}events:{name}`: Per-event sorted set, score=unix timestamp, member=msgID

## Dependencies

- `github.com/rbaliyan/event/v3` - Transport interface and message types
- `github.com/google/uuid` - UUID generation for message IDs
- `github.com/redis/go-redis/v9` - Redis client
- `go.mongodb.org/mongo-driver` - MongoDB driver
- `go.opentelemetry.io/otel` - OpenTelemetry metrics and tracing
- `google.golang.org/grpc` - gRPC server and client
- `google.golang.org/protobuf` - Protocol Buffers runtime
- `github.com/grpc-ecosystem/grpc-gateway/v2` - HTTP/JSON gateway for gRPC

## Related Libraries

- `github.com/rbaliyan/event/v3` - Core event bus with transports (Redis Streams, Kafka, NATS, Channel), type-safe generics, middleware, idempotency, backoff strategies
- `github.com/rbaliyan/event-dlq` - Dead-letter queue manager; `dlq.Manager` satisfies the `DeadLetterQueue` interface
