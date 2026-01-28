# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Build Commands

```bash
go test ./...          # Run all tests
go test -run TestName  # Run a specific test
go build ./...         # Build all packages
go mod tidy            # Clean up dependencies
```

## Project Overview

Event Scheduler (`github.com/rbaliyan/event-scheduler`) is a production-grade delayed and scheduled message delivery library for Go. It provides storage backends for scheduling messages to be delivered at a future time, with retry/backoff support and optional dead-letter queue integration.

## Architecture

### Core Components

**Scheduler Interface (scheduler.go)** - Defines the `Scheduler` interface and common types:
- `Message`: Scheduled message with ID, payload, metadata, delivery time, and retry count
- `Filter`: Criteria for listing scheduled messages
- `BackoffStrategy`: Interface for retry backoff (compatible with `event/v3/backoff`)
- `DeadLetterQueue`: Interface for DLQ storage (compatible with `event-dlq` Manager)
- Option functions: `WithPollInterval`, `WithBatchSize`, `WithKeyPrefix`, `WithMetrics`, `WithBackoff`, `WithMaxRetries`, `WithDLQ`

**Redis Scheduler (redis.go)** - Production-ready Redis implementation:
- Uses sorted sets with scheduled time as score
- Lua scripts for atomic claim operations
- 2-phase processing: claim -> publish -> delete
- Automatic recovery of stuck messages
- Supports single node, Sentinel, and Cluster modes
- Full retry/backoff/maxRetries/DLQ support

**MongoDB Scheduler (mongodb.go)** - MongoDB implementation:
- Uses `scheduled_messages` collection
- `FindOneAndUpdate` for atomic claiming
- Status field tracks pending/processing state
- Automatic stuck message recovery
- Indexes for efficient querying
- Full retry/backoff/maxRetries/DLQ support

**PostgreSQL Scheduler (postgres.go)** - PostgreSQL implementation:
- Uses `FOR UPDATE SKIP LOCKED` for concurrent safety
- Transactional batch processing
- JSONB metadata storage
- `EnsureTable()` for auto-creating the schema
- `MigrateAddRetryCount()` for upgrading existing tables
- Full retry/backoff/maxRetries/DLQ support

**Metrics (metrics.go)** - OpenTelemetry metrics:
- Counters: scheduled, delivered, failed, cancelled, recovered, DLQ sent
- Gauges: pending messages, stuck messages (via observable callbacks)
- Histograms: delivery delay, processing duration
- Optional namespace prefix for multi-instance deployments

### Data Flow

```
Schedule() -> Store message with scheduled time
    |
Start() -> Polling loop (at PollInterval)
    |
processDue() -> Find messages where scheduled_at <= now
    |
    +-> Claim message (atomic operation)
    |
    +-> Publish to transport
    |       |
    |       +-> Success: Delete from storage
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

### Dead-Letter Queue

The `DeadLetterQueue` interface provides loose coupling to DLQ implementations:
```go
type DeadLetterQueue interface {
    Store(ctx context.Context, eventName, originalID string, payload []byte,
          metadata map[string]string, err error, retryCount int, source string) error
}
```
The `dlq.Manager` from `github.com/rbaliyan/event-dlq` satisfies this interface.

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

### Default Configuration

- Poll Interval: 100ms
- Batch Size: 100
- Key Prefix: "scheduler:"
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

**MongoDB Collection: `scheduled_messages`**
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
    "retry_count": int (optional, default 0)
}
```

**PostgreSQL Table: `scheduled_messages`**
```sql
CREATE TABLE scheduled_messages (
    id           VARCHAR(36) PRIMARY KEY,
    event_name   VARCHAR(255) NOT NULL,
    payload      BYTEA NOT NULL,
    metadata     JSONB,
    scheduled_at TIMESTAMP NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
    retry_count  INTEGER NOT NULL DEFAULT 0
);
CREATE INDEX idx_scheduled_due ON scheduled_messages(scheduled_at);
```

Use `EnsureTable()` to auto-create, or `MigrateAddRetryCount()` to add the column to existing tables.

**Redis Keys**
- `{prefix}messages`: Sorted set, score=unix timestamp, member=JSON message
- `{prefix}processing`: Sorted set for messages being processed

## Dependencies

- `github.com/rbaliyan/event/v3` - Transport interface and message types
- `github.com/google/uuid` - UUID generation for message IDs
- `github.com/redis/go-redis/v9` - Redis client
- `go.mongodb.org/mongo-driver` - MongoDB driver
- `go.opentelemetry.io/otel` - OpenTelemetry metrics and tracing

## Related Libraries

- `github.com/rbaliyan/event/v3` - Core event bus with transports (Redis Streams, Kafka, NATS, Channel), type-safe generics, middleware, idempotency, backoff strategies
- `github.com/rbaliyan/event-dlq` - Dead-letter queue manager; `dlq.Manager` satisfies the `DeadLetterQueue` interface
