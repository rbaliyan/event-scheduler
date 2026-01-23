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

Event Scheduler (`github.com/rbaliyan/event-scheduler`) is a production-grade delayed and scheduled message delivery library for Go. It provides storage backends for scheduling messages to be delivered at a future time.

## Architecture

### Core Components

**Scheduler Interface (scheduler.go)** - Defines the `Scheduler` interface and common types:
- `Message`: Represents a scheduled message with ID, payload, metadata, and delivery time
- `Filter`: Criteria for listing scheduled messages
- `Options`: Configuration for poll interval, batch size, and key prefix
- Option functions: `WithPollInterval`, `WithBatchSize`, `WithKeyPrefix`

**Redis Scheduler (redis.go)** - Production-ready Redis implementation:
- Uses sorted sets with scheduled time as score
- Lua scripts for atomic claim operations
- 2-phase processing: claim -> publish -> delete
- Automatic recovery of stuck messages
- Supports single node, Sentinel, and Cluster modes

**MongoDB Scheduler (mongodb.go)** - MongoDB implementation:
- Uses `scheduled_messages` collection
- `FindOneAndUpdate` for atomic claiming
- Status field tracks pending/processing state
- Automatic stuck message recovery
- Indexes for efficient querying

**PostgreSQL Scheduler (postgres.go)** - PostgreSQL implementation:
- Uses `FOR UPDATE SKIP LOCKED` for concurrent safety
- Transactional batch processing
- JSONB metadata storage

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
    |
    +-> Delete from storage
```

### High Availability Pattern

All implementations use a 2-phase approach for crash safety:

1. **Claim Phase**: Atomically mark message as "processing"
2. **Publish Phase**: Send to transport
3. **Delete Phase**: Remove from storage

If scheduler crashes after step 1, the `recoverStuck()` function moves messages back to "pending" after `stuckDuration` (default: 5 minutes).

### Key Design Patterns

- **Interface-Based Design**: All implementations satisfy the `Scheduler` interface
- **Functional Options**: Configuration via `Option` functions
- **Graceful Shutdown**: Stop channel signals polling loop to exit
- **Atomic Operations**: Lua scripts (Redis) / FindOneAndUpdate (MongoDB) / FOR UPDATE SKIP LOCKED (PostgreSQL)

### Default Configuration

- Poll Interval: 100ms
- Batch Size: 100
- Key Prefix: "scheduler:"
- Stuck Duration: 5 minutes

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
    "claimed_at": ISODate (optional)
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
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);
```

**Redis Keys**
- `{prefix}messages`: Sorted set, score=unix timestamp, member=JSON message
- `{prefix}processing`: Sorted set for messages being processed

## Dependencies

- `github.com/rbaliyan/event/v3` - Transport interface and message types
- `github.com/google/uuid` - UUID generation for message IDs
- `github.com/redis/go-redis/v9` - Redis client
- `go.mongodb.org/mongo-driver` - MongoDB driver
- `go.opentelemetry.io/otel/trace` - Trace context for messages
