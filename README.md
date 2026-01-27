# Event Scheduler

A production-grade delayed and scheduled message delivery library for Go. Schedule messages for future delivery with support for Redis, PostgreSQL, and MongoDB backends.

## Features

- **Delayed Message Delivery**: Schedule messages for delivery at a specific time or after a delay
- **Multiple Storage Backends**: Redis, PostgreSQL, and MongoDB implementations
- **High Availability Safe**: Uses atomic operations and 2-phase commit for crash safety
- **Automatic Recovery**: Recovers messages stuck in processing state after scheduler crashes
- **Configurable Polling**: Adjustable poll intervals and batch sizes
- **Cancellation Support**: Cancel scheduled messages before delivery

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

    "github.com/rbaliyan/event-scheduler"
    "github.com/redis/go-redis/v9"
)

func main() {
    ctx := context.Background()

    // Create Redis client
    rdb := redis.NewClient(&redis.Options{
        Addr: "localhost:6379",
    })

    // Create scheduler with your transport
    sched := scheduler.NewRedisScheduler(rdb, transport,
        scheduler.WithPollInterval(100*time.Millisecond),
        scheduler.WithBatchSize(100),
    )

    // Start the scheduler (blocks until stopped)
    go sched.Start(ctx)

    // Schedule a message for delivery in 1 hour
    payload, _ := json.Marshal(map[string]string{"order_id": "12345"})
    id := uuid.New().String()
    err := sched.Schedule(ctx, scheduler.Message{
        ID:          id,
        EventName:   "orders.reminder",
        Payload:     payload,
        ScheduledAt: time.Now().Add(time.Hour),
    })
    if err != nil {
        panic(err)
    }

    // Or schedule for a specific time
    deliveryTime := time.Now().Add(24 * time.Hour)
    err = sched.Schedule(ctx, scheduler.Message{
        ID:          uuid.New().String(),
        EventName:   "orders.reminder",
        Payload:     payload,
        ScheduledAt: deliveryTime,
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

    "github.com/rbaliyan/event-scheduler"
    "go.mongodb.org/mongo-driver/mongo"
    "go.mongodb.org/mongo-driver/mongo/options"
)

func main() {
    ctx := context.Background()

    // Connect to MongoDB
    client, err := mongo.Connect(ctx, options.Client().ApplyURI("mongodb://localhost:27017"))
    if err != nil {
        panic(err)
    }
    db := client.Database("myapp")

    // Create scheduler
    sched := scheduler.NewMongoScheduler(db, transport,
        scheduler.WithPollInterval(100*time.Millisecond),
    )

    // Optionally customize collection name
    sched.WithCollection("scheduled_jobs")

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

    "github.com/rbaliyan/event-scheduler"
    _ "github.com/lib/pq"
)

func main() {
    ctx := context.Background()

    // Connect to PostgreSQL
    db, err := sql.Open("postgres", "postgres://localhost/myapp?sslmode=disable")
    if err != nil {
        panic(err)
    }

    // Create scheduler
    sched := scheduler.NewPostgresScheduler(db, transport,
        scheduler.WithPollInterval(100*time.Millisecond),
    )

    // Optionally customize table name
    sched.WithTable("scheduled_jobs")

    // Start the scheduler
    go sched.Start(ctx)

    // Schedule messages...
}
```

**Required PostgreSQL Schema:**

```sql
CREATE TABLE scheduled_messages (
    id           VARCHAR(36) PRIMARY KEY,
    event_name   VARCHAR(255) NOT NULL,
    payload      BYTEA NOT NULL,
    metadata     JSONB,
    scheduled_at TIMESTAMP NOT NULL,
    created_at   TIMESTAMP NOT NULL DEFAULT NOW()
);

CREATE INDEX idx_scheduled_due ON scheduled_messages(scheduled_at);
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
    ID          string            // Unique identifier (auto-generated if empty)
    EventName   string            // Event/topic to publish when delivered
    Payload     []byte            // Message data (typically JSON)
    Metadata    map[string]string // Additional key-value pairs
    ScheduledAt time.Time         // When to deliver the message
    CreatedAt   time.Time         // When the message was scheduled
}
```

### Configuration Options

```go
// Set poll interval (default: 100ms)
scheduler.WithPollInterval(50 * time.Millisecond)

// Set batch size (default: 100)
scheduler.WithBatchSize(500)

// Set key prefix for storage keys (default: "scheduler:")
scheduler.WithKeyPrefix("myapp:scheduler:")
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

- Uses `FOR UPDATE SKIP LOCKED` for safe concurrent processing
- Transactional batch processing
- Index on `scheduled_at` for efficient queries

## High Availability

All implementations support running multiple scheduler instances for high availability:

1. **Atomic Claiming**: Messages are atomically claimed before processing
2. **2-Phase Processing**: Claim -> Publish -> Delete ensures at-least-once delivery
3. **Stuck Message Recovery**: Messages stuck in "processing" state are automatically recovered after a configurable timeout (default: 5 minutes)

```go
// Configure stuck message recovery timeout
sched.WithStuckDuration(10 * time.Minute)
```

## Best Practices

1. **Use unique message IDs** for deduplication and cancellation
2. **Set appropriate poll intervals** (100ms-1s for most cases)
3. **Handle idempotency** in your message handlers
4. **Monitor scheduler lag** (time between scheduled and actual delivery)
5. **Run multiple instances** for high availability
6. **Create database indexes** before deploying to production

## License

MIT License
