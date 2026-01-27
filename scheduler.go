// Package scheduler provides delayed and scheduled message delivery.
//
// Scheduled messages are stored and delivered at a specified future time.
// This enables use cases like:
//   - Delayed retries with exponential backoff
//   - Reminder notifications
//   - Time-based workflows
//   - Scheduled batch processing
//
// # Overview
//
// The package provides:
//   - Scheduler interface for scheduling and managing messages
//   - RedisScheduler: Production-ready Redis implementation
//   - PostgresScheduler: PostgreSQL implementation
//   - MongoScheduler: MongoDB implementation
//
// # Basic Usage
//
//	scheduler := scheduler.NewRedisScheduler(redisClient, transport)
//	go scheduler.Start(ctx)
//
//	// Schedule a message for 1 hour from now
//	err := scheduler.Schedule(ctx, scheduler.Message{
//	    ID:          uuid.New().String(),
//	    EventName:   "orders.reminder",
//	    Payload:     payload,
//	    ScheduledAt: time.Now().Add(time.Hour),
//	})
//
// # Convenience Methods
//
//	// Schedule for a specific time
//	id, err := scheduler.ScheduleAt(ctx, "orders.reminder", payload, metadata, futureTime)
//
//	// Schedule after a delay
//	id, err := scheduler.ScheduleAfter(ctx, "orders.reminder", payload, metadata, time.Hour)
//
// # Cancellation
//
//	// Cancel a scheduled message
//	err := scheduler.Cancel(ctx, messageID)
//
// # Architecture
//
// The scheduler uses a polling loop to check for due messages:
//  1. Poll storage at PollInterval for messages where ScheduledAt <= now
//  2. Publish due messages to the transport
//  3. Remove delivered messages from storage
//
// For Redis, this uses sorted sets with scheduled time as the score.
// For SQL databases, this uses indexed queries on the scheduled_at column.
//
// # Best Practices
//
//   - Use unique message IDs to prevent duplicates
//   - Set appropriate poll intervals (100ms-1s for most cases)
//   - Handle idempotency in message handlers
//   - Monitor scheduler lag (time between scheduled and actual delivery)
package scheduler

import (
	"context"
	"time"
)

// Message represents a scheduled message.
//
// A Message contains the data to be delivered and metadata about when
// to deliver it.
//
// Example:
//
//	msg := scheduler.Message{
//	    ID:          uuid.New().String(),
//	    EventName:   "orders.reminder",
//	    Payload:     jsonPayload,
//	    Metadata:    map[string]string{"order_id": orderID},
//	    ScheduledAt: time.Now().Add(24 * time.Hour),
//	}
type Message struct {
	// ID is a unique identifier for the message.
	// Used for cancellation and deduplication.
	ID string

	// EventName is the event/topic to publish to when delivered.
	EventName string

	// Payload is the message data (typically JSON).
	Payload []byte

	// Metadata contains additional key-value pairs for the message.
	Metadata map[string]string

	// ScheduledAt is when the message should be delivered.
	ScheduledAt time.Time

	// CreatedAt is when the message was scheduled.
	CreatedAt time.Time

	// RetryCount tracks the number of delivery attempts.
	// This is used internally for backoff calculations.
	RetryCount int `json:"retry_count,omitempty"`
}

// Scheduler schedules messages for future delivery.
//
// Implementations poll for due messages and publish them to a transport.
// All implementations must be safe for concurrent use.
//
// Implementations:
//   - RedisScheduler: Uses Redis sorted sets
//   - PostgresScheduler: Uses PostgreSQL
//   - MongoScheduler: Uses MongoDB
type Scheduler interface {
	// Schedule adds a message for future delivery.
	// The message will be published to EventName at ScheduledAt.
	Schedule(ctx context.Context, msg Message) error

	// ScheduleAt is a convenience method to schedule for a specific time.
	// Returns the generated message ID.
	ScheduleAt(ctx context.Context, eventName string, payload []byte, metadata map[string]string, at time.Time) (string, error)

	// ScheduleAfter is a convenience method to schedule after a delay.
	// Returns the generated message ID.
	ScheduleAfter(ctx context.Context, eventName string, payload []byte, metadata map[string]string, delay time.Duration) (string, error)

	// Cancel cancels a scheduled message before delivery.
	// Returns error if message not found.
	Cancel(ctx context.Context, id string) error

	// Get retrieves a scheduled message by ID.
	// Returns error if message not found.
	Get(ctx context.Context, id string) (*Message, error)

	// List returns scheduled messages matching the filter.
	// Returns empty slice if no matches.
	List(ctx context.Context, filter Filter) ([]*Message, error)

	// Start begins the scheduler polling loop.
	// This method blocks until the context is cancelled or Stop is called.
	Start(ctx context.Context) error

	// Stop gracefully stops the scheduler.
	// Waits for in-flight operations to complete.
	Stop(ctx context.Context) error
}

// Filter specifies criteria for listing scheduled messages.
//
// All fields are optional. Empty filter returns all messages.
//
// Example:
//
//	// Get reminder messages scheduled for the next hour
//	filter := scheduler.Filter{
//	    EventName: "orders.reminder",
//	    Before:    time.Now().Add(time.Hour),
//	    Limit:     100,
//	}
type Filter struct {
	// EventName filters by event name (empty = all events).
	EventName string

	// Before returns messages scheduled before this time (zero = no maximum).
	Before time.Time

	// After returns messages scheduled after this time (zero = no minimum).
	After time.Time

	// Limit is the maximum number of messages to return (0 = no limit).
	Limit int
}

// BackoffStrategy defines a backoff strategy for retry logic.
// This interface matches backoff.Strategy from the main event library
// (github.com/rbaliyan/event/v3/backoff) for loose coupling. Implementations
// from that package can be used directly without importing this package.
//
// Implementations must be safe for concurrent use.
type BackoffStrategy interface {
	// NextDelay returns the delay for the given attempt (0-indexed).
	// Attempt 0 is the first retry, not the initial attempt.
	NextDelay(attempt int) time.Duration

	// Reset resets the strategy state if any.
	Reset()
}

// options configures the scheduler behavior.
//
// Use the With* functions to configure options:
//
//	scheduler := NewRedisScheduler(client, transport,
//	    WithPollInterval(100*time.Millisecond),
//	    WithBatchSize(50),
//	)
type options struct {
	// pollInterval is how often to check for due messages.
	// Lower values reduce delivery latency but increase load.
	// Default: 100ms
	pollInterval time.Duration

	// batchSize is the maximum number of messages to process per poll.
	// Default: 100
	batchSize int

	// keyPrefix is the prefix for storage keys.
	// Default: "scheduler:"
	keyPrefix string

	// metrics is the optional metrics instance for recording scheduler metrics.
	// When nil, no metrics are recorded.
	metrics *Metrics

	// backoff is the optional backoff strategy for retry delivery.
	// When a message fails to deliver, this determines how long to wait
	// before the next retry attempt.
	// When nil, failed messages are retried immediately on the next poll.
	backoff BackoffStrategy

	// maxRetries is the maximum number of delivery attempts before giving up.
	// If 0 (default), messages are retried indefinitely.
	// After MaxRetries attempts, the message is removed from the scheduler
	// and the failure is logged.
	maxRetries int
}

// defaultOptions returns default scheduler options.
//
// Defaults:
//   - pollInterval: 100ms
//   - batchSize: 100
//   - keyPrefix: "scheduler:"
func defaultOptions() *options {
	return &options{
		pollInterval: 100 * time.Millisecond,
		batchSize:    100,
		keyPrefix:    "scheduler:",
	}
}

// Option is a function that modifies scheduler options.
type Option func(*options)

// WithPollInterval sets how often to check for due messages.
//
// Lower values reduce the latency between scheduled time and actual delivery,
// but increase load on the storage backend.
//
// Typical values: 50ms-1s
//
// Example:
//
//	// Check every 50ms for low-latency delivery
//	scheduler := NewRedisScheduler(client, transport, WithPollInterval(50*time.Millisecond))
func WithPollInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.pollInterval = d
		}
	}
}

// WithBatchSize sets the maximum number of messages to process per poll.
//
// Higher values improve throughput when many messages are due at once,
// but may increase memory usage.
//
// Example:
//
//	// Process up to 500 messages per poll
//	scheduler := NewRedisScheduler(client, transport, WithBatchSize(500))
func WithBatchSize(size int) Option {
	return func(o *options) {
		if size > 0 {
			o.batchSize = size
		}
	}
}

// WithKeyPrefix sets the prefix for storage keys.
//
// Use for multi-tenant deployments or to organize keys by application.
//
// Example:
//
//	scheduler := NewRedisScheduler(client, transport, WithKeyPrefix("myapp:scheduler:"))
func WithKeyPrefix(prefix string) Option {
	return func(o *options) {
		if prefix != "" {
			o.keyPrefix = prefix
		}
	}
}

// WithMetrics enables OpenTelemetry metrics for the scheduler.
//
// When metrics are enabled, the scheduler will record:
//   - scheduled_messages_total: Counter of total messages scheduled
//   - scheduled_messages_delivered_total: Counter of messages successfully delivered
//   - scheduled_messages_failed_total: Counter of messages that failed delivery
//   - scheduled_messages_pending: Gauge of current pending scheduled messages
//   - scheduled_messages_stuck: Gauge of current stuck messages being recovered
//   - schedule_delivery_delay_seconds: Histogram of delay between scheduled and actual delivery
//   - schedule_processing_duration_seconds: Histogram of time to process and deliver a message
//
// Example:
//
//	metrics, _ := scheduler.NewMetrics()
//	scheduler := scheduler.NewRedisScheduler(client, transport,
//	    scheduler.WithMetrics(metrics),
//	)
func WithMetrics(m *Metrics) Option {
	return func(o *options) {
		if m != nil {
			o.metrics = m
		}
	}
}

// WithBackoff sets a backoff strategy for retry delivery.
//
// When a message fails to deliver, this strategy determines how long to wait
// before the next retry attempt. The message's ScheduledAt is updated to
// now + backoff delay, so it will be picked up in a future poll.
//
// If not set, failed messages are immediately returned to the pending queue
// and may be retried on the next poll.
//
// Example:
//
//	// Using the event library's backoff package
//	import "github.com/rbaliyan/event/v3/backoff"
//
//	scheduler := NewRedisScheduler(client, transport,
//	    WithBackoff(&backoff.Exponential{
//	        Initial:    time.Second,
//	        Multiplier: 2.0,
//	        Max:        5 * time.Minute,
//	        Jitter:     0.1,
//	    }),
//	)
func WithBackoff(strategy BackoffStrategy) Option {
	return func(o *options) {
		o.backoff = strategy
	}
}

// WithMaxRetries sets the maximum number of delivery attempts.
//
// After this many failed attempts, the message is removed from the scheduler
// and the failure is logged. Use this to prevent messages from being retried
// indefinitely.
//
// If set to 0 (default), messages are retried indefinitely until successful.
//
// Example:
//
//	scheduler := NewRedisScheduler(client, transport,
//	    WithBackoff(backoffStrategy),
//	    WithMaxRetries(5),
//	)
func WithMaxRetries(max int) Option {
	return func(o *options) {
		if max >= 0 {
			o.maxRetries = max
		}
	}
}
