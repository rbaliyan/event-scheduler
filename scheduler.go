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
	"log/slog"
	"regexp"
	"time"

	"github.com/rbaliyan/event/v3/backoff"
	"github.com/rbaliyan/event/v3/health"
)

// validIdentifier matches safe SQL/collection identifiers (alphanumeric and underscores).
var validIdentifier = regexp.MustCompile(`^[a-zA-Z_][a-zA-Z0-9_]*$`)

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

// HealthChecker is an optional interface that schedulers can implement
// to provide health check capabilities for monitoring and readiness probes.
type HealthChecker = health.Checker

// NewHealthResult creates a health result with scheduler-specific details.
// This is a helper for creating health results with pending/stuck message counts.
func NewHealthResult(status health.Status, message string, latency time.Duration, pending, stuck int64) *health.Result {
	return &health.Result{
		Status:    status,
		Message:   message,
		Latency:   latency,
		CheckedAt: time.Now(),
		Details: map[string]any{
			"pending_messages": pending,
			"stuck_messages":   stuck,
		},
	}
}

// HealthPendingMessages extracts the pending message count from a health result.
func HealthPendingMessages(r *health.Result) int64 {
	if r == nil || r.Details == nil {
		return 0
	}
	if v, ok := r.Details["pending_messages"].(int64); ok {
		return v
	}
	return 0
}

// HealthStuckMessages extracts the stuck message count from a health result.
func HealthStuckMessages(r *health.Result) int64 {
	if r == nil || r.Details == nil {
		return 0
	}
	if v, ok := r.Details["stuck_messages"].(int64); ok {
		return v
	}
	return 0
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

// FilterBuilder provides a fluent API for constructing Filter queries.
//
// Example:
//
//	filter := scheduler.NewFilterBuilder().
//	    ForEvent("orders.reminder").
//	    Before(time.Now().Add(time.Hour)).
//	    After(time.Now()).
//	    WithLimit(100).
//	    Build()
type FilterBuilder struct {
	filter Filter
}

// NewFilterBuilder creates a new filter builder.
func NewFilterBuilder() *FilterBuilder {
	return &FilterBuilder{}
}

// ForEvent filters by event name.
func (b *FilterBuilder) ForEvent(name string) *FilterBuilder {
	b.filter.EventName = name
	return b
}

// Before filters messages scheduled before the given time.
func (b *FilterBuilder) Before(t time.Time) *FilterBuilder {
	b.filter.Before = t
	return b
}

// After filters messages scheduled after the given time.
func (b *FilterBuilder) After(t time.Time) *FilterBuilder {
	b.filter.After = t
	return b
}

// InTimeRange filters messages scheduled between start and end times.
func (b *FilterBuilder) InTimeRange(start, end time.Time) *FilterBuilder {
	b.filter.After = start
	b.filter.Before = end
	return b
}

// DueWithin filters messages scheduled within the given duration from now.
func (b *FilterBuilder) DueWithin(d time.Duration) *FilterBuilder {
	b.filter.After = time.Now()
	b.filter.Before = time.Now().Add(d)
	return b
}

// WithLimit sets the maximum number of results.
func (b *FilterBuilder) WithLimit(limit int) *FilterBuilder {
	b.filter.Limit = limit
	return b
}

// Build returns the constructed filter.
func (b *FilterBuilder) Build() Filter {
	return b.filter
}

// BackoffStrategy is an alias for backoff.Strategy from the main event library.
// All implementations from github.com/rbaliyan/event/v3/backoff can be used directly.
//
// Implementations must be safe for concurrent use.
type BackoffStrategy = backoff.Strategy

// Resetter is an optional interface for backoff strategies that maintain state.
// If a BackoffStrategy also implements Resetter, Reset() will be called at the
// start of each processing cycle to clear any accumulated state.
//
// Note: The standard backoff implementations (Constant, Linear, Exponential) are
// stateless and do not need to implement this interface.
type Resetter interface {
	// Reset resets the strategy state.
	Reset()
}

// DeadLetterQueue defines the interface for dead-letter queue storage.
// This provides loose coupling so the scheduler doesn't depend directly
// on the dlq package. The dlq.Manager from github.com/rbaliyan/event-dlq
// satisfies this interface.
//
// Implementations must be safe for concurrent use.
type DeadLetterQueue interface {
	// Store adds a failed message to the dead-letter queue.
	Store(ctx context.Context, eventName, originalID string, payload []byte, metadata map[string]string, err error, retryCount int, source string) error
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

	// minPollInterval is the minimum poll interval for adaptive polling.
	// When adaptive polling is enabled, the interval will not go below this.
	// Default: 10ms
	minPollInterval time.Duration

	// maxPollInterval is the maximum poll interval for adaptive polling.
	// When adaptive polling is enabled, the interval will not exceed this.
	// Default: 5s
	maxPollInterval time.Duration

	// adaptivePolling enables adaptive poll interval adjustment.
	// When enabled, the scheduler increases the poll interval when no
	// messages are found and decreases it when messages are processed.
	// Default: false
	adaptivePolling bool

	// batchSize is the maximum number of messages to process per poll.
	// Default: 100
	batchSize int

	// keyPrefix is the prefix for storage keys (Redis).
	// Default: "scheduler:"
	keyPrefix string

	// table is the table name for SQL backends (PostgreSQL).
	// Default: "scheduled_messages"
	table string

	// collection is the collection name for MongoDB.
	// Default: "scheduled_messages"
	collection string

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

	// dlq is the optional dead-letter queue for messages that exceed maxRetries.
	// When nil, messages that exceed maxRetries are simply discarded.
	// When set, messages are sent to the DLQ before being removed.
	dlq DeadLetterQueue

	// stuckDuration is how long a message can be in "processing" before recovery.
	// Messages stuck in processing longer than this duration are moved back to pending.
	// This handles scheduler crashes where messages were claimed but never published.
	// Default: 5 minutes
	stuckDuration time.Duration

	// logger is the logger for scheduler operations.
	// Default: slog.Default()
	logger *slog.Logger
}

// defaultOptions returns default scheduler options.
//
// Defaults:
//   - pollInterval: 100ms
//   - minPollInterval: 10ms
//   - maxPollInterval: 5s
//   - adaptivePolling: false
//   - batchSize: 100
//   - keyPrefix: "scheduler:"
//   - table: "scheduled_messages"
//   - collection: "scheduled_messages"
func defaultOptions() *options {
	return &options{
		pollInterval:    100 * time.Millisecond,
		minPollInterval: 10 * time.Millisecond,
		maxPollInterval: 5 * time.Second,
		adaptivePolling: false,
		batchSize:       100,
		keyPrefix:       "scheduler:",
		table:           "scheduled_messages",
		collection:      "scheduled_messages",
		stuckDuration:   5 * time.Minute,
		logger:          slog.Default(),
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

// WithAdaptivePolling enables adaptive poll interval adjustment.
//
// When enabled, the scheduler dynamically adjusts the poll interval:
//   - Decreases toward minPollInterval when messages are found (high activity)
//   - Increases toward maxPollInterval when no messages are found (low activity)
//
// This reduces database load during idle periods while maintaining low latency
// when messages are due. The initial poll interval is set to the base pollInterval.
//
// Example:
//
//	// Enable adaptive polling with custom min/max bounds
//	scheduler := NewRedisScheduler(client, transport,
//	    WithAdaptivePolling(true),
//	    WithMinPollInterval(10*time.Millisecond),
//	    WithMaxPollInterval(2*time.Second),
//	)
func WithAdaptivePolling(enabled bool) Option {
	return func(o *options) {
		o.adaptivePolling = enabled
	}
}

// WithMinPollInterval sets the minimum poll interval for adaptive polling.
//
// When adaptive polling is enabled and messages are being processed,
// the poll interval will decrease but not below this minimum.
//
// Default: 10ms
//
// Example:
//
//	scheduler := NewRedisScheduler(client, transport,
//	    WithAdaptivePolling(true),
//	    WithMinPollInterval(5*time.Millisecond),
//	)
func WithMinPollInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.minPollInterval = d
		}
	}
}

// WithMaxPollInterval sets the maximum poll interval for adaptive polling.
//
// When adaptive polling is enabled and no messages are found,
// the poll interval will increase but not exceed this maximum.
//
// Default: 5s
//
// Example:
//
//	scheduler := NewRedisScheduler(client, transport,
//	    WithAdaptivePolling(true),
//	    WithMaxPollInterval(10*time.Second),
//	)
func WithMaxPollInterval(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.maxPollInterval = d
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

// WithKeyPrefix sets the prefix for storage keys (Redis only).
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

// WithTable sets the table name for SQL backends (PostgreSQL).
//
// Use for multi-tenant deployments or custom table naming conventions.
//
// Example:
//
//	scheduler := NewPostgresScheduler(db, transport, WithTable("my_scheduled_jobs"))
func WithTable(table string) Option {
	return func(o *options) {
		if table != "" {
			o.table = table
		}
	}
}

// WithCollection sets the collection name for MongoDB.
//
// Use for multi-tenant deployments or custom collection naming conventions.
//
// Example:
//
//	scheduler := NewMongoScheduler(db, transport, WithCollection("my_scheduled_jobs"))
func WithCollection(collection string) Option {
	return func(o *options) {
		if collection != "" {
			o.collection = collection
		}
	}
}

// WithLogger sets a custom logger for the scheduler.
//
// The logger is used for info and error messages during scheduler operations.
// If not set, slog.Default() is used.
//
// Example:
//
//	scheduler := NewRedisScheduler(client, transport,
//	    WithLogger(slog.Default().With("service", "my-app")),
//	)
func WithLogger(l *slog.Logger) Option {
	return func(o *options) {
		if l != nil {
			o.logger = l
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

// WithDLQ sets the dead-letter queue for messages that exceed max retries.
//
// When a message fails delivery and has exceeded the configured MaxRetries,
// it will be sent to the DLQ before being removed from the scheduler.
// This prevents permanent message loss.
//
// The dlq.Manager from github.com/rbaliyan/event-dlq satisfies the
// DeadLetterQueue interface and can be used directly.
//
// Example:
//
//	dlqManager := dlq.NewManager(dlqStore, transport)
//	scheduler := NewRedisScheduler(client, transport,
//	    WithMaxRetries(5),
//	    WithDLQ(dlqManager),
//	)
func WithDLQ(d DeadLetterQueue) Option {
	return func(o *options) {
		o.dlq = d
	}
}

// WithStuckDuration sets how long a message can be in "processing" before recovery.
// Messages stuck in processing longer than this duration are moved back to pending.
// This handles scheduler crashes where messages were claimed but never published.
// Default: 5 minutes
//
// Example:
//
//	scheduler := NewRedisScheduler(client, transport,
//	    WithStuckDuration(10*time.Minute),
//	)
func WithStuckDuration(d time.Duration) Option {
	return func(o *options) {
		if d > 0 {
			o.stuckDuration = d
		}
	}
}

// adaptivePollState tracks state for adaptive polling.
type adaptivePollState struct {
	current time.Duration
	min     time.Duration
	max     time.Duration
}

// newAdaptivePollState creates a new adaptive poll state.
func newAdaptivePollState(initial, min, max time.Duration) *adaptivePollState {
	return &adaptivePollState{
		current: initial,
		min:     min,
		max:     max,
	}
}

// adjust adjusts the poll interval based on whether messages were processed.
// Returns the new poll interval.
func (s *adaptivePollState) adjust(messagesProcessed int) time.Duration {
	if messagesProcessed > 0 {
		// Messages found - decrease interval (more activity expected)
		s.current = s.current * 3 / 4 // Reduce by 25%
		if s.current < s.min {
			s.current = s.min
		}
	} else {
		// No messages - increase interval (less activity expected)
		s.current = s.current * 3 / 2 // Increase by 50%
		if s.current > s.max {
			s.current = s.max
		}
	}
	return s.current
}
