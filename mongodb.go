package scheduler

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	"github.com/rbaliyan/event/v3/transport"
	"go.mongodb.org/mongo-driver/v2/bson"
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
)

/*
MongoDB Schema:

Collection: scheduled_messages

Document structure:
{
    "_id": string (message ID),
    "event_name": string,
    "payload": Binary,
    "metadata": object,
    "scheduled_at": ISODate,
    "created_at": ISODate,
    "status": string (pending/processing),
    "claimed_at": ISODate (optional, set when processing),
    "retry_count": int (0 by default)
}

Indexes:
db.scheduled_messages.createIndex({ "scheduled_at": 1 })
db.scheduled_messages.createIndex({ "event_name": 1 })
db.scheduled_messages.createIndex({ "status": 1, "scheduled_at": 1 })
*/

// SchedulerStatus represents the state of a scheduled message
type SchedulerStatus string

const (
	// SchedulerStatusPending indicates the message is waiting to be delivered
	SchedulerStatusPending SchedulerStatus = "pending"

	// SchedulerStatusProcessing indicates the message is claimed by a scheduler
	SchedulerStatusProcessing SchedulerStatus = "processing"
)

// MongoMessage represents a scheduled message document in MongoDB
type MongoMessage struct {
	ID          string            `bson:"_id"`
	EventName   string            `bson:"event_name"`
	Payload     []byte            `bson:"payload"`
	Metadata    map[string]string `bson:"metadata,omitempty"`
	ScheduledAt time.Time         `bson:"scheduled_at"`
	CreatedAt   time.Time         `bson:"created_at"`
	Status      SchedulerStatus   `bson:"status,omitempty"`
	ClaimedAt   *time.Time        `bson:"claimed_at,omitempty"`
	RetryCount  int               `bson:"retry_count,omitempty"`
}

// ToMessage converts MongoMessage to Message
func (m *MongoMessage) ToMessage() *Message {
	return &Message{
		ID:          m.ID,
		EventName:   m.EventName,
		Payload:     m.Payload,
		Metadata:    m.Metadata,
		ScheduledAt: m.ScheduledAt,
		CreatedAt:   m.CreatedAt,
		RetryCount:  m.RetryCount,
	}
}

// FromMessage creates a MongoMessage from Message
func FromSchedulerMessage(m *Message) *MongoMessage {
	return &MongoMessage{
		ID:          m.ID,
		EventName:   m.EventName,
		Payload:     m.Payload,
		Metadata:    m.Metadata,
		ScheduledAt: m.ScheduledAt,
		CreatedAt:   m.CreatedAt,
		RetryCount:  m.RetryCount,
	}
}

// MongoScheduler uses MongoDB for scheduling
type MongoScheduler struct {
	collection    *mongo.Collection
	transport     transport.Transport
	opts          *options
	logger        *slog.Logger
	stopCh        chan struct{}
	stoppedCh     chan struct{}
	stopOnce      sync.Once
	stuckDuration time.Duration // How long before a processing message is considered stuck
}

// NewMongoScheduler creates a new MongoDB-based scheduler
func NewMongoScheduler(db *mongo.Database, t transport.Transport, opts ...Option) *MongoScheduler {
	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &MongoScheduler{
		collection:    db.Collection(o.collection),
		transport:     t,
		opts:          o,
		logger:        o.logger.With("component", "scheduler.mongodb"),
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
		stuckDuration: 5 * time.Minute, // Recover messages stuck in processing after 5 min
	}
}

// WithCollection sets a custom collection name
func (s *MongoScheduler) WithCollection(name string) *MongoScheduler {
	s.collection = s.collection.Database().Collection(name)
	return s
}

// WithLogger sets a custom logger
func (s *MongoScheduler) WithLogger(l *slog.Logger) *MongoScheduler {
	s.logger = l
	return s
}

// WithStuckDuration sets how long a message can be in "processing" before recovery.
// Messages stuck in processing longer than this duration are moved back to pending.
// This handles scheduler crashes where messages were claimed but never published.
// Default: 5 minutes
func (s *MongoScheduler) WithStuckDuration(d time.Duration) *MongoScheduler {
	s.stuckDuration = d
	return s
}

// Collection returns the underlying MongoDB collection
func (s *MongoScheduler) Collection() *mongo.Collection {
	return s.collection
}

// Indexes returns the required indexes for the scheduler collection.
// Users can use this to create indexes manually or merge with their own indexes.
//
// Example:
//
//	indexes := scheduler.Indexes()
//	_, err := collection.Indexes().CreateMany(ctx, indexes)
func (s *MongoScheduler) Indexes() []mongo.IndexModel {
	return []mongo.IndexModel{
		{
			Keys: bson.D{{Key: "scheduled_at", Value: 1}},
		},
		{
			Keys: bson.D{{Key: "event_name", Value: 1}},
		},
		{
			Keys: bson.D{
				{Key: "status", Value: 1},
				{Key: "scheduled_at", Value: 1},
			},
		},
	}
}

// EnsureIndexes creates the required indexes for the scheduler collection
func (s *MongoScheduler) EnsureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, s.Indexes())
	return err
}

// Schedule adds a message for future delivery
func (s *MongoScheduler) Schedule(ctx context.Context, msg Message) error {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}

	mongoMsg := FromSchedulerMessage(&msg)
	mongoMsg.Status = SchedulerStatusPending

	_, err := s.collection.InsertOne(ctx, mongoMsg)
	if err != nil {
		if mongo.IsDuplicateKeyError(err) {
			return fmt.Errorf("message already exists: %s", msg.ID)
		}
		return fmt.Errorf("insert: %w", err)
	}

	// Record metrics
	if s.opts.metrics != nil {
		s.opts.metrics.RecordScheduled(ctx, msg.EventName)
	}

	s.logger.Debug("scheduled message",
		"id", msg.ID,
		"event", msg.EventName,
		"scheduled_at", msg.ScheduledAt)

	return nil
}

// Cancel cancels a scheduled message
func (s *MongoScheduler) Cancel(ctx context.Context, id string) error {
	// First, get the message to record the event name for metrics
	var eventName string
	if s.opts.metrics != nil {
		var mongoMsg MongoMessage
		err := s.collection.FindOne(ctx, bson.M{"_id": id}).Decode(&mongoMsg)
		if err == nil {
			eventName = mongoMsg.EventName
		}
	}

	filter := bson.M{"_id": id}

	result, err := s.collection.DeleteOne(ctx, filter)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	if result.DeletedCount == 0 {
		return fmt.Errorf("%s: %w", id, ErrNotFound)
	}

	// Record metrics
	if s.opts.metrics != nil {
		s.opts.metrics.RecordCancelled(ctx, eventName)
	}

	s.logger.Debug("cancelled scheduled message", "id", id)
	return nil
}

// Get retrieves a scheduled message by ID
func (s *MongoScheduler) Get(ctx context.Context, id string) (*Message, error) {
	filter := bson.M{"_id": id}

	var mongoMsg MongoMessage
	err := s.collection.FindOne(ctx, filter).Decode(&mongoMsg)
	if err != nil {
		if err == mongo.ErrNoDocuments {
			return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("find: %w", err)
	}

	return mongoMsg.ToMessage(), nil
}

// List returns scheduled messages
func (s *MongoScheduler) List(ctx context.Context, filter Filter) ([]*Message, error) {
	mongoFilter := bson.M{}

	if filter.EventName != "" {
		mongoFilter["event_name"] = filter.EventName
	}

	if !filter.After.IsZero() {
		if mongoFilter["scheduled_at"] == nil {
			mongoFilter["scheduled_at"] = bson.M{}
		}
		mongoFilter["scheduled_at"].(bson.M)["$gte"] = filter.After
	}

	if !filter.Before.IsZero() {
		if mongoFilter["scheduled_at"] == nil {
			mongoFilter["scheduled_at"] = bson.M{}
		}
		mongoFilter["scheduled_at"].(bson.M)["$lte"] = filter.Before
	}

	opts := mongoopts.Find().SetSort(bson.D{{Key: "scheduled_at", Value: 1}})
	if filter.Limit > 0 {
		opts.SetLimit(int64(filter.Limit))
	}

	cursor, err := s.collection.Find(ctx, mongoFilter, opts)
	if err != nil {
		return nil, fmt.Errorf("find: %w", err)
	}
	defer cursor.Close(ctx)

	var messages []*Message
	for cursor.Next(ctx) {
		var mongoMsg MongoMessage
		if err := cursor.Decode(&mongoMsg); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		messages = append(messages, mongoMsg.ToMessage())
	}

	return messages, cursor.Err()
}

// Start begins the scheduler polling loop.
// Also periodically recovers messages stuck in "processing" state
// (from crashed scheduler instances).
func (s *MongoScheduler) Start(ctx context.Context) error {
	ticker := time.NewTicker(s.opts.pollInterval)
	defer ticker.Stop()

	// Recovery ticker for stuck messages (check every minute)
	recoveryTicker := time.NewTicker(time.Minute)
	defer recoveryTicker.Stop()

	s.logger.Info("scheduler started",
		"poll_interval", s.opts.pollInterval,
		"batch_size", s.opts.batchSize)

	// Recover any stuck messages at startup
	s.recoverStuck(ctx)

	for {
		select {
		case <-ctx.Done():
			close(s.stoppedCh)
			return ctx.Err()
		case <-s.stopCh:
			close(s.stoppedCh)
			return nil
		case <-ticker.C:
			s.processDue(ctx)
		case <-recoveryTicker.C:
			s.recoverStuck(ctx)
		}
	}
}

// Stop gracefully stops the scheduler
func (s *MongoScheduler) Stop(ctx context.Context) error {
	s.stopOnce.Do(func() {
		close(s.stopCh)
	})

	select {
	case <-s.stoppedCh:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// processDue processes messages that are due for delivery.
// Uses a 2-phase approach for crash safety:
// 1. Atomically claim message (status: pending -> processing)
// 2. Publish to transport
// 3. Delete the message
// If the scheduler crashes after claiming, recoverStuck will move it back to pending.
func (s *MongoScheduler) processDue(ctx context.Context) {
	now := time.Now()

	// Reset backoff state at the start of each processing cycle
	if s.opts.backoff != nil {
		if r, ok := s.opts.backoff.(Resetter); ok {
			r.Reset()
		}
	}

	for i := 0; i < s.opts.batchSize; i++ {
		processingStart := time.Now()

		msg, err := s.claimDue(ctx, now)
		if err != nil {
			if err == mongo.ErrNoDocuments {
				break // No more due messages
			}
			s.logger.Error("failed to claim due message", "error", err)
			break
		}

		// Publish to transport
		if publishErr := publishScheduledMessage(ctx, s.transport, msg); publishErr != nil {
			s.logger.Error("failed to publish scheduled message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", publishErr,
				"retry_count", msg.RetryCount)

			// Record failure metrics
			if s.opts.metrics != nil {
				s.opts.metrics.RecordFailed(ctx, msg.EventName, "publish_error")
			}

			// Check if max retries exceeded
			if s.opts.maxRetries > 0 && msg.RetryCount >= s.opts.maxRetries {
				s.logger.Error("scheduled message exceeded max retries, discarding",
					"id", msg.ID,
					"event", msg.EventName,
					"retry_count", msg.RetryCount,
					"max_retries", s.opts.maxRetries)

				// Send to DLQ if configured
				if s.opts.dlq != nil {
					if dlqErr := s.opts.dlq.Store(ctx, msg.EventName, msg.ID, msg.Payload, msg.Metadata, publishErr, msg.RetryCount, "scheduler"); dlqErr != nil {
						s.logger.Error("failed to send message to DLQ",
							"id", msg.ID,
							"error", dlqErr)
					} else if s.opts.metrics != nil {
						s.opts.metrics.RecordDLQSent(ctx, msg.EventName)
					}
				}

				// Delete the message
				s.deleteClaimed(ctx, msg.ID)
				continue
			}

			// Increment retry count and calculate next retry time
			newRetryCount := msg.RetryCount + 1
			nextRetryAt := time.Now()
			if s.opts.backoff != nil {
				backoffDelay := s.opts.backoff.NextDelay(newRetryCount - 1)
				nextRetryAt = nextRetryAt.Add(backoffDelay)
				s.logger.Debug("scheduling retry with backoff",
					"id", msg.ID,
					"retry_count", newRetryCount,
					"backoff_delay", backoffDelay,
					"next_retry_at", nextRetryAt)
			}

			// Update document: reset status to pending, increment retry_count, set new scheduled_at
			_, updateErr := s.collection.UpdateByID(ctx, msg.ID, bson.M{
				"$set": bson.M{
					"status":       SchedulerStatusPending,
					"retry_count":  newRetryCount,
					"scheduled_at": nextRetryAt,
				},
				"$unset": bson.M{
					"claimed_at": "",
				},
			})
			if updateErr != nil {
				s.logger.Error("failed to update message for retry",
					"id", msg.ID,
					"error", updateErr)
			}
			continue
		}

		// Delete the message after successful publish
		if err := s.deleteClaimed(ctx, msg.ID); err != nil {
			s.logger.Error("failed to delete published message",
				"id", msg.ID,
				"error", err)
		}

		// Record delivery metrics
		if s.opts.metrics != nil {
			s.opts.metrics.RecordDelivered(ctx, msg.EventName, msg.ScheduledAt, processingStart)
		}

		s.logger.Debug("delivered scheduled message",
			"id", msg.ID,
			"event", msg.EventName)
	}
}

// claimDue atomically claims a due message for processing.
// Uses FindOneAndUpdate to prevent race conditions in HA deployments.
func (s *MongoScheduler) claimDue(ctx context.Context, now time.Time) (*Message, error) {
	filter := bson.M{
		"scheduled_at": bson.M{"$lte": now},
		"$or": []bson.M{
			{"status": SchedulerStatusPending},
			{"status": bson.M{"$exists": false}}, // Backward compat: no status = pending
		},
	}
	claimedAt := time.Now()
	update := bson.M{
		"$set": bson.M{
			"status":     SchedulerStatusProcessing,
			"claimed_at": claimedAt,
		},
	}
	opts := mongoopts.FindOneAndUpdate().
		SetSort(bson.D{{Key: "scheduled_at", Value: 1}}).
		SetReturnDocument(mongoopts.After)

	var mongoMsg MongoMessage
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&mongoMsg)
	if err != nil {
		return nil, err
	}

	return mongoMsg.ToMessage(), nil
}

// deleteClaimed deletes a message that was successfully published
func (s *MongoScheduler) deleteClaimed(ctx context.Context, id string) error {
	_, err := s.collection.DeleteOne(ctx, bson.M{"_id": id})
	return err
}

// recoverStuck moves messages stuck in "processing" back to "pending".
// This handles scheduler crashes where messages were claimed but never published.
func (s *MongoScheduler) recoverStuck(ctx context.Context) {
	cutoff := time.Now().Add(-s.stuckDuration)
	filter := bson.M{
		"status":     SchedulerStatusProcessing,
		"claimed_at": bson.M{"$lt": cutoff},
	}
	update := bson.M{
		"$set": bson.M{
			"status": SchedulerStatusPending,
		},
		"$unset": bson.M{
			"claimed_at": "",
		},
	}

	result, err := s.collection.UpdateMany(ctx, filter, update)
	if err != nil {
		s.logger.Error("failed to recover stuck messages", "error", err)
		return
	}

	if result.ModifiedCount > 0 {
		// Record recovery metrics
		if s.opts.metrics != nil {
			s.opts.metrics.RecordRecovered(ctx, result.ModifiedCount)
		}

		s.logger.Warn("recovered stuck scheduled messages",
			"count", result.ModifiedCount,
			"stuck_duration", s.stuckDuration)
	}
}

// Count returns the number of scheduled messages
func (s *MongoScheduler) Count(ctx context.Context) (int64, error) {
	return s.collection.CountDocuments(ctx, bson.M{})
}

// CountByEvent returns the number of scheduled messages for a specific event
func (s *MongoScheduler) CountByEvent(ctx context.Context, eventName string) (int64, error) {
	filter := bson.M{"event_name": eventName}
	return s.collection.CountDocuments(ctx, filter)
}

// CountDue returns the number of messages due for delivery
func (s *MongoScheduler) CountDue(ctx context.Context) (int64, error) {
	filter := bson.M{
		"scheduled_at": bson.M{"$lte": time.Now()},
	}
	return s.collection.CountDocuments(ctx, filter)
}

// DeleteOlderThan removes messages older than the specified age (already delivered would be deleted)
func (s *MongoScheduler) DeleteOlderThan(ctx context.Context, age time.Duration) (int64, error) {
	cutoff := time.Now().Add(-age)
	filter := bson.M{
		"created_at": bson.M{"$lt": cutoff},
	}

	result, err := s.collection.DeleteMany(ctx, filter)
	if err != nil {
		return 0, fmt.Errorf("delete: %w", err)
	}

	return result.DeletedCount, nil
}

// CountPending returns the number of pending scheduled messages.
// This is useful for monitoring and metrics.
func (s *MongoScheduler) CountPending(ctx context.Context) (int64, error) {
	filter := bson.M{
		"$or": []bson.M{
			{"status": SchedulerStatusPending},
			{"status": bson.M{"$exists": false}}, // Backward compat: no status = pending
		},
	}
	return s.collection.CountDocuments(ctx, filter)
}

// CountProcessing returns the number of messages currently being processed.
// This is useful for monitoring and metrics.
func (s *MongoScheduler) CountProcessing(ctx context.Context) (int64, error) {
	filter := bson.M{"status": SchedulerStatusProcessing}
	return s.collection.CountDocuments(ctx, filter)
}

// CountStuck returns the number of messages stuck in processing (older than stuckDuration).
// This is useful for monitoring and metrics.
func (s *MongoScheduler) CountStuck(ctx context.Context) (int64, error) {
	cutoff := time.Now().Add(-s.stuckDuration)
	filter := bson.M{
		"status":     SchedulerStatusProcessing,
		"claimed_at": bson.M{"$lt": cutoff},
	}
	return s.collection.CountDocuments(ctx, filter)
}

// SetupMetricsCallbacks configures the metrics gauge callbacks for pending and stuck messages.
// This should be called after creating the scheduler if metrics are enabled.
//
// Example:
//
//	metrics, _ := scheduler.NewMetrics()
//	s := scheduler.NewMongoScheduler(db, transport, scheduler.WithMetrics(metrics))
//	s.SetupMetricsCallbacks(ctx)
func (s *MongoScheduler) SetupMetricsCallbacks(ctx context.Context) {
	if s.opts.metrics == nil {
		return
	}

	s.opts.metrics.SetPendingCallback(func() int64 {
		count, err := s.CountPending(ctx)
		if err != nil {
			s.logger.Error("failed to count pending messages for metrics", "error", err)
			return 0
		}
		return count
	})

	s.opts.metrics.SetStuckCallback(func() int64 {
		count, err := s.CountStuck(ctx)
		if err != nil {
			s.logger.Error("failed to count stuck messages for metrics", "error", err)
			return 0
		}
		return count
	})
}

// Health performs a health check on the MongoDB scheduler.
//
// The health check:
//   - Pings MongoDB to verify connectivity
//   - Counts pending messages
//   - Counts stuck messages (if any)
//
// Returns HealthStatusHealthy if MongoDB is responsive and no stuck messages.
// Returns HealthStatusDegraded if stuck messages exist.
// Returns HealthStatusUnhealthy if MongoDB is not responsive.
func (s *MongoScheduler) Health(ctx context.Context) *HealthCheckResult {
	start := time.Now()
	result := &HealthCheckResult{
		Status:    HealthStatusHealthy,
		CheckedAt: start,
		Details:   make(map[string]any),
	}

	// Ping MongoDB
	if err := s.collection.Database().Client().Ping(ctx, nil); err != nil {
		result.Status = HealthStatusUnhealthy
		result.Message = fmt.Sprintf("mongodb ping failed: %v", err)
		result.Latency = time.Since(start)
		return result
	}

	// Count pending messages
	pending, err := s.CountPending(ctx)
	if err != nil {
		result.Status = HealthStatusDegraded
		result.Message = fmt.Sprintf("failed to count pending: %v", err)
	}
	result.PendingMessages = pending

	// Count stuck messages
	stuck, err := s.CountStuck(ctx)
	if err != nil {
		result.Status = HealthStatusDegraded
		result.Message = fmt.Sprintf("failed to count stuck: %v", err)
	}
	result.StuckMessages = stuck

	// Degraded if stuck messages exist
	if stuck > 0 && result.Status == HealthStatusHealthy {
		result.Status = HealthStatusDegraded
		result.Message = fmt.Sprintf("%d messages stuck in processing", stuck)
	}

	result.Latency = time.Since(start)
	result.Details["collection"] = s.collection.Name()
	result.Details["database"] = s.collection.Database().Name()

	return result
}

// Compile-time checks
var (
	_ Scheduler     = (*MongoScheduler)(nil)
	_ HealthChecker = (*MongoScheduler)(nil)
)
