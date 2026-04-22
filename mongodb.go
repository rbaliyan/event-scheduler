package scheduler

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"strings"
	"sync"
	"time"

	"github.com/google/uuid"
	eventerrors "github.com/rbaliyan/event/v3/errors"
	"github.com/rbaliyan/event/v3/health"
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

// schedulerStatus represents the state of a scheduled message
type schedulerStatus string

const (
	// statusPending indicates the message is waiting to be delivered
	statusPending schedulerStatus = "pending"

	// statusProcessing indicates the message is claimed by a scheduler
	statusProcessing schedulerStatus = "processing"
)

// mongoMessage represents a scheduled message document in MongoDB
type mongoMessage struct {
	ID          string            `bson:"_id"`
	EventName   string            `bson:"event_name"`
	Payload     []byte            `bson:"payload"`
	Metadata    map[string]string `bson:"metadata,omitempty"`
	ScheduledAt time.Time         `bson:"scheduled_at"`
	CreatedAt   time.Time         `bson:"created_at"`
	Status      schedulerStatus   `bson:"status,omitempty"`
	ClaimedAt   *time.Time        `bson:"claimed_at,omitempty"`
	RetryCount  int               `bson:"retry_count,omitempty"`
}

// toMessage converts mongoMessage to Message
func (m *mongoMessage) toMessage() *Message {
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

// fromMessage creates a mongoMessage from Message
func fromMessage(m *Message) *mongoMessage {
	return &mongoMessage{
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
	collection *mongo.Collection
	transport  transport.Transport
	opts       *options
	logger     *slog.Logger
	stopCh     chan struct{}
	stoppedCh  chan struct{}
	stopOnce   sync.Once
}

// NewMongoScheduler creates a new MongoDB-based scheduler.
//
// Parameters:
//   - db: required - MongoDB database connection (must not be nil)
//   - t: required - transport for publishing messages (must not be nil)
//   - opts: optional configuration options
//
// Returns an error if db or t is nil.
func NewMongoScheduler(db *mongo.Database, t transport.Transport, opts ...Option) (*MongoScheduler, error) {
	if err := eventerrors.RequireNotNil(db, "db"); err != nil {
		return nil, err
	}
	if err := eventerrors.RequireNotNil(t, "transport"); err != nil {
		return nil, err
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	if !validIdentifier.MatchString(o.collection) {
		return nil, fmt.Errorf("scheduler: invalid collection name %q", o.collection)
	}

	s := &MongoScheduler{
		collection: db.Collection(o.collection),
		transport:  t,
		opts:       o,
		logger:     o.logger.With("component", "scheduler.mongodb"),
		stopCh:     make(chan struct{}),
		stoppedCh:  make(chan struct{}),
	}
	go func() { // #nosec G118 — background goroutine intentionally outlives constructor context
		if err := s.EnsureIndexes(context.Background()); err != nil {
			s.logger.Error("failed to ensure scheduler indexes", "error", err)
		}
	}()
	return s, nil
}

// Collection returns the underlying MongoDB collection.
// Use this if you need direct access for operational tasks (e.g., dropping in tests).
func (s *MongoScheduler) Collection() *mongo.Collection {
	return s.collection
}

// Indexes returns the required index models for the scheduler collection.
// Use this if you prefer to manage indexes separately (e.g., via migrations).
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

// EnsureIndexes creates the required indexes for the scheduler collection.
// Call this once during application startup.
func (s *MongoScheduler) EnsureIndexes(ctx context.Context) error {
	_, err := s.collection.Indexes().CreateMany(ctx, s.Indexes())
	return err
}

// Schedule adds a message for future delivery
func (s *MongoScheduler) Schedule(ctx context.Context, msg Message) error {
	if msg.EventName == "" {
		return fmt.Errorf("schedule: event name must not be empty")
	}
	if msg.ScheduledAt.IsZero() {
		return fmt.Errorf("schedule: scheduled_at must not be zero")
	}
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}

	mongoMsg := fromMessage(&msg)
	mongoMsg.Status = statusPending

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
	if id == "" {
		return fmt.Errorf("cancel: id must not be empty")
	}

	// First, get the message to record the event name for metrics
	var eventName string
	if s.opts.metrics != nil {
		var mongoMsg mongoMessage
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
	if id == "" {
		return nil, fmt.Errorf("get: id must not be empty")
	}

	filter := bson.M{"_id": id}

	var mongoMsg mongoMessage
	err := s.collection.FindOne(ctx, filter).Decode(&mongoMsg)
	if err != nil {
		if errors.Is(err, mongo.ErrNoDocuments) {
			return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("find: %w", err)
	}

	return mongoMsg.toMessage(), nil
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
	if filter.Offset > 0 {
		opts.SetSkip(int64(filter.Offset))
	}

	cursor, err := s.collection.Find(ctx, mongoFilter, opts)
	if err != nil {
		return nil, fmt.Errorf("find: %w", err)
	}
	defer func() { _ = cursor.Close(ctx) }()

	var messages []*Message
	for cursor.Next(ctx) {
		var mongoMsg mongoMessage
		if err := cursor.Decode(&mongoMsg); err != nil {
			return nil, fmt.Errorf("decode: %w", err)
		}
		messages = append(messages, mongoMsg.toMessage())
	}

	return messages, cursor.Err()
}

// Start begins the scheduler polling loop.
// Also periodically recovers messages stuck in "processing" state
// (from crashed scheduler instances).
//
// When adaptive polling is enabled, the poll interval adjusts dynamically:
// - Decreases when messages are found (more activity expected)
// - Increases when no messages are found (less activity expected)
func (s *MongoScheduler) Start(ctx context.Context) error {
	s.setupMetricsCallbacks(ctx)

	runSchedulerLoop(
		s.logger,
		s.opts,
		ctx.Done(),
		s.stopCh,
		func() int { return s.processDue(ctx) },
		func() { s.recoverStuck(ctx) },
		func() { close(s.stoppedCh) },
	)

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
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
// Returns the number of messages processed (for adaptive polling).
func (s *MongoScheduler) processDue(ctx context.Context) int {
	processed := 0
	now := time.Now()
	resetBackoff(s.opts)

	for i := 0; i < s.opts.batchSize; i++ {
		processingStart := time.Now()

		msg, err := s.claimDue(ctx, now)
		if err != nil {
			if errors.Is(err, mongo.ErrNoDocuments) {
				break
			}
			s.logger.Error("failed to claim due message", "error", err)
			break
		}

		if publishErr := publishScheduledMessage(ctx, s.transport, msg); publishErr != nil {
			s.logger.Error("failed to publish scheduled message",
				"id", msg.ID, "event", msg.EventName,
				"error", publishErr, "retry_count", msg.RetryCount)
			s.handlePublishError(ctx, msg, publishErr)
			continue
		}

		if err := s.deleteClaimed(ctx, msg.ID); err != nil {
			s.logger.Error("failed to delete published message", "id", msg.ID, "error", err)
		}
		if s.opts.metrics != nil {
			s.opts.metrics.RecordDelivered(ctx, msg.EventName, msg.ScheduledAt, processingStart)
		}
		s.logger.Debug("delivered scheduled message", "id", msg.ID, "event", msg.EventName)
		processed++
	}
	return processed
}

// handlePublishError applies the DLQ-or-retry decision for a failed publish.
func (s *MongoScheduler) handlePublishError(ctx context.Context, msg *Message, publishErr error) {
	decision := handleDeliveryFailure(ctx, s.opts, msg, publishErr, s.logger)
	if decision.sendToDLQ {
		if err := s.deleteClaimed(ctx, msg.ID); err != nil {
			s.logger.Error("failed to delete claimed message", "id", msg.ID, "error", err)
		}
		return
	}
	_, updateErr := s.collection.UpdateByID(ctx, msg.ID, bson.M{
		"$set": bson.M{
			"status":       statusPending,
			"retry_count":  decision.retryCount,
			"scheduled_at": decision.nextRetryAt,
		},
		"$unset": bson.M{"claimed_at": ""},
	})
	if updateErr != nil {
		s.logger.Error("failed to update message for retry", "id", msg.ID, "error", updateErr)
	}
}

// claimDue atomically claims a due message for processing.
// Uses FindOneAndUpdate to prevent race conditions in HA deployments.
func (s *MongoScheduler) claimDue(ctx context.Context, now time.Time) (*Message, error) {
	filter := bson.M{
		"scheduled_at": bson.M{"$lte": now},
		"$or": []bson.M{
			{"status": statusPending},
			{"status": bson.M{"$exists": false}}, // Backward compat: no status = pending
		},
	}
	claimedAt := time.Now()
	update := bson.M{
		"$set": bson.M{
			"status":     statusProcessing,
			"claimed_at": claimedAt,
		},
	}
	opts := mongoopts.FindOneAndUpdate().
		SetSort(bson.D{{Key: "scheduled_at", Value: 1}}).
		SetReturnDocument(mongoopts.After)

	var mongoMsg mongoMessage
	err := s.collection.FindOneAndUpdate(ctx, filter, update, opts).Decode(&mongoMsg)
	if err != nil {
		return nil, err
	}

	return mongoMsg.toMessage(), nil
}

// deleteClaimed deletes a message that was successfully published
func (s *MongoScheduler) deleteClaimed(ctx context.Context, id string) error {
	_, err := s.collection.DeleteOne(ctx, bson.M{"_id": id})
	return err
}

// recoverStuck moves messages stuck in "processing" back to "pending".
// This handles scheduler crashes where messages were claimed but never published.
func (s *MongoScheduler) recoverStuck(ctx context.Context) {
	cutoff := time.Now().Add(-s.opts.stuckDuration)
	filter := bson.M{
		"status":     statusProcessing,
		"claimed_at": bson.M{"$lt": cutoff},
	}
	update := bson.M{
		"$set": bson.M{
			"status": statusPending,
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
			"stuck_duration", s.opts.stuckDuration)
	}
}

// countPending returns the number of pending scheduled messages.
func (s *MongoScheduler) countPending(ctx context.Context) (int64, error) {
	filter := bson.M{
		"$or": []bson.M{
			{"status": statusPending},
			{"status": bson.M{"$exists": false}}, // Backward compat: no status = pending
		},
	}
	return s.collection.CountDocuments(ctx, filter)
}

// countStuck returns the number of messages stuck in processing (older than stuckDuration).
func (s *MongoScheduler) countStuck(ctx context.Context) (int64, error) {
	cutoff := time.Now().Add(-s.opts.stuckDuration)
	filter := bson.M{
		"status":     statusProcessing,
		"claimed_at": bson.M{"$lt": cutoff},
	}
	return s.collection.CountDocuments(ctx, filter)
}

// setupMetricsCallbacks configures the metrics gauge callbacks for pending and stuck messages.
// This should be called after creating the scheduler if metrics are enabled.
func (s *MongoScheduler) setupMetricsCallbacks(ctx context.Context) {
	if s.opts.metrics == nil {
		return
	}

	s.opts.metrics.SetPendingCallback(func() int64 {
		count, err := s.countPending(ctx)
		if err != nil {
			s.logger.Error("failed to count pending messages for metrics", "error", err)
			return 0
		}
		return count
	})

	s.opts.metrics.SetStuckCallback(func() int64 {
		count, err := s.countStuck(ctx)
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
// Returns health.StatusHealthy if MongoDB is responsive and no stuck messages.
// Returns health.StatusDegraded if stuck messages exist.
// Returns health.StatusUnhealthy if MongoDB is not responsive.
func (s *MongoScheduler) Health(ctx context.Context) *health.Result {
	start := time.Now()

	// Ping MongoDB
	if err := s.collection.Database().Client().Ping(ctx, nil); err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("mongodb ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}

	status := health.StatusHealthy
	var messages []string

	// Count pending messages
	pending, err := s.countPending(ctx)
	if err != nil {
		status = health.StatusDegraded
		messages = append(messages, fmt.Sprintf("failed to count pending: %v", err))
	}

	// Count stuck messages
	stuck, err := s.countStuck(ctx)
	if err != nil {
		status = health.StatusDegraded
		messages = append(messages, fmt.Sprintf("failed to count stuck: %v", err))
	}

	// Degraded if stuck messages exist
	if stuck > 0 && status == health.StatusHealthy {
		status = health.StatusDegraded
		messages = append(messages, fmt.Sprintf("%d messages stuck in processing", stuck))
	}

	return &health.Result{
		Status:    status,
		Message:   strings.Join(messages, "; "),
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"pending_messages": pending,
			"stuck_messages":   stuck,
			"collection":       s.collection.Name(),
			"database":         s.collection.Database().Name(),
		},
	}
}

// Compile-time checks
var (
	_ Scheduler     = (*MongoScheduler)(nil)
	_ HealthChecker = (*MongoScheduler)(nil)
)
