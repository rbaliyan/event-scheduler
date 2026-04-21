package scheduler

import (
	"context"
	"encoding/json"
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
	"github.com/redis/go-redis/v9"
)

// Redis key layout:
//   {prefix}messages          sorted set  score=scheduled_unix  member=msgID
//   {prefix}processing        sorted set  score=claimed_unix    member=msgID
//   {prefix}events:{name}     sorted set  score=scheduled_unix  member=msgID
//   {prefix}index             hash        field=msgID           value=JSON
//
// All sorted sets hold only message IDs. Full message data lives exclusively
// in the index hash, so every set operation is O(log N) with no JSON scanning.

// scheduleScript atomically writes all three sorted-set entries and the index hash.
// KEYS[1]=messages  KEYS[2]=index  KEYS[3]=events:{event_name}
// ARGV[1]=score(unix)  ARGV[2]=json  ARGV[3]=msgID
var scheduleScript = redis.NewScript(`
local score    = tonumber(ARGV[1])
local data     = ARGV[2]
local msgID    = ARGV[3]
redis.call('ZADD', KEYS[1], score, msgID)
redis.call('HSET', KEYS[2], msgID, data)
redis.call('ZADD', KEYS[3], score, msgID)
return 1
`)

// claimScript atomically moves one due message from pending to processing and
// returns {msgID, json} so the caller never needs a second round-trip.
// KEYS[1]=messages  KEYS[2]=processing  KEYS[3]=index
// ARGV[1]=max_score(now)  ARGV[2]=claimed_at
var claimScript = redis.NewScript(`
local ids = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
if #ids == 0 then
    return nil
end
local msgID = ids[1]
redis.call('ZREM',  KEYS[1], msgID)
redis.call('ZADD',  KEYS[2], ARGV[2], msgID)
local data = redis.call('HGET', KEYS[3], msgID)
return {msgID, data}
`)

// RedisScheduler uses Redis sorted sets for scheduling.
//
// All sorted sets store message IDs (not full JSON) so every set operation
// is O(log N). Full message data lives in a single index hash, fetched with
// O(1) HGET or O(M) HMGET for bulk operations.
//
// The scheduler uses a 2-phase approach for HA safety:
//  1. Atomically move message ID from "messages" to "processing" set
//  2. Publish to transport
//  3. Remove from "processing" set
//
// If a scheduler crashes after step 1, recoverStuck() will move messages
// back to the main set after stuckDuration.
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	scheduler := scheduler.NewRedisScheduler(rdb, transport,
//	    scheduler.WithPollInterval(100*time.Millisecond),
//	    scheduler.WithBatchSize(100),
//	)
//
//	// Start the scheduler (blocks until stopped)
//	go scheduler.Start(ctx)
//
//	// Schedule messages
//	scheduler.Schedule(ctx, scheduler.Message{
//	    EventName:   "orders.reminder",
//	    Payload:     payload,
//	    ScheduledAt: time.Now().Add(time.Hour),
//	})
//
//	// Stop gracefully
//	scheduler.Stop(ctx)
type RedisScheduler struct {
	client    redis.Cmdable
	transport transport.Transport
	opts      *options
	logger    *slog.Logger
	stopCh    chan struct{}
	stoppedCh chan struct{}
	stopOnce  sync.Once
}

// NewRedisScheduler creates a new Redis-based scheduler.
//
// Parameters:
//   - client: A connected Redis client (supports single node, Sentinel, Cluster)
//   - t: Transport for publishing due messages
//   - opts: Optional configuration options
//
// Returns an error if client or t is nil.
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	scheduler, err := scheduler.NewRedisScheduler(rdb, transport)
func NewRedisScheduler(client redis.Cmdable, t transport.Transport, opts ...Option) (*RedisScheduler, error) {
	if err := eventerrors.RequireNotNil(client, "client"); err != nil {
		return nil, err
	}
	if err := eventerrors.RequireNotNil(t, "transport"); err != nil {
		return nil, err
	}

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &RedisScheduler{
		client:    client,
		transport: t,
		opts:      o,
		logger:    o.logger.With("component", "scheduler.redis"),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}, nil
}

// Schedule adds a message for future delivery.
//
// The message is stored in a Redis sorted set with ScheduledAt as the score.
// If ID is empty, a UUID is generated. If CreatedAt is zero, it's set to now.
func (s *RedisScheduler) Schedule(ctx context.Context, msg Message) error {
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

	data, err := json.Marshal(msg)
	if err != nil {
		return fmt.Errorf("marshal message: %w", err)
	}

	// Atomically store in sorted set, index, and per-event sorted set using Lua script
	if err := scheduleScript.Run(ctx, s.client,
		[]string{s.key(), s.indexKey(), s.eventKey(msg.EventName)},
		msg.ScheduledAt.Unix(), string(data), msg.ID,
	).Err(); err != nil {
		return fmt.Errorf("schedule: %w", err)
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

// Cancel cancels a scheduled message before delivery.
//
// Reads event name from the index (needed for event set cleanup), then removes
// the ID from all sorted sets and the index in a single pipeline.
func (s *RedisScheduler) Cancel(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("cancel: id must not be empty")
	}

	data, err := s.client.HGet(ctx, s.indexKey(), id).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return fmt.Errorf("%s: %w", id, ErrNotFound)
		}
		return fmt.Errorf("hget index: %w", err)
	}

	var msg Message
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	// All sorted sets hold the message ID directly — ZRem by id, no JSON needed.
	pipe := s.client.Pipeline()
	pipe.ZRem(ctx, s.key(), id)
	pipe.ZRem(ctx, s.processingKey(), id)
	pipe.ZRem(ctx, s.eventKey(msg.EventName), id)
	pipe.HDel(ctx, s.indexKey(), id)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("cancel pipeline: %w", err)
	}

	if s.opts.metrics != nil {
		s.opts.metrics.RecordCancelled(ctx, msg.EventName)
	}

	s.logger.Debug("cancelled scheduled message", "id", id)
	return nil
}

// Get retrieves a scheduled message by ID.
//
// Looks up the message in the index for O(1) retrieval.
// Returns error if the message is not found.
func (s *RedisScheduler) Get(ctx context.Context, id string) (*Message, error) {
	if id == "" {
		return nil, fmt.Errorf("get: id must not be empty")
	}

	member, err := s.client.HGet(ctx, s.indexKey(), id).Result()
	if err != nil {
		if errors.Is(err, redis.Nil) {
			return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
		}
		return nil, fmt.Errorf("hget index: %w", err)
	}

	var msg Message
	if err := json.Unmarshal([]byte(member), &msg); err != nil {
		return nil, fmt.Errorf("unmarshal: %w", err)
	}

	return &msg, nil
}

// List returns scheduled messages matching the filter.
//
// When EventName is set, queries the per-event sorted set for O(log N + M) lookup,
// then bulk-fetches full messages from the index hash. When EventName is empty,
// queries the main sorted set directly. Offset and Limit are pushed down to Redis
// in both paths.
func (s *RedisScheduler) List(ctx context.Context, filter Filter) ([]*Message, error) {
	min := "-inf"
	max := "+inf"

	if !filter.After.IsZero() {
		min = fmt.Sprintf("%d", filter.After.Unix())
	}
	if !filter.Before.IsZero() {
		max = fmt.Sprintf("%d", filter.Before.Unix())
	}

	rangeBy := &redis.ZRangeBy{Min: min, Max: max}
	if filter.Offset > 0 {
		rangeBy.Offset = int64(filter.Offset)
	}
	if filter.Limit > 0 {
		rangeBy.Count = int64(filter.Limit)
	}

	if filter.EventName != "" {
		ids, err := s.client.ZRangeByScore(ctx, s.eventKey(filter.EventName), rangeBy).Result()
		if err != nil {
			return nil, fmt.Errorf("zrangebyscore events: %w", err)
		}
		if len(ids) == 0 {
			return nil, nil
		}
		return s.fetchMessages(ctx, ids)
	}

	// No EventName filter: query main sorted set (IDs) then bulk-fetch from index.
	ids, err := s.client.ZRangeByScore(ctx, s.key(), rangeBy).Result()
	if err != nil {
		return nil, fmt.Errorf("zrangebyscore: %w", err)
	}
	if len(ids) == 0 {
		return nil, nil
	}
	return s.fetchMessages(ctx, ids)
}

// fetchMessages bulk-fetches full message data from the index hash for the given IDs.
func (s *RedisScheduler) fetchMessages(ctx context.Context, ids []string) ([]*Message, error) {
	members, err := s.client.HMGet(ctx, s.indexKey(), ids...).Result()
	if err != nil {
		return nil, fmt.Errorf("hmget: %w", err)
	}
	var messages []*Message
	for i, m := range members {
		if m == nil {
			continue
		}
		jsonStr, ok := m.(string)
		if !ok {
			continue
		}
		var msg Message
		if err := json.Unmarshal([]byte(jsonStr), &msg); err != nil {
			s.logger.Warn("skipping corrupt message in list", "id", ids[i], "error", err)
			continue
		}
		messages = append(messages, &msg)
	}
	return messages, nil
}

// Start begins the scheduler polling loop.
//
// This method blocks until the context is cancelled or Stop is called.
// It polls Redis at PollInterval for due messages and publishes them.
// Also periodically recovers messages stuck in "processing" state
// (from crashed scheduler instances).
//
// When adaptive polling is enabled, the poll interval adjusts dynamically:
// - Decreases when messages are found (more activity expected)
// - Increases when no messages are found (less activity expected)
//
// Example:
//
//	go scheduler.Start(ctx)
func (s *RedisScheduler) Start(ctx context.Context) error {
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

// Stop gracefully stops the scheduler.
//
// Signals the polling loop to stop and waits for it to finish.
// Returns context error if the context expires before stopping.
func (s *RedisScheduler) Stop(ctx context.Context) error {
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
// 1. Atomically move message from "messages" to "processing" set
// 2. Publish to transport
// 3. Remove from "processing" set
// If the scheduler crashes after step 1, recoverStuck will move it back.
// Returns the number of messages processed (for adaptive polling).
func (s *RedisScheduler) processDue(ctx context.Context) int {
	processed := 0
	now := time.Now().Unix()

	// Reset backoff state at the start of each processing cycle to avoid
	// shared mutable state accumulating across cycles. Each cycle computes
	// delays independently using the per-message retry count.
	if s.opts.backoff != nil {
		if r, ok := s.opts.backoff.(Resetter); ok {
			r.Reset()
		}
	}

	for i := 0; i < s.opts.batchSize; i++ {
		processingStart := time.Now()

		// Atomically move message from pending to processing
		msg, member, err := s.claimDueMessage(ctx, now)
		if err != nil {
			if errors.Is(err, redis.Nil) {
				break // No more due messages
			}
			s.logger.Error("failed to claim due message", "error", err)
			break
		}

		// Publish to transport
		if err := publishScheduledMessage(ctx, s.transport, msg); err != nil {
			s.logger.Error("failed to publish scheduled message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", err,
				"retry_count", msg.RetryCount)

			// Remove from processing set
			if err := s.client.ZRem(ctx, s.processingKey(), member).Err(); err != nil {
				s.logger.Warn("failed to remove from processing set", "id", msg.ID, "error", err)
			}

			decision := handleDeliveryFailure(ctx, s.opts, msg, err, s.logger)
			if decision.sendToDLQ {
				cleanPipe := s.client.Pipeline()
				cleanPipe.HDel(ctx, s.indexKey(), msg.ID)
				cleanPipe.ZRem(ctx, s.eventKey(msg.EventName), msg.ID)
				if _, pipeErr := cleanPipe.Exec(ctx); pipeErr != nil {
					s.logger.Warn("failed to clean up DLQ message", "id", msg.ID, "error", pipeErr)
				}
				continue
			}

			// Update message for retry
			msg.RetryCount = decision.retryCount
			msg.ScheduledAt = decision.nextRetryAt

			updatedData, marshalErr := json.Marshal(msg)
			if marshalErr != nil {
				s.logger.Error("failed to marshal message for retry", "error", marshalErr)
				continue
			}

			// Reuse scheduleScript for atomicity: ZADD messages + HSET index + ZADD events.
			if err := scheduleScript.Run(ctx, s.client,
				[]string{s.key(), s.indexKey(), s.eventKey(msg.EventName)},
				msg.ScheduledAt.Unix(), string(updatedData), msg.ID,
			).Err(); err != nil {
				s.logger.Error("failed to reschedule for retry", "id", msg.ID, "error", err)
			}
			continue
		}

		// Remove from processing set, index, and per-event set after successful publish
		pipe := s.client.Pipeline()
		pipe.ZRem(ctx, s.processingKey(), member)
		pipe.HDel(ctx, s.indexKey(), msg.ID)
		pipe.ZRem(ctx, s.eventKey(msg.EventName), msg.ID)
		if _, err := pipe.Exec(ctx); err != nil {
			s.logger.Warn("failed to clean up processed message", "id", msg.ID, "error", err)
		}

		// Record delivery metrics
		if s.opts.metrics != nil {
			s.opts.metrics.RecordDelivered(ctx, msg.EventName, msg.ScheduledAt, processingStart)
		}

		s.logger.Debug("delivered scheduled message",
			"id", msg.ID,
			"event", msg.EventName)
		processed++
	}
	return processed
}

// claimDueMessage atomically moves one due message from pending to processing.
// The Lua script returns {msgID, json}; the second return value is the msgID,
// used by the caller to reference the message in the processing set.
func (s *RedisScheduler) claimDueMessage(ctx context.Context, now int64) (*Message, string, error) {
	claimedAt := time.Now().Unix()
	result, err := claimScript.Run(ctx, s.client,
		[]string{s.key(), s.processingKey(), s.indexKey()},
		now, claimedAt,
	).Result()
	if err != nil {
		return nil, "", err
	}
	if result == nil {
		return nil, "", redis.Nil
	}

	arr, ok := result.([]interface{})
	if !ok || len(arr) < 2 {
		return nil, "", fmt.Errorf("unexpected claim result type: %T", result)
	}
	msgID, ok := arr[0].(string)
	if !ok {
		return nil, "", fmt.Errorf("unexpected msgID type: %T", arr[0])
	}
	if arr[1] == nil {
		// Message was removed from index before we claimed it — clean up orphan.
		s.client.ZRem(ctx, s.processingKey(), msgID)
		return nil, "", redis.Nil
	}
	data, ok := arr[1].(string)
	if !ok {
		return nil, "", fmt.Errorf("unexpected data type: %T", arr[1])
	}

	var msg Message
	if err := json.Unmarshal([]byte(data), &msg); err != nil {
		// Corrupt index entry — remove everywhere.
		pipe := s.client.Pipeline()
		pipe.ZRem(ctx, s.processingKey(), msgID)
		pipe.HDel(ctx, s.indexKey(), msgID)
		pipe.Exec(ctx)
		s.logger.Error("corrupt message in index, discarding", "id", msgID, "error", err)
		return nil, "", redis.Nil
	}

	return &msg, msgID, nil
}

// recoverStuck moves messages stuck in "processing" back to "pending".
// Processing set holds IDs; we fetch scheduled_at from the index to restore
// the correct score in the messages set.
func (s *RedisScheduler) recoverStuck(ctx context.Context) {
	cutoff := time.Now().Add(-s.opts.stuckDuration).Unix()

	ids, err := s.client.ZRangeByScore(ctx, s.processingKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", cutoff),
		Count: 100,
	}).Result()
	if err != nil {
		s.logger.Error("failed to get stuck messages", "error", err)
		return
	}
	if len(ids) == 0 {
		return
	}

	members, err := s.client.HMGet(ctx, s.indexKey(), ids...).Result()
	if err != nil {
		s.logger.Error("failed to fetch stuck messages from index", "error", err)
		return
	}

	var recovered int64
	for i, m := range members {
		msgID := ids[i]
		if m == nil {
			// Index already cleaned up — remove the orphan from processing.
			s.client.ZRem(ctx, s.processingKey(), msgID)
			continue
		}
		jsonStr, ok := m.(string)
		if !ok {
			continue
		}
		var msg Message
		if err := json.Unmarshal([]byte(jsonStr), &msg); err != nil {
			// Corrupt entry — purge from all sets and index.
			pipe := s.client.Pipeline()
			pipe.ZRem(ctx, s.processingKey(), msgID)
			pipe.HDel(ctx, s.indexKey(), msgID)
			pipe.Exec(ctx)
			continue
		}

		pipe := s.client.Pipeline()
		pipe.ZRem(ctx, s.processingKey(), msgID)
		pipe.ZAdd(ctx, s.key(), redis.Z{
			Score:  float64(msg.ScheduledAt.Unix()),
			Member: msgID,
		})
		if _, err := pipe.Exec(ctx); err == nil {
			recovered++
		}
	}

	if recovered > 0 {
		if s.opts.metrics != nil {
			s.opts.metrics.RecordRecovered(ctx, recovered)
		}
		s.logger.Warn("recovered stuck scheduled messages",
			"count", recovered,
			"stuck_duration", s.opts.stuckDuration)
	}
}

// key returns the Redis key for the scheduled messages sorted set.
func (s *RedisScheduler) key() string {
	return s.opts.keyPrefix + "messages"
}

// processingKey returns the Redis key for messages currently being processed.
func (s *RedisScheduler) processingKey() string {
	return s.opts.keyPrefix + "processing"
}

// indexKey returns the Redis key for the message ID to member index hash.
func (s *RedisScheduler) indexKey() string {
	return s.opts.keyPrefix + "index"
}

// eventKey returns the Redis key for the per-event sorted set.
// Score is the scheduled unix timestamp; members are message IDs.
func (s *RedisScheduler) eventKey(eventName string) string {
	return s.opts.keyPrefix + "events:" + eventName
}

// countPending returns the number of pending scheduled messages.
func (s *RedisScheduler) countPending(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, s.key()).Result()
}

// countStuck returns the number of messages stuck in processing (older than stuckDuration).
func (s *RedisScheduler) countStuck(ctx context.Context) (int64, error) {
	cutoff := time.Now().Add(-s.opts.stuckDuration).Unix()
	return s.client.ZCount(ctx, s.processingKey(), "-inf", fmt.Sprintf("%d", cutoff)).Result()
}

// setupMetricsCallbacks configures the metrics gauge callbacks for pending and stuck messages.
// This should be called after creating the scheduler if metrics are enabled.
func (s *RedisScheduler) setupMetricsCallbacks(ctx context.Context) {
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

// Health performs a health check on the Redis scheduler.
//
// The health check:
//   - Pings Redis to verify connectivity
//   - Counts pending messages
//   - Counts stuck messages (if any)
//
// Returns health.StatusHealthy if Redis is responsive and no stuck messages.
// Returns health.StatusDegraded if stuck messages exist.
// Returns health.StatusUnhealthy if Redis is not responsive.
func (s *RedisScheduler) Health(ctx context.Context) *health.Result {
	start := time.Now()

	// Ping Redis
	if err := s.client.Ping(ctx).Err(); err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("redis ping failed: %v", err),
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
			"key_prefix":       s.opts.keyPrefix,
		},
	}
}

// Compile-time checks
var (
	_ Scheduler     = (*RedisScheduler)(nil)
	_ HealthChecker = (*RedisScheduler)(nil)
)
