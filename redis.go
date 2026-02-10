package scheduler

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	eventerrors "github.com/rbaliyan/event/v3/errors"
	"github.com/rbaliyan/event/v3/health"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/redis/go-redis/v9"
)

// RedisScheduler uses Redis sorted sets for scheduling.
//
// RedisScheduler provides distributed scheduled message delivery using Redis.
// Messages are stored in a sorted set with the scheduled time as the score,
// enabling efficient retrieval of due messages.
//
// Redis Data Structure:
//   - Sorted Set: {prefix}messages - score=scheduled_time, member=JSON message
//   - Sorted Set: {prefix}processing - messages being processed (for crash recovery)
//
// The scheduler uses a 2-phase approach for HA safety:
//  1. Atomically move message from "messages" to "processing" set
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
	client        redis.Cmdable
	transport     transport.Transport
	opts          *options
	logger        *slog.Logger
	stopCh        chan struct{}
	stoppedCh     chan struct{}
	stopOnce      sync.Once
	stuckDuration time.Duration // How long before a processing message is considered stuck
}

// NewRedisScheduler creates a new Redis-based scheduler.
//
// Parameters:
//   - client: A connected Redis client (supports single node, Sentinel, Cluster)
//   - t: Transport for publishing due messages
//   - opts: Optional configuration options
//
// Panics if client or t is nil (programming error).
//
// Example:
//
//	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
//	scheduler := scheduler.NewRedisScheduler(rdb, transport)
func NewRedisScheduler(client redis.Cmdable, t transport.Transport, opts ...Option) *RedisScheduler {
	eventerrors.RequireNotNil(client, "client")
	eventerrors.RequireNotNil(t, "transport")

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &RedisScheduler{
		client:        client,
		transport:     t,
		opts:          o,
		logger:        o.logger.With("component", "scheduler.redis"),
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
		stuckDuration: o.stuckDuration,
	}
}

// Schedule adds a message for future delivery.
//
// The message is stored in a Redis sorted set with ScheduledAt as the score.
// If ID is empty, a UUID is generated. If CreatedAt is zero, it's set to now.
func (s *RedisScheduler) Schedule(ctx context.Context, msg Message) error {
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

	// Store in sorted set with scheduled time as score and index for O(1) lookups
	pipe := s.client.Pipeline()
	pipe.ZAdd(ctx, s.key(), redis.Z{
		Score:  float64(msg.ScheduledAt.Unix()),
		Member: data,
	})
	pipe.HSet(ctx, s.indexKey(), msg.ID, data)
	if _, err = pipe.Exec(ctx); err != nil {
		return fmt.Errorf("schedule pipeline: %w", err)
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
// Looks up the message in the index and removes it from both the sorted set and index.
// Returns error if the message is not found.
func (s *RedisScheduler) Cancel(ctx context.Context, id string) error {
	// Look up the member from the index
	member, err := s.client.HGet(ctx, s.indexKey(), id).Result()
	if err != nil {
		if err == redis.Nil {
			return fmt.Errorf("%s: %w", id, ErrNotFound)
		}
		return fmt.Errorf("hget index: %w", err)
	}

	var msg Message
	if err := json.Unmarshal([]byte(member), &msg); err != nil {
		return fmt.Errorf("unmarshal: %w", err)
	}

	// Remove from sorted set and index atomically via pipeline
	pipe := s.client.Pipeline()
	pipe.ZRem(ctx, s.key(), member)
	pipe.ZRem(ctx, s.processingKey(), member)
	pipe.HDel(ctx, s.indexKey(), id)
	if _, err := pipe.Exec(ctx); err != nil {
		return fmt.Errorf("cancel pipeline: %w", err)
	}

	// Record metrics
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
	member, err := s.client.HGet(ctx, s.indexKey(), id).Result()
	if err != nil {
		if err == redis.Nil {
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
// Uses ZRANGEBYSCORE to efficiently query by scheduled time.
// Returns empty slice if no matches.
func (s *RedisScheduler) List(ctx context.Context, filter Filter) ([]*Message, error) {
	min := "-inf"
	max := "+inf"

	if !filter.After.IsZero() {
		min = fmt.Sprintf("%d", filter.After.Unix())
	}
	if !filter.Before.IsZero() {
		max = fmt.Sprintf("%d", filter.Before.Unix())
	}

	results, err := s.client.ZRangeByScore(ctx, s.key(), &redis.ZRangeBy{
		Min: min,
		Max: max,
	}).Result()

	if err != nil {
		return nil, fmt.Errorf("zrangebyscore: %w", err)
	}

	var messages []*Message
	for _, result := range results {
		var msg Message
		if err := json.Unmarshal([]byte(result), &msg); err != nil {
			continue
		}

		if filter.EventName != "" && msg.EventName != filter.EventName {
			continue
		}

		messages = append(messages, &msg)

		if filter.Limit > 0 && len(messages) >= filter.Limit {
			break
		}
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
	currentInterval := s.opts.pollInterval
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	// Recovery ticker for stuck messages (check every minute)
	recoveryTicker := time.NewTicker(time.Minute)
	defer recoveryTicker.Stop()

	// Initialize adaptive polling state if enabled
	var adaptiveState *adaptivePollState
	if s.opts.adaptivePolling {
		adaptiveState = newAdaptivePollState(
			s.opts.pollInterval,
			s.opts.minPollInterval,
			s.opts.maxPollInterval,
		)
		s.logger.Info("scheduler started with adaptive polling",
			"initial_interval", s.opts.pollInterval,
			"min_interval", s.opts.minPollInterval,
			"max_interval", s.opts.maxPollInterval,
			"batch_size", s.opts.batchSize)
	} else {
		s.logger.Info("scheduler started",
			"poll_interval", s.opts.pollInterval,
			"batch_size", s.opts.batchSize)
	}

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
			processed := s.processDue(ctx)

			// Adjust poll interval if adaptive polling is enabled
			if adaptiveState != nil {
				newInterval := adaptiveState.adjust(processed)
				if newInterval != currentInterval {
					currentInterval = newInterval
					ticker.Reset(currentInterval)
				}
			}
		case <-recoveryTicker.C:
			s.recoverStuck(ctx)
		}
	}
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
			if err == redis.Nil {
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

			// Record failure metrics
			if s.opts.metrics != nil {
				s.opts.metrics.RecordFailed(ctx, msg.EventName, "publish_error")
			}

			// Remove from processing set
			s.client.ZRem(ctx, s.processingKey(), member)

			// Check if max retries exceeded
			if s.opts.maxRetries > 0 && msg.RetryCount >= s.opts.maxRetries {
				s.logger.Error("scheduled message exceeded max retries, discarding",
					"id", msg.ID,
					"event", msg.EventName,
					"retry_count", msg.RetryCount,
					"max_retries", s.opts.maxRetries)

				// Send to DLQ if configured
				if s.opts.dlq != nil {
					if dlqErr := s.opts.dlq.Store(ctx, msg.EventName, msg.ID, msg.Payload, msg.Metadata, err, msg.RetryCount, "scheduler"); dlqErr != nil {
						s.logger.Error("failed to send message to DLQ",
							"id", msg.ID,
							"error", dlqErr)
					} else if s.opts.metrics != nil {
						s.opts.metrics.RecordDLQSent(ctx, msg.EventName)
					}
				}

				// Clean up index entry
				s.client.HDel(ctx, s.indexKey(), msg.ID)
				continue
			}

			// Increment retry count and calculate next retry time
			msg.RetryCount++
			nextRetryAt := time.Now()
			if s.opts.backoff != nil {
				backoffDelay := s.opts.backoff.NextDelay(msg.RetryCount - 1)
				nextRetryAt = nextRetryAt.Add(backoffDelay)
				s.logger.Debug("scheduling retry with backoff",
					"id", msg.ID,
					"retry_count", msg.RetryCount,
					"backoff_delay", backoffDelay,
					"next_retry_at", nextRetryAt)
			}
			msg.ScheduledAt = nextRetryAt

			// Re-serialize with updated retry count and scheduled time
			updatedMember, marshalErr := json.Marshal(msg)
			if marshalErr != nil {
				s.logger.Error("failed to marshal message for retry", "error", marshalErr)
				continue
			}

			// Add back to pending with updated scheduled time and update index
			retryPipe := s.client.Pipeline()
			retryPipe.ZAdd(ctx, s.key(), redis.Z{
				Score:  float64(msg.ScheduledAt.Unix()),
				Member: updatedMember,
			})
			retryPipe.HSet(ctx, s.indexKey(), msg.ID, updatedMember)
			if _, err := retryPipe.Exec(ctx); err != nil {
				s.logger.Error("failed to execute retry pipeline", "id", msg.ID, "error", err)
			}
			continue
		}

		// Remove from processing set and index after successful publish
		s.client.ZRem(ctx, s.processingKey(), member)
		s.client.HDel(ctx, s.indexKey(), msg.ID)

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

// claimDueMessage atomically claims a due message using a Lua script.
// Moves the message from the pending set to the processing set.
// This prevents race conditions in HA deployments where multiple schedulers run.
func (s *RedisScheduler) claimDueMessage(ctx context.Context, now int64) (*Message, string, error) {
	// Lua script to atomically:
	// 1. Get the first item with score <= now from pending
	// 2. Remove it from pending set
	// 3. Add it to processing set with current timestamp as score
	// 4. Return the member
	script := redis.NewScript(`
		local items = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', ARGV[1], 'LIMIT', 0, 1)
		if #items == 0 then
			return nil
		end
		redis.call('ZREM', KEYS[1], items[1])
		redis.call('ZADD', KEYS[2], ARGV[2], items[1])
		return items[1]
	`)

	claimedAt := time.Now().Unix()
	result, err := script.Run(ctx, s.client, []string{s.key(), s.processingKey()}, now, claimedAt).Result()
	if err != nil {
		return nil, "", err
	}

	if result == nil {
		return nil, "", redis.Nil
	}

	member := result.(string)

	var msg Message
	if err := json.Unmarshal([]byte(member), &msg); err != nil {
		// Corrupt message - remove from processing and clean index
		s.client.ZRem(ctx, s.processingKey(), member)
		var partial struct{ ID string }
		if json.Unmarshal([]byte(member), &partial) == nil && partial.ID != "" {
			s.client.HDel(ctx, s.indexKey(), partial.ID)
		}
		s.logger.Error("failed to unmarshal claimed message", "error", err)
		return nil, "", redis.Nil
	}

	return &msg, member, nil
}

// recoverStuck moves messages stuck in "processing" back to "pending".
// This handles scheduler crashes where messages were claimed but never published.
func (s *RedisScheduler) recoverStuck(ctx context.Context) {
	cutoff := time.Now().Add(-s.stuckDuration).Unix()

	// Get messages that have been in processing too long
	results, err := s.client.ZRangeByScoreWithScores(ctx, s.processingKey(), &redis.ZRangeBy{
		Min:   "-inf",
		Max:   fmt.Sprintf("%d", cutoff),
		Count: 100,
	}).Result()

	if err != nil {
		s.logger.Error("failed to get stuck messages", "error", err)
		return
	}

	if len(results) == 0 {
		return
	}

	var recovered int64
	for _, z := range results {
		member := z.Member.(string)

		var msg Message
		if err := json.Unmarshal([]byte(member), &msg); err != nil {
			// Corrupt message - remove from processing and clean index
			s.client.ZRem(ctx, s.processingKey(), member)
			// Try partial decode for ID to clean index
			var partial struct{ ID string }
			if json.Unmarshal([]byte(member), &partial) == nil && partial.ID != "" {
				s.client.HDel(ctx, s.indexKey(), partial.ID)
			}
			continue
		}

		// Move back to pending with original scheduled time
		pipe := s.client.Pipeline()
		pipe.ZRem(ctx, s.processingKey(), member)
		pipe.ZAdd(ctx, s.key(), redis.Z{
			Score:  float64(msg.ScheduledAt.Unix()),
			Member: member,
		})
		if _, err := pipe.Exec(ctx); err == nil {
			recovered++
		}
	}

	if recovered > 0 {
		// Record recovery metrics
		if s.opts.metrics != nil {
			s.opts.metrics.RecordRecovered(ctx, recovered)
		}

		s.logger.Warn("recovered stuck scheduled messages",
			"count", recovered,
			"stuck_duration", s.stuckDuration)
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

// CountPending returns the number of pending scheduled messages.
// This is useful for monitoring and metrics.
func (s *RedisScheduler) CountPending(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, s.key()).Result()
}

// CountProcessing returns the number of messages currently being processed.
// This is useful for monitoring and metrics.
func (s *RedisScheduler) CountProcessing(ctx context.Context) (int64, error) {
	return s.client.ZCard(ctx, s.processingKey()).Result()
}

// CountStuck returns the number of messages stuck in processing (older than stuckDuration).
// This is useful for monitoring and metrics.
func (s *RedisScheduler) CountStuck(ctx context.Context) (int64, error) {
	cutoff := time.Now().Add(-s.stuckDuration).Unix()
	return s.client.ZCount(ctx, s.processingKey(), "-inf", fmt.Sprintf("%d", cutoff)).Result()
}

// SetupMetricsCallbacks configures the metrics gauge callbacks for pending and stuck messages.
// This should be called after creating the scheduler if metrics are enabled.
//
// Example:
//
//	metrics, _ := scheduler.NewMetrics()
//	s := scheduler.NewRedisScheduler(client, transport, scheduler.WithMetrics(metrics))
//	s.SetupMetricsCallbacks(ctx)
func (s *RedisScheduler) SetupMetricsCallbacks(ctx context.Context) {
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

// Health performs a health check on the Redis scheduler.
//
// The health check:
//   - Pings Redis to verify connectivity
//   - Counts pending messages
//   - Counts stuck messages (if any)
//
// Returns HealthStatusHealthy if Redis is responsive and no stuck messages.
// Returns HealthStatusDegraded if stuck messages exist.
// Returns HealthStatusUnhealthy if Redis is not responsive.
func (s *RedisScheduler) Health(ctx context.Context) *health.Result {
	start := time.Now()

	// Ping Redis
	if err := s.client.Ping(ctx).Err(); err != nil {
		return &health.Result{
			Status:    HealthStatusUnhealthy,
			Message:   fmt.Sprintf("redis ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}

	status := HealthStatusHealthy
	var message string

	// Count pending messages
	pending, err := s.CountPending(ctx)
	if err != nil {
		status = HealthStatusDegraded
		message = fmt.Sprintf("failed to count pending: %v", err)
	}

	// Count stuck messages
	stuck, err := s.CountStuck(ctx)
	if err != nil {
		status = HealthStatusDegraded
		message = fmt.Sprintf("failed to count stuck: %v", err)
	}

	// Degraded if stuck messages exist
	if stuck > 0 && status == HealthStatusHealthy {
		status = HealthStatusDegraded
		message = fmt.Sprintf("%d messages stuck in processing", stuck)
	}

	return &health.Result{
		Status:    status,
		Message:   message,
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
