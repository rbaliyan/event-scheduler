package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/google/uuid"
	eventerrors "github.com/rbaliyan/event/v3/errors"
	"github.com/rbaliyan/event/v3/health"
	"github.com/rbaliyan/event/v3/transport"
)

/*
PostgreSQL Schema:

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
*/

// PostgresScheduler uses PostgreSQL for scheduling
type PostgresScheduler struct {
	db        *sql.DB
	transport transport.Transport
	opts      *options
	table     string
	logger    *slog.Logger
	stopCh    chan struct{}
	stoppedCh chan struct{}
	stopOnce  sync.Once
}

// NewPostgresScheduler creates a new PostgreSQL-based scheduler.
//
// Parameters:
//   - db: required - PostgreSQL database connection (must not be nil)
//   - t: required - transport for publishing messages (must not be nil)
//   - opts: optional configuration options
//
// Panics if db or t is nil (programming error).
func NewPostgresScheduler(db *sql.DB, t transport.Transport, opts ...Option) *PostgresScheduler {
	eventerrors.RequireNotNil(db, "db")
	eventerrors.RequireNotNil(t, "transport")

	o := defaultOptions()
	for _, opt := range opts {
		opt(o)
	}

	return &PostgresScheduler{
		db:        db,
		transport: t,
		opts:      o,
		table:     o.table,
		logger:    o.logger.With("component", "scheduler.postgres"),
		stopCh:    make(chan struct{}),
		stoppedCh: make(chan struct{}),
	}
}

// WithTable sets a custom table name
func (s *PostgresScheduler) WithTable(table string) *PostgresScheduler {
	s.table = table
	return s
}

// WithLogger sets a custom logger
func (s *PostgresScheduler) WithLogger(l *slog.Logger) *PostgresScheduler {
	s.logger = l
	return s
}

// Schedule adds a message for future delivery
func (s *PostgresScheduler) Schedule(ctx context.Context, msg Message) error {
	if msg.ID == "" {
		msg.ID = uuid.New().String()
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}

	metadata, err := json.Marshal(msg.Metadata)
	if err != nil {
		return fmt.Errorf("marshal metadata: %w", err)
	}

	query := fmt.Sprintf(`
		INSERT INTO %s (id, event_name, payload, metadata, scheduled_at, created_at, retry_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7)
	`, s.table)

	_, err = s.db.ExecContext(ctx, query,
		msg.ID,
		msg.EventName,
		msg.Payload,
		metadata,
		msg.ScheduledAt,
		msg.CreatedAt,
		msg.RetryCount,
	)

	if err != nil {
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
func (s *PostgresScheduler) Cancel(ctx context.Context, id string) error {
	// First, get the event name for metrics if enabled
	var eventName string
	if s.opts.metrics != nil {
		query := fmt.Sprintf("SELECT event_name FROM %s WHERE id = $1", s.table)
		_ = s.db.QueryRowContext(ctx, query, id).Scan(&eventName)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.table)

	result, err := s.db.ExecContext(ctx, query, id)
	if err != nil {
		return fmt.Errorf("delete: %w", err)
	}

	rows, _ := result.RowsAffected()
	if rows == 0 {
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
func (s *PostgresScheduler) Get(ctx context.Context, id string) (*Message, error) {
	query := fmt.Sprintf(`
		SELECT id, event_name, payload, metadata, scheduled_at, created_at, retry_count
		FROM %s
		WHERE id = $1
	`, s.table)

	var msg Message
	var metadata []byte

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&msg.ID,
		&msg.EventName,
		&msg.Payload,
		&metadata,
		&msg.ScheduledAt,
		&msg.CreatedAt,
		&msg.RetryCount,
	)

	if err == sql.ErrNoRows {
		return nil, fmt.Errorf("%s: %w", id, ErrNotFound)
	}
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}

	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &msg.Metadata); err != nil {
			return nil, fmt.Errorf("unmarshal metadata: %w", err)
		}
	}

	return &msg, nil
}

// List returns scheduled messages
func (s *PostgresScheduler) List(ctx context.Context, filter Filter) ([]*Message, error) {
	query := fmt.Sprintf(`
		SELECT id, event_name, payload, metadata, scheduled_at, created_at, retry_count
		FROM %s
		WHERE 1=1
	`, s.table)

	var args []interface{}
	argIndex := 1

	if filter.EventName != "" {
		query += fmt.Sprintf(" AND event_name = $%d", argIndex)
		args = append(args, filter.EventName)
		argIndex++
	}

	if !filter.After.IsZero() {
		query += fmt.Sprintf(" AND scheduled_at >= $%d", argIndex)
		args = append(args, filter.After)
		argIndex++
	}

	if !filter.Before.IsZero() {
		query += fmt.Sprintf(" AND scheduled_at <= $%d", argIndex)
		args = append(args, filter.Before)
		argIndex++
	}

	query += " ORDER BY scheduled_at ASC"

	if filter.Limit > 0 {
		query += fmt.Sprintf(" LIMIT $%d", argIndex)
		args = append(args, filter.Limit)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer rows.Close()

	var messages []*Message
	for rows.Next() {
		var msg Message
		var metadata []byte

		err := rows.Scan(
			&msg.ID,
			&msg.EventName,
			&msg.Payload,
			&metadata,
			&msg.ScheduledAt,
			&msg.CreatedAt,
			&msg.RetryCount,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &msg.Metadata); err != nil {
				return nil, fmt.Errorf("unmarshal metadata: %w", err)
			}
		}

		messages = append(messages, &msg)
	}

	return messages, nil
}

// Start begins the scheduler polling loop
//
// When adaptive polling is enabled, the poll interval adjusts dynamically:
// - Decreases when messages are found (more activity expected)
// - Increases when no messages are found (less activity expected)
func (s *PostgresScheduler) Start(ctx context.Context) error {
	currentInterval := s.opts.pollInterval
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

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
		}
	}
}

// Stop gracefully stops the scheduler
func (s *PostgresScheduler) Stop(ctx context.Context) error {
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

// processDue processes messages that are due for delivery
// Returns the number of messages processed (for adaptive polling).
func (s *PostgresScheduler) processDue(ctx context.Context) int {
	// Reset backoff state at the start of each processing cycle
	if s.opts.backoff != nil {
		if r, ok := s.opts.backoff.(Resetter); ok {
			r.Reset()
		}
	}

	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		s.logger.Error("failed to begin transaction", "error", err)
		return 0
	}
	defer func() { _ = tx.Rollback() }()

	// Lock and fetch due messages
	query := fmt.Sprintf(`
		SELECT id, event_name, payload, metadata, scheduled_at, created_at, retry_count
		FROM %s
		WHERE scheduled_at <= $1
		ORDER BY scheduled_at ASC
		LIMIT $2
		FOR UPDATE SKIP LOCKED
	`, s.table)

	rows, err := tx.QueryContext(ctx, query, time.Now(), s.opts.batchSize)
	if err != nil {
		s.logger.Error("failed to query due messages", "error", err)
		return 0
	}
	defer rows.Close()

	var toDelete []string
	var toDiscard []string // exceeded maxRetries
	type retryUpdate struct {
		ID         string
		RetryCount int
		NextRetry  time.Time
	}
	var toRetry []retryUpdate

	for rows.Next() {
		processingStart := time.Now()

		var msg Message
		var metadata []byte

		err := rows.Scan(
			&msg.ID,
			&msg.EventName,
			&msg.Payload,
			&metadata,
			&msg.ScheduledAt,
			&msg.CreatedAt,
			&msg.RetryCount,
		)
		if err != nil {
			s.logger.Error("failed to scan message", "error", err)
			continue
		}

		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &msg.Metadata); err != nil {
				s.logger.Error("failed to unmarshal metadata", "error", err)
				continue
			}
		}

		// Publish to transport
		if publishErr := publishScheduledMessage(ctx, s.transport, &msg); publishErr != nil {
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

				toDiscard = append(toDiscard, msg.ID)
				continue
			}

			// Schedule retry with backoff
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

			toRetry = append(toRetry, retryUpdate{
				ID:         msg.ID,
				RetryCount: newRetryCount,
				NextRetry:  nextRetryAt,
			})
			continue
		}

		toDelete = append(toDelete, msg.ID)

		// Record delivery metrics
		if s.opts.metrics != nil {
			s.opts.metrics.RecordDelivered(ctx, msg.EventName, msg.ScheduledAt, processingStart)
		}

		s.logger.Debug("delivered scheduled message",
			"id", msg.ID,
			"event", msg.EventName)
	}

	// Delete delivered messages
	if len(toDelete) > 0 {
		deleteQuery := fmt.Sprintf("DELETE FROM %s WHERE id = ANY($1)", s.table)
		_, err = tx.ExecContext(ctx, deleteQuery, toDelete)
		if err != nil {
			s.logger.Error("failed to delete delivered messages", "error", err)
			return 0
		}
	}

	// Delete discarded messages (max retries exceeded)
	if len(toDiscard) > 0 {
		discardQuery := fmt.Sprintf("DELETE FROM %s WHERE id = ANY($1)", s.table)
		_, err = tx.ExecContext(ctx, discardQuery, toDiscard)
		if err != nil {
			s.logger.Error("failed to delete discarded messages", "error", err)
			return 0
		}
	}

	// Update messages for retry (new scheduled_at + incremented retry_count)
	for _, r := range toRetry {
		updateQuery := fmt.Sprintf(
			"UPDATE %s SET retry_count = $1, scheduled_at = $2 WHERE id = $3",
			s.table)
		_, err = tx.ExecContext(ctx, updateQuery, r.RetryCount, r.NextRetry, r.ID)
		if err != nil {
			s.logger.Error("failed to update message for retry",
				"id", r.ID,
				"error", err)
		}
	}

	if err := tx.Commit(); err != nil {
		s.logger.Error("failed to commit transaction", "error", err)
		return 0
	}

	return len(toDelete)
}

// EnsureTable creates the scheduled_messages table if it doesn't exist.
func (s *PostgresScheduler) EnsureTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id           VARCHAR(36) PRIMARY KEY,
			event_name   VARCHAR(255) NOT NULL,
			payload      BYTEA NOT NULL,
			metadata     JSONB,
			scheduled_at TIMESTAMP NOT NULL,
			created_at   TIMESTAMP NOT NULL DEFAULT NOW(),
			retry_count  INTEGER NOT NULL DEFAULT 0
		);
		CREATE INDEX IF NOT EXISTS idx_%s_scheduled_due ON %s(scheduled_at);
	`, s.table, s.table, s.table)
	_, err := s.db.ExecContext(ctx, query)
	return err
}

// MigrateAddRetryCount adds the retry_count column to an existing table.
// Safe to call multiple times; uses ADD COLUMN IF NOT EXISTS.
func (s *PostgresScheduler) MigrateAddRetryCount(ctx context.Context) error {
	query := fmt.Sprintf(
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0",
		s.table)
	_, err := s.db.ExecContext(ctx, query)
	return err
}

// CountPending returns the number of pending scheduled messages.
// This is useful for monitoring and metrics.
func (s *PostgresScheduler) CountPending(ctx context.Context) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s", s.table)
	var count int64
	err := s.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// SetupMetricsCallbacks configures the metrics gauge callbacks for pending messages.
// This should be called after creating the scheduler if metrics are enabled.
//
// Note: PostgreSQL scheduler doesn't have a separate processing state like Redis/MongoDB,
// so the stuck messages gauge will always be 0.
//
// Example:
//
//	metrics, _ := scheduler.NewMetrics()
//	s := scheduler.NewPostgresScheduler(db, transport, scheduler.WithMetrics(metrics))
//	s.SetupMetricsCallbacks(ctx)
func (s *PostgresScheduler) SetupMetricsCallbacks(ctx context.Context) {
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

	// PostgreSQL uses FOR UPDATE SKIP LOCKED, so there's no separate "stuck" state
	// Messages are either pending or being processed in a transaction
	s.opts.metrics.SetStuckCallback(func() int64 {
		return 0
	})
}

// Health performs a health check on the PostgreSQL scheduler.
//
// The health check:
//   - Pings PostgreSQL to verify connectivity
//   - Counts pending messages
//
// Note: PostgreSQL scheduler uses FOR UPDATE SKIP LOCKED, so there's no
// separate "stuck" state. Messages are atomically locked within transactions.
//
// Returns HealthStatusHealthy if PostgreSQL is responsive.
// Returns HealthStatusUnhealthy if PostgreSQL is not responsive.
func (s *PostgresScheduler) Health(ctx context.Context) *health.Result {
	start := time.Now()

	// Ping PostgreSQL
	if err := s.db.PingContext(ctx); err != nil {
		return &health.Result{
			Status:    HealthStatusUnhealthy,
			Message:   fmt.Sprintf("postgres ping failed: %v", err),
			Latency:   time.Since(start),
			CheckedAt: start,
		}
	}

	// Count pending messages
	pending, err := s.CountPending(ctx)
	message := ""
	status := HealthStatusHealthy
	if err != nil {
		status = HealthStatusDegraded
		message = fmt.Sprintf("failed to count pending: %v", err)
	}

	return &health.Result{
		Status:    status,
		Message:   message,
		Latency:   time.Since(start),
		CheckedAt: start,
		Details: map[string]any{
			"pending_messages": pending,
			"stuck_messages":   int64(0), // PostgreSQL uses FOR UPDATE SKIP LOCKED
			"table":            s.table,
		},
	}
}

// Compile-time checks
var (
	_ Scheduler     = (*PostgresScheduler)(nil)
	_ HealthChecker = (*PostgresScheduler)(nil)
)
