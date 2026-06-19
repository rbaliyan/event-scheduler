package scheduler

import (
	"context"
	"database/sql"
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

	"github.com/lib/pq" // pq driver registration, pq.Listener (via opts.listener), and pq.Array
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
    retry_count  INTEGER NOT NULL DEFAULT 0,
    status       VARCHAR(20) NOT NULL DEFAULT 'pending',
    claimed_at   TIMESTAMP
);

CREATE INDEX idx_scheduled_due ON scheduled_messages(status, scheduled_at);

Like the Redis and MongoDB backends, delivery is a 2-phase claim:
the claim query atomically flips due rows from 'pending' to 'processing'
(via UPDATE ... FOR UPDATE SKIP LOCKED ... RETURNING), the transport publish
happens outside any row lock, and a short follow-up statement deletes or
reschedules each row. recoverStuck moves rows abandoned in 'processing'
(e.g. after a crash) back to 'pending' once they exceed stuckDuration.
*/

// PostgresScheduler uses PostgreSQL for scheduling
type PostgresScheduler struct {
	db            *sql.DB
	transport     transport.Transport
	opts          *options
	table         string
	logger        *slog.Logger
	stopCh        chan struct{}
	stoppedCh     chan struct{}
	stopOnce      sync.Once
	notifyChannel string // empty = no NOTIFY on Schedule
}

// NewPostgresScheduler creates a new PostgreSQL-based scheduler.
//
// Parameters:
//   - db: required - PostgreSQL database connection (must not be nil)
//   - t: required - transport for publishing messages (must not be nil)
//   - opts: optional configuration options
//
// Returns an error if db or t is nil.
func NewPostgresScheduler(db *sql.DB, t transport.Transport, opts ...Option) (*PostgresScheduler, error) {
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

	if !validIdentifier.MatchString(o.table) {
		return nil, fmt.Errorf("scheduler: invalid table name %q", o.table)
	}

	return &PostgresScheduler{
		db:            db,
		transport:     t,
		opts:          o,
		table:         o.table,
		logger:        o.logger.With("component", "scheduler.postgres"),
		stopCh:        make(chan struct{}),
		stoppedCh:     make(chan struct{}),
		notifyChannel: o.notifyChannel,
	}, nil
}

// Schedule adds a message for future delivery
func (s *PostgresScheduler) Schedule(ctx context.Context, msg Message) error {
	if msg.EventName == "" {
		return fmt.Errorf("schedule: event name must not be empty")
	}
	if msg.ScheduledAt.IsZero() {
		return fmt.Errorf("schedule: scheduled_at must not be zero")
	}
	if err := validateRecurrence(msg.Recurrence); err != nil {
		return err
	}
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

	recType, recVal, maxOcc, until := encodeRecurrence(msg.Recurrence)

	// #nosec G201 -- table name is set at construction, not user input
	query := fmt.Sprintf(`
		INSERT INTO %s (id, event_name, payload, metadata, scheduled_at, created_at, retry_count,
		                recurrence_type, recurrence_value, max_occurrences, until, occurrence_count)
		VALUES ($1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11, $12)
	`, s.table)

	_, err = s.db.ExecContext(ctx, query,
		msg.ID,
		msg.EventName,
		msg.Payload,
		metadata,
		msg.ScheduledAt,
		msg.CreatedAt,
		msg.RetryCount,
		recType, recVal, maxOcc, until, msg.OccurrenceCount,
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

	// Notify any listening scheduler loop that a new message has been scheduled.
	// Best-effort: polling catches any missed notifications.
	if s.notifyChannel != "" {
		if _, err := s.db.ExecContext(ctx, `SELECT pg_notify($1, '')`, s.notifyChannel); err != nil {
			s.logger.Debug("pg_notify failed, relying on poller",
				"channel", s.notifyChannel, "error", err)
		}
	}

	return nil
}

// Cancel cancels a scheduled message
func (s *PostgresScheduler) Cancel(ctx context.Context, id string) error {
	if id == "" {
		return fmt.Errorf("cancel: id must not be empty")
	}

	// First, get the event name for metrics if enabled
	var eventName string
	if s.opts.metrics != nil {
		query := fmt.Sprintf("SELECT event_name FROM %s WHERE id = $1", s.table) // #nosec G201 -- table name is set at construction, not user input
		_ = s.db.QueryRowContext(ctx, query, id).Scan(&eventName)
	}

	query := fmt.Sprintf("DELETE FROM %s WHERE id = $1", s.table) // #nosec G201 -- table name is set at construction, not user input

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
	if id == "" {
		return nil, fmt.Errorf("get: id must not be empty")
	}
	// #nosec G201 -- table name is set at construction, not user input
	query := fmt.Sprintf(`
		SELECT id, event_name, payload, metadata, scheduled_at, created_at, retry_count,
		       recurrence_type, recurrence_value, max_occurrences, until, occurrence_count
		FROM %s
		WHERE id = $1
	`, s.table)

	var msg Message
	var metadata []byte
	var recType, recVal sql.NullString
	var maxOcc, occCount sql.NullInt32
	var until sql.NullTime

	err := s.db.QueryRowContext(ctx, query, id).Scan(
		&msg.ID,
		&msg.EventName,
		&msg.Payload,
		&metadata,
		&msg.ScheduledAt,
		&msg.CreatedAt,
		&msg.RetryCount,
		&recType, &recVal, &maxOcc, &until, &occCount,
	)

	if errors.Is(err, sql.ErrNoRows) {
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
	msg.Recurrence = decodeRecurrence(recType, recVal, maxOcc, until)
	if occCount.Valid {
		msg.OccurrenceCount = int(occCount.Int32)
	}

	return &msg, nil
}

// #nosec G201 -- table name is set at construction, not user input
// List returns scheduled messages
func (s *PostgresScheduler) List(ctx context.Context, filter Filter) ([]*Message, error) {
	query := fmt.Sprintf(`
		SELECT id, event_name, payload, metadata, scheduled_at, created_at, retry_count,
		       recurrence_type, recurrence_value, max_occurrences, until, occurrence_count
		FROM %s
		WHERE 1=1
	`, s.table)

	var args []any
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
		argIndex++
	}

	if filter.Offset > 0 {
		query += fmt.Sprintf(" OFFSET $%d", argIndex)
		args = append(args, filter.Offset)
	}

	rows, err := s.db.QueryContext(ctx, query, args...)
	if err != nil {
		return nil, fmt.Errorf("query: %w", err)
	}
	defer func() { _ = rows.Close() }()

	var messages []*Message
	for rows.Next() {
		var msg Message
		var metadata []byte
		var recType, recVal sql.NullString
		var maxOcc, occCount sql.NullInt32
		var until sql.NullTime

		err := rows.Scan(
			&msg.ID,
			&msg.EventName,
			&msg.Payload,
			&metadata,
			&msg.ScheduledAt,
			&msg.CreatedAt,
			&msg.RetryCount,
			&recType, &recVal, &maxOcc, &until, &occCount,
		)
		if err != nil {
			return nil, fmt.Errorf("scan: %w", err)
		}

		if len(metadata) > 0 {
			if err := json.Unmarshal(metadata, &msg.Metadata); err != nil {
				return nil, fmt.Errorf("unmarshal metadata: %w", err)
			}
		}
		msg.Recurrence = decodeRecurrence(recType, recVal, maxOcc, until)
		if occCount.Valid {
			msg.OccurrenceCount = int(occCount.Int32)
		}

		messages = append(messages, &msg)
	}

	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("rows iteration: %w", err)
	}

	return messages, nil
}

// Start begins the scheduler polling loop.
// Also periodically recovers messages stuck in "processing" state
// (from crashed scheduler instances).
//
// When adaptive polling is enabled, the poll interval adjusts dynamically:
// - Decreases when messages are found (more activity expected)
// - Increases when no messages are found (less activity expected)
func (s *PostgresScheduler) Start(ctx context.Context) error {
	s.setupMetricsCallbacks(ctx)

	// Build notify channel if a pq.Listener is configured.
	// The listener bridges PG NOTIFY events into a plain struct channel so
	// runSchedulerLoop stays decoupled from the pq package.
	var notifyCh <-chan struct{}
	var bridgeDone <-chan struct{} // non-nil only when the bridge goroutine is running
	if s.opts.listener != nil {
		ch := make(chan struct{}, 1)
		notifyCh = ch
		if err := s.opts.listener.Listen(s.opts.notifyChannel); err != nil {
			s.logger.Warn("failed to listen on notify channel, using polling only",
				"channel", s.opts.notifyChannel, "error", err)
		} else {
			done := make(chan struct{})
			bridgeDone = done
			go func() {
				defer close(done)
				defer func() { _ = s.opts.listener.Unlisten(s.opts.notifyChannel) }()
				for {
					select {
					case <-ctx.Done():
						return
					case <-s.stopCh:
						return
					case <-s.opts.listener.Notify:
						select {
						case ch <- struct{}{}:
						default: // coalesce multiple rapid notifications
						}
					}
				}
			}()
		}
	}

	// onExit waits for the bridge goroutine to finish its Unlisten before
	// signalling stoppedCh. This ensures Stop() does not return while the
	// pq.Listener is still subscribed to the channel.
	onExit := func() {
		if bridgeDone != nil {
			<-bridgeDone
		}
		close(s.stoppedCh)
	}

	runSchedulerLoop(
		s.logger,
		s.opts,
		ctx.Done(),
		s.stopCh,
		func() int { return s.processDue(ctx) },
		func() { s.recoverStuck(ctx) },
		notifyCh,
		onExit,
	)

	if ctx.Err() != nil {
		return ctx.Err()
	}
	return nil
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

// retryUpdate holds the retry scheduling state for a failed message.
type retryUpdate struct {
	ID         string
	RetryCount int
	NextRetry  time.Time
}

// recurrenceUpdate holds the reschedule state for a successfully delivered recurring message.
type recurrenceUpdate struct {
	ID              string
	NextAt          time.Time
	OccurrenceCount int
}

// processDue processes messages that are due for delivery.
//
// Uses the same 2-phase claim discipline as the Redis and MongoDB backends:
//  1. Atomically claim due rows (status: pending -> processing) via
//     UPDATE ... FOR UPDATE SKIP LOCKED ... RETURNING, in a short transaction.
//  2. Publish to the transport outside any row lock, so a slow or blocked
//     transport never holds locks or a transaction open.
//  3. Delete or reschedule each row in a short follow-up transaction.
//
// If the scheduler crashes after claiming, recoverStuck moves the row back to
// pending. Returns the number of messages processed (for adaptive polling).
func (s *PostgresScheduler) processDue(ctx context.Context) int {
	resetBackoff(s.opts)

	claimed, err := s.claimDue(ctx)
	if err != nil {
		s.logger.Error("failed to claim due messages", "error", err)
		return 0
	}
	if len(claimed) == 0 {
		return 0
	}

	toDelete, toDiscard, toRetry, toRecur := s.deliverBatch(ctx, claimed)

	if err := s.applyBatchResults(ctx, toDelete, toDiscard, toRetry, toRecur); err != nil {
		s.logger.Error("failed to apply batch results", "error", err)
		// Rows remain in 'processing' and will be recovered by recoverStuck.
		return 0
	}

	// Report every handled message (delivered, discarded, retried, or rescheduled)
	// so adaptive polling sees real activity for recurring and failing workloads,
	// not just terminal one-shot deliveries.
	return len(toDelete) + len(toDiscard) + len(toRetry) + len(toRecur)
}

// claimDue atomically claims due pending messages for processing and returns them.
//
// The inner SELECT ... FOR UPDATE SKIP LOCKED lets concurrent schedulers claim
// disjoint batches without blocking each other; the surrounding UPDATE flips the
// claimed rows to 'processing' and RETURNING streams them back in a single
// statement, so no lock is held while the caller publishes.
func (s *PostgresScheduler) claimDue(ctx context.Context) ([]*Message, error) {
	// #nosec G201 -- table name is set at construction, not user input
	query := fmt.Sprintf(`
		UPDATE %s SET status = 'processing', claimed_at = $1
		WHERE id IN (
			SELECT id FROM %s
			WHERE scheduled_at <= $2 AND (status = 'pending' OR status IS NULL)
			ORDER BY scheduled_at ASC
			LIMIT $3
			FOR UPDATE SKIP LOCKED
		)
		RETURNING id, event_name, payload, metadata, scheduled_at, created_at, retry_count,
		          recurrence_type, recurrence_value, max_occurrences, until, occurrence_count
	`, s.table, s.table)

	now := time.Now()
	rows, err := s.db.QueryContext(ctx, query, now, now, s.opts.batchSize)
	if err != nil {
		return nil, err
	}
	defer func() { _ = rows.Close() }()

	var claimed []*Message
	for rows.Next() {
		msg, scanErr := scanPostgresMessage(rows)
		if scanErr != nil {
			s.logger.Error("failed to scan claimed message", "error", scanErr)
			continue
		}
		claimed = append(claimed, msg)
	}
	if rowErr := rows.Err(); rowErr != nil {
		return nil, rowErr
	}
	return claimed, nil
}

// deliverBatch publishes each claimed message outside any lock and classifies
// the outcome as delivered (toDelete), DLQ/discarded (toDiscard), to retry, or to recur.
func (s *PostgresScheduler) deliverBatch(ctx context.Context, claimed []*Message) (toDelete, toDiscard []string, toRetry []retryUpdate, toRecur []recurrenceUpdate) {
	for _, msg := range claimed {
		processingStart := time.Now()

		if publishErr := publishScheduledMessage(ctx, s.transport, msg); publishErr != nil {
			s.logger.Error("failed to publish scheduled message",
				"id", msg.ID,
				"event", msg.EventName,
				"error", publishErr,
				"retry_count", msg.RetryCount)
			decision := handleDeliveryFailure(ctx, s.opts, msg, publishErr, s.logger)
			if decision.sendToDLQ {
				toDiscard = append(toDiscard, msg.ID)
			} else {
				toRetry = append(toRetry, retryUpdate{
					ID:         msg.ID,
					RetryCount: decision.retryCount,
					NextRetry:  decision.nextRetryAt,
				})
			}
			continue
		}

		if s.opts.metrics != nil {
			s.opts.metrics.RecordDelivered(ctx, msg.EventName, msg.ScheduledAt, processingStart)
		}
		s.logger.Debug("delivered scheduled message", "id", msg.ID, "event", msg.EventName)

		outcome := handleSuccessfulDelivery(ctx, s.opts, msg, time.Now())
		if outcome.terminal {
			toDelete = append(toDelete, msg.ID)
		} else {
			toRecur = append(toRecur, recurrenceUpdate{
				ID:              msg.ID,
				NextAt:          outcome.nextAt,
				OccurrenceCount: outcome.newCount,
			})
		}
	}
	return toDelete, toDiscard, toRetry, toRecur
}

// scanPostgresMessage scans one row into a Message, including JSON metadata and recurrence fields.
func scanPostgresMessage(rows *sql.Rows) (*Message, error) {
	var msg Message
	var metadata []byte
	var recType, recVal sql.NullString
	var maxOcc, occCount sql.NullInt32
	var until sql.NullTime
	if err := rows.Scan(
		&msg.ID, &msg.EventName, &msg.Payload,
		&metadata, &msg.ScheduledAt, &msg.CreatedAt, &msg.RetryCount,
		&recType, &recVal, &maxOcc, &until, &occCount,
	); err != nil {
		return nil, err
	}
	if len(metadata) > 0 {
		if err := json.Unmarshal(metadata, &msg.Metadata); err != nil {
			return nil, err
		}
	}
	msg.Recurrence = decodeRecurrence(recType, recVal, maxOcc, until)
	if occCount.Valid {
		msg.OccurrenceCount = int(occCount.Int32)
	}
	return &msg, nil
}

// applyBatchResults applies deletes, retry-updates, and recurrence-reschedules
// in a single short transaction, after publishing has completed outside any lock.
// Retried and rescheduled rows are returned to 'pending' (claimed_at cleared) so
// the next poll can pick them up.
func (s *PostgresScheduler) applyBatchResults(ctx context.Context, toDelete, toDiscard []string, toRetry []retryUpdate, toRecur []recurrenceUpdate) error {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer func() { _ = tx.Rollback() }()

	if len(toDelete) > 0 {
		// #nosec G201 -- table name is set at construction, not user input
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = ANY($1)", s.table), pq.Array(toDelete)); err != nil {
			return fmt.Errorf("delete delivered messages: %w", err)
		}
	}
	if len(toDiscard) > 0 {
		// #nosec G201 -- table name is set at construction, not user input
		if _, err := tx.ExecContext(ctx, fmt.Sprintf("DELETE FROM %s WHERE id = ANY($1)", s.table), pq.Array(toDiscard)); err != nil {
			return fmt.Errorf("delete discarded messages: %w", err)
		}
	}
	for _, r := range toRetry {
		// #nosec G201 -- table name is set at construction, not user input
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf("UPDATE %s SET retry_count = $1, scheduled_at = $2, status = 'pending', claimed_at = NULL WHERE id = $3", s.table),
			r.RetryCount, r.NextRetry, r.ID,
		); err != nil {
			return fmt.Errorf("update message %s for retry: %w", r.ID, err)
		}
	}
	for _, r := range toRecur {
		// Reset retry_count so the next occurrence gets a fresh retry window.
		// #nosec G201 -- table name is set at construction, not user input
		if _, err := tx.ExecContext(ctx,
			fmt.Sprintf("UPDATE %s SET scheduled_at = $1, occurrence_count = $2, retry_count = 0, status = 'pending', claimed_at = NULL WHERE id = $3", s.table),
			r.NextAt, r.OccurrenceCount, r.ID,
		); err != nil {
			return fmt.Errorf("reschedule recurring message %s: %w", r.ID, err)
		}
	}

	if err := tx.Commit(); err != nil {
		return fmt.Errorf("commit batch results: %w", err)
	}
	return nil
}

// recoverStuck moves messages stuck in 'processing' back to 'pending'.
// This handles scheduler crashes where a row was claimed but never finalized.
func (s *PostgresScheduler) recoverStuck(ctx context.Context) {
	cutoff := time.Now().Add(-s.opts.stuckDuration)
	// #nosec G201 -- table name is set at construction, not user input
	query := fmt.Sprintf(
		"UPDATE %s SET status = 'pending', claimed_at = NULL WHERE status = 'processing' AND claimed_at < $1",
		s.table)
	result, err := s.db.ExecContext(ctx, query, cutoff)
	if err != nil {
		s.logger.Error("failed to recover stuck messages", "error", err)
		return
	}
	recovered, _ := result.RowsAffected()
	if recovered > 0 {
		if s.opts.metrics != nil {
			s.opts.metrics.RecordRecovered(ctx, recovered)
		}
		s.logger.Warn("recovered stuck scheduled messages",
			"count", recovered,
			"stuck_duration", s.opts.stuckDuration)
	}
}

// EnsureTable creates the scheduled_messages table if it doesn't exist.
func (s *PostgresScheduler) EnsureTable(ctx context.Context) error {
	query := fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s (
			id               VARCHAR(36) PRIMARY KEY,
			event_name       VARCHAR(255) NOT NULL,
			payload          BYTEA NOT NULL,
			metadata         JSONB,
			scheduled_at     TIMESTAMP NOT NULL,
			created_at       TIMESTAMP NOT NULL DEFAULT NOW(),
			retry_count      INTEGER NOT NULL DEFAULT 0,
			recurrence_type  VARCHAR(10),
			recurrence_value TEXT,
			max_occurrences  INTEGER NOT NULL DEFAULT 0,
			until            TIMESTAMP,
			occurrence_count INTEGER NOT NULL DEFAULT 0,
			status           VARCHAR(20) NOT NULL DEFAULT 'pending',
			claimed_at       TIMESTAMP
		);
		CREATE INDEX IF NOT EXISTS idx_%s_scheduled_due ON %s(status, scheduled_at);
	`, s.table, s.table, s.table)
	_, err := s.db.ExecContext(ctx, query)
	return err
}

// MigrateAddRetryCount adds the retry_count column to an existing table.
// Safe to call multiple times; uses ADD COLUMN IF NOT EXISTS.
func (s *PostgresScheduler) MigrateAddRetryCount(ctx context.Context) error {
	query := fmt.Sprintf( // #nosec G201 -- table name is set at construction, not user input
		"ALTER TABLE %s ADD COLUMN IF NOT EXISTS retry_count INTEGER NOT NULL DEFAULT 0",
		s.table)
	_, err := s.db.ExecContext(ctx, query)
	return err
}

// MigrateAddRecurrence adds the five recurrence columns to an existing table.
// Safe to call multiple times; uses ADD COLUMN IF NOT EXISTS.
// Call this when upgrading from a version that pre-dates recurrence support.
func (s *PostgresScheduler) MigrateAddRecurrence(ctx context.Context) error {
	query := fmt.Sprintf( // #nosec G201 -- table name is set at construction, not user input
		`ALTER TABLE %s
			ADD COLUMN IF NOT EXISTS recurrence_type  VARCHAR(10),
			ADD COLUMN IF NOT EXISTS recurrence_value TEXT,
			ADD COLUMN IF NOT EXISTS max_occurrences  INTEGER NOT NULL DEFAULT 0,
			ADD COLUMN IF NOT EXISTS until            TIMESTAMP,
			ADD COLUMN IF NOT EXISTS occurrence_count INTEGER NOT NULL DEFAULT 0`,
		s.table)
	_, err := s.db.ExecContext(ctx, query)
	return err
}

// MigrateAddProcessingState adds the status and claimed_at columns to an existing
// table and switches the due index to (status, scheduled_at).
// Safe to call multiple times; uses ADD COLUMN IF NOT EXISTS.
// Call this when upgrading from a version that pre-dates 2-phase claim support.
func (s *PostgresScheduler) MigrateAddProcessingState(ctx context.Context) error {
	query := fmt.Sprintf( // #nosec G201 -- table name is set at construction, not user input
		`ALTER TABLE %s
			ADD COLUMN IF NOT EXISTS status     VARCHAR(20) NOT NULL DEFAULT 'pending',
			ADD COLUMN IF NOT EXISTS claimed_at TIMESTAMP;
		CREATE INDEX IF NOT EXISTS idx_%s_status_due ON %s(status, scheduled_at);`,
		s.table, s.table, s.table)
	_, err := s.db.ExecContext(ctx, query)
	return err
}

// countPending returns the number of pending scheduled messages
// (rows not currently claimed for processing).
func (s *PostgresScheduler) countPending(ctx context.Context) (int64, error) {
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'pending' OR status IS NULL", s.table) // #nosec G201 -- table name is set at construction, not user input
	var count int64
	err := s.db.QueryRowContext(ctx, query).Scan(&count)
	return count, err
}

// countStuck returns the number of messages stuck in processing (older than stuckDuration).
func (s *PostgresScheduler) countStuck(ctx context.Context) (int64, error) {
	cutoff := time.Now().Add(-s.opts.stuckDuration)
	query := fmt.Sprintf("SELECT COUNT(*) FROM %s WHERE status = 'processing' AND claimed_at < $1", s.table) // #nosec G201 -- table name is set at construction, not user input
	var count int64
	err := s.db.QueryRowContext(ctx, query, cutoff).Scan(&count)
	return count, err
}

// setupMetricsCallbacks configures the metrics gauge callbacks for pending and stuck messages.
// This should be called after creating the scheduler if metrics are enabled.
func (s *PostgresScheduler) setupMetricsCallbacks(ctx context.Context) {
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

// Health performs a health check on the PostgreSQL scheduler.
//
// The health check:
//   - Pings PostgreSQL to verify connectivity
//   - Counts pending messages
//   - Counts stuck messages (claimed but not finalized beyond stuckDuration)
//
// Returns health.StatusHealthy if PostgreSQL is responsive and no stuck messages.
// Returns health.StatusDegraded if stuck messages exist.
// Returns health.StatusUnhealthy if PostgreSQL is not responsive.
func (s *PostgresScheduler) Health(ctx context.Context) *health.Result {
	start := time.Now()

	// Ping PostgreSQL
	if err := s.db.PingContext(ctx); err != nil {
		return &health.Result{
			Status:    health.StatusUnhealthy,
			Message:   fmt.Sprintf("postgres ping failed: %v", err),
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
			"table":            s.table,
		},
	}
}

// Compile-time checks
var (
	_ Scheduler     = (*PostgresScheduler)(nil)
	_ HealthChecker = (*PostgresScheduler)(nil)
)
