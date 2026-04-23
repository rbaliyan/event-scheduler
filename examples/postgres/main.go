// Package main demonstrates usage of the PostgreSQL-backed scheduler.
//
// Features shown:
//   - PostgreSQL connection with automatic schema creation
//   - Custom table name
//   - Retry with constant backoff
//   - Message scheduling, retrieval, listing with pagination, and cancellation
//   - Schema migration helper for existing tables
//   - Graceful shutdown via OS signal
package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	scheduler "github.com/rbaliyan/event-scheduler"
	"github.com/rbaliyan/event/v3/backoff"
	"github.com/rbaliyan/event/v3/transport/channel"
	_ "github.com/lib/pq"
)

// logDLQ is a dead letter queue that logs failed messages.
// In production, use github.com/rbaliyan/event-dlq instead.
type logDLQ struct{ logger *slog.Logger }

func (d *logDLQ) Store(_ context.Context, p scheduler.DLQStoreParams) error {
	d.logger.Error("message permanently failed, sent to DLQ",
		"event", p.EventName,
		"id", p.OriginalID,
		"retries", p.RetryCount,
		"error", p.Err,
	)
	return nil
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelDebug}))

	// --- Transport ---
	// channel.New() is used here for a self-contained example.
	// In production replace with a Redis, NATS, Kafka, or other transport.
	t := channel.New()

	// --- PostgreSQL connection ---
	dsn := "postgres://postgres:postgres@localhost:5432/myapp?sslmode=disable"
	if v := os.Getenv("POSTGRES_DSN"); v != "" {
		dsn = v
	}
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		logger.Error("failed to open database", "error", err)
		os.Exit(1)
	}
	defer func() { _ = db.Close() }()

	// --- Scheduler ---
	sched, err := scheduler.NewPostgresScheduler(db, t,
		// Store in a custom table (default: "scheduled_messages")
		scheduler.WithTable("order_scheduled_messages"),

		// Polling behaviour
		scheduler.WithPollInterval(time.Second),
		scheduler.WithAdaptivePolling(true),
		scheduler.WithMinPollInterval(200*time.Millisecond),
		scheduler.WithMaxPollInterval(30*time.Second),
		scheduler.WithBatchSize(100),

		// Retry: constant 10s backoff, up to 3 retries then DLQ
		scheduler.WithBackoff(&backoff.Constant{Delay: 10 * time.Second}),
		scheduler.WithMaxRetries(3),
		scheduler.WithDLQ(&logDLQ{logger: logger}),

		scheduler.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create scheduler", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Auto-create table and indexes ---
	// EnsureTable is idempotent; safe to call on every startup.
	if err := sched.EnsureTable(ctx); err != nil {
		logger.Error("failed to ensure table", "error", err)
		os.Exit(1)
	}

	// For existing tables created before retry support was added:
	// sched.MigrateAddRetryCount(ctx)

	// --- Start scheduler loop ---
	go func() {
		if err := sched.Start(ctx); err != nil {
			logger.Error("scheduler stopped with error", "error", err)
		}
	}()

	// --- Schedule messages ---
	type emailPayload struct {
		To      string `json:"to"`
		Subject string `json:"subject"`
	}

	var scheduledIDs []string
	subjects := []string{"Order confirmed", "Order shipped", "Order delivered"}
	for i, subject := range subjects {
		payload, _ := json.Marshal(emailPayload{
			To:      "customer@example.com",
			Subject: subject,
		})

		msg := scheduler.Message{
			EventName:   "emails.transactional",
			Payload:     payload,
			ScheduledAt: time.Now().Add(time.Duration(i+1) * 24 * time.Hour),
			Metadata:    map[string]string{"template": "order_update"},
		}

		if err := sched.Schedule(ctx, msg); err != nil {
			logger.Error("failed to schedule message", "error", err)
			continue
		}
		logger.Info("scheduled message", "id", msg.ID, "subject", subject)
		scheduledIDs = append(scheduledIDs, msg.ID)
	}

	// --- Retrieve a single message ---
	if len(scheduledIDs) > 0 {
		msg, err := sched.Get(ctx, scheduledIDs[0])
		if err != nil {
			logger.Error("failed to get message", "error", err)
		} else {
			logger.Info("retrieved message", "id", msg.ID, "event", msg.EventName)
		}
	}

	// --- List with pagination ---
	page1, err := sched.List(ctx, scheduler.Filter{
		EventName: "emails.transactional",
		Limit:     2,
		Offset:    0,
	})
	if err != nil {
		logger.Error("failed to list messages (page 1)", "error", err)
	} else {
		logger.Info("page 1", "count", len(page1))
	}

	page2, err := sched.List(ctx, scheduler.Filter{
		EventName: "emails.transactional",
		Limit:     2,
		Offset:    2,
	})
	if err != nil {
		logger.Error("failed to list messages (page 2)", "error", err)
	} else {
		logger.Info("page 2", "count", len(page2))
	}

	// --- Cancel the last scheduled message ---
	if len(scheduledIDs) > 0 {
		last := scheduledIDs[len(scheduledIDs)-1]
		if err := sched.Cancel(ctx, last); err != nil {
			logger.Error("failed to cancel", "id", last, "error", err)
		} else {
			logger.Info("cancelled message", "id", last)
		}
	}

	// --- Health check ---
	result := sched.Health(ctx)
	logger.Info("health check", "status", result.Status)

	// --- Wait for shutdown signal ---
	<-ctx.Done()
	logger.Info("shutting down scheduler")

	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := sched.Stop(stopCtx); err != nil {
		logger.Error("scheduler shutdown error", "error", err)
	}
}
