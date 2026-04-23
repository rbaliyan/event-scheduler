// Package main demonstrates advanced usage of the Redis-backed scheduler.
//
// Features shown:
//   - Adaptive polling (speeds up under load, slows down when idle)
//   - Exponential backoff with max retries
//   - Dead letter queue for permanently failed messages
//   - OpenTelemetry metrics wiring
//   - Health checking
//   - Message scheduling, retrieval, listing, and cancellation
//   - Graceful shutdown via OS signal
package main

import (
	"context"
	"encoding/json"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	scheduler "github.com/rbaliyan/event-scheduler"
	"github.com/rbaliyan/event/v3/backoff"
	"github.com/rbaliyan/event/v3/transport/channel"
	"github.com/redis/go-redis/v9"
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
	// In production replace with a Redis, NATS, Kafka, or other transport
	// that routes delivered messages to your event handlers.
	t := channel.New()

	// --- Redis client ---
	rdb := redis.NewClient(&redis.Options{
		Addr: "localhost:6379",
	})

	// --- Metrics (optional) ---
	metrics, err := scheduler.NewMetrics(
		scheduler.WithNamespace("myapp_orders"),
	)
	if err != nil {
		logger.Error("failed to create metrics", "error", err)
		os.Exit(1)
	}
	defer func() { _ = metrics.Close() }()

	// --- Scheduler ---
	sched, err := scheduler.NewRedisScheduler(rdb, t,
		// Polling behaviour
		scheduler.WithPollInterval(500*time.Millisecond),
		scheduler.WithAdaptivePolling(true),
		scheduler.WithMinPollInterval(100*time.Millisecond),
		scheduler.WithMaxPollInterval(30*time.Second),
		scheduler.WithBatchSize(50),

		// Key namespace to avoid collisions with other services
		scheduler.WithKeyPrefix("orders"),

		// Recovery: reclaim messages stuck in "processing" after a crash
		scheduler.WithStuckDuration(5*time.Minute),

		// Retry: exponential backoff, up to 3 retries before DLQ
		scheduler.WithBackoff(&backoff.Exponential{
			Initial:    time.Second,
			Multiplier: 2.0,
			Max:        5 * time.Minute,
		}),
		scheduler.WithMaxRetries(3),
		scheduler.WithDLQ(&logDLQ{logger: logger}),

		// Observability
		scheduler.WithMetrics(metrics),
		scheduler.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create scheduler", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Start scheduler loop ---
	go func() {
		if err := sched.Start(ctx); err != nil {
			logger.Error("scheduler stopped with error", "error", err)
		}
	}()

	// --- Schedule messages ---
	type orderPayload struct {
		OrderID    string `json:"order_id"`
		CustomerID string `json:"customer_id"`
	}

	var scheduledIDs []string
	for i := range 5 {
		payload, _ := json.Marshal(orderPayload{
			OrderID:    "ORD-" + string(rune('A'+i)),
			CustomerID: "CUST-001",
		})

		msg := scheduler.Message{
			EventName:   "orders.reminder",
			Payload:     payload,
			ScheduledAt: time.Now().Add(time.Duration(i+1) * time.Minute),
			Metadata:    map[string]string{"priority": "high"},
		}

		if err := sched.Schedule(ctx, msg); err != nil {
			logger.Error("failed to schedule message", "error", err)
			continue
		}
		logger.Info("scheduled message", "id", msg.ID, "event", msg.EventName)
		scheduledIDs = append(scheduledIDs, msg.ID)
	}

	// --- Retrieve a single message ---
	if len(scheduledIDs) > 0 {
		msg, err := sched.Get(ctx, scheduledIDs[0])
		if err != nil {
			logger.Error("failed to get message", "error", err)
		} else {
			logger.Info("retrieved message", "id", msg.ID, "scheduled_at", msg.ScheduledAt)
		}
	}

	// --- List pending messages ---
	pending, err := sched.List(ctx, scheduler.Filter{
		EventName: "orders.reminder",
		After:     time.Now(),
		Limit:     10,
	})
	if err != nil {
		logger.Error("failed to list messages", "error", err)
	} else {
		logger.Info("pending messages", "count", len(pending))
	}

	// --- Cancel the last scheduled message ---
	if len(scheduledIDs) > 0 {
		last := scheduledIDs[len(scheduledIDs)-1]
		if err := sched.Cancel(ctx, last); err != nil {
			logger.Error("failed to cancel message", "id", last, "error", err)
		} else {
			logger.Info("cancelled message", "id", last)
		}
	}

	// --- Health check ---
	result := sched.Health(ctx)
	logger.Info("health check", "status", result.Status, "details", result.Details)

	// --- Wait for shutdown signal ---
	<-ctx.Done()
	logger.Info("shutting down scheduler")

	stopCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := sched.Stop(stopCtx); err != nil {
		logger.Error("scheduler shutdown error", "error", err)
	}
}
