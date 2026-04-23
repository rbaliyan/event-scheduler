// Package main demonstrates usage of the MongoDB-backed scheduler.
//
// Features shown:
//   - MongoDB connection and index creation
//   - Custom collection name
//   - Stuck message recovery configuration
//   - Retry with linear backoff
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
	"go.mongodb.org/mongo-driver/v2/mongo"
	mongoopts "go.mongodb.org/mongo-driver/v2/mongo/options"
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

	// --- MongoDB connection ---
	// mongo-driver v2: Connect accepts only ClientOptions (no context parameter).
	client, err := mongo.Connect(
		mongoopts.Client().ApplyURI("mongodb://localhost:27017"),
	)
	if err != nil {
		logger.Error("failed to connect to MongoDB", "error", err)
		os.Exit(1)
	}
	db := client.Database("myapp")

	// --- Scheduler ---
	sched, err := scheduler.NewMongoScheduler(db, t,
		// Store in a custom collection (default: "scheduled_messages")
		scheduler.WithCollection("order_reminders"),

		// Polling behaviour
		scheduler.WithPollInterval(500*time.Millisecond),
		scheduler.WithAdaptivePolling(true),
		scheduler.WithMinPollInterval(100*time.Millisecond),
		scheduler.WithMaxPollInterval(30*time.Second),
		scheduler.WithBatchSize(25),

		// Recovery: reclaim messages stuck in "processing" after a crash
		scheduler.WithStuckDuration(10*time.Minute),

		// Retry: linear backoff, up to 5 retries before DLQ
		scheduler.WithBackoff(&backoff.Linear{
			Initial: 2 * time.Second,
			Step:    5 * time.Second,
			Max:     2 * time.Minute,
		}),
		scheduler.WithMaxRetries(5),
		scheduler.WithDLQ(&logDLQ{logger: logger}),

		scheduler.WithLogger(logger),
	)
	if err != nil {
		logger.Error("failed to create scheduler", "error", err)
		os.Exit(1)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// --- Create indexes for optimal performance ---
	// EnsureIndexes is idempotent; safe to call on every startup.
	if err := sched.EnsureIndexes(ctx); err != nil {
		logger.Error("failed to ensure indexes", "error", err)
		os.Exit(1)
	}

	// --- Start scheduler loop ---
	go func() {
		if err := sched.Start(ctx); err != nil {
			logger.Error("scheduler stopped with error", "error", err)
		}
	}()

	// --- Schedule messages ---
	type notificationPayload struct {
		UserID  string `json:"user_id"`
		Message string `json:"message"`
	}

	var scheduledIDs []string
	for i := range 3 {
		payload, _ := json.Marshal(notificationPayload{
			UserID:  "user-" + string(rune('A'+i)),
			Message: "Your order is ready for pickup",
		})

		msg := scheduler.Message{
			EventName:   "notifications.push",
			Payload:     payload,
			ScheduledAt: time.Now().Add(time.Duration(i+1) * time.Hour),
		}

		if err := sched.Schedule(ctx, msg); err != nil {
			logger.Error("failed to schedule message", "error", err)
			continue
		}
		logger.Info("scheduled message", "id", msg.ID)
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

	// --- List messages scheduled in the next 2 hours ---
	pending, err := sched.List(ctx, scheduler.Filter{
		EventName: "notifications.push",
		Before:    time.Now().Add(2 * time.Hour),
		Limit:     20,
	})
	if err != nil {
		logger.Error("failed to list messages", "error", err)
	} else {
		logger.Info("messages due within 2 hours", "count", len(pending))
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

	if err := client.Disconnect(context.Background()); err != nil {
		logger.Error("failed to disconnect MongoDB", "error", err)
	}
}
