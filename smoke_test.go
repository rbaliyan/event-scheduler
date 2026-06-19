package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/health"
	"go.mongodb.org/mongo-driver/v2/mongo"
)

// Smoke tests: fast, hermetic happy-path checks needing no external infra.
// They guard the "does the basic wiring work" surface for every backend that
// can be exercised without Docker.

func TestSmoke_ConstructorGuards(t *testing.T) {
	t.Parallel()
	ft := newFakeTransport()

	if _, err := NewRedisScheduler(nil, ft); err == nil {
		t.Error("NewRedisScheduler(nil client) should error")
	}
	if _, err := NewPostgresScheduler(nil, ft); err == nil {
		t.Error("NewPostgresScheduler(nil db) should error")
	}
	if _, err := NewMongoScheduler(nil, ft); err == nil {
		t.Error("NewMongoScheduler(nil db) should error")
	}
	// nil transport is rejected by every backend.
	if _, err := NewMongoScheduler(&mongo.Database{}, nil); err == nil {
		t.Error("NewMongoScheduler(nil transport) should error")
	}
}

func TestSmoke_MongoCollectionNameValidation(t *testing.T) {
	t.Parallel()
	// Construction validates the collection identifier before any server I/O.
	_, err := NewMongoScheduler(&mongo.Database{}, newFakeTransport(), WithCollection("bad name!"))
	if err == nil {
		t.Error("expected error for invalid collection name")
	}
}

func TestSmoke_RedisStartStopRoundTrip(t *testing.T) {
	t.Parallel()
	sched, ft, cleanup := newMiniRedisScheduler(t, WithPollInterval(5*time.Millisecond))
	defer cleanup()
	ctx := context.Background()

	if err := sched.Schedule(ctx, Message{
		ID: "s1", EventName: "e", Payload: []byte("p"), ScheduledAt: time.Now().Add(-time.Second),
	}); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	go func() { _ = sched.Start(ctx) }()

	// Wait for delivery via the real polling loop (no fixed sleep).
	deadline := time.After(2 * time.Second)
	for ft.count() == 0 {
		select {
		case <-deadline:
			t.Fatal("message not delivered by the polling loop within 2s")
		case <-time.After(2 * time.Millisecond):
		}
	}

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := sched.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
}

func TestSmoke_RedisHealth(t *testing.T) {
	t.Parallel()
	sched, _, cleanup := newMiniRedisScheduler(t)
	defer cleanup()
	ctx := context.Background()

	if err := sched.Schedule(ctx, Message{
		ID: "h1", EventName: "e", Payload: []byte("p"), ScheduledAt: time.Now().Add(time.Hour),
	}); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	res := sched.Health(ctx)
	if res.Status != health.StatusHealthy {
		t.Errorf("expected healthy, got %v (%s)", res.Status, res.Message)
	}
	if res.Details["pending_messages"].(int64) != 1 {
		t.Errorf("pending_messages = %v, want 1", res.Details["pending_messages"])
	}
}

func TestSmoke_RedisCronRecurrence(t *testing.T) {
	t.Parallel()
	sched, ft, cleanup := newMiniRedisScheduler(t)
	defer cleanup()
	ctx := context.Background()

	if err := sched.Schedule(ctx, Message{
		ID: "cron1", EventName: "e", Payload: []byte("p"),
		ScheduledAt: time.Now().Add(-time.Second),
		Recurrence:  &Recurrence{Type: RecurrenceCron, Cron: "* * * * *"},
	}); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	sched.processDue(ctx)
	if ft.count() != 1 {
		t.Fatalf("expected 1 delivery, got %d", ft.count())
	}
	got, err := sched.Get(ctx, "cron1")
	if err != nil {
		t.Fatalf("cron message should still exist (recurring), got %v", err)
	}
	if got.OccurrenceCount != 1 {
		t.Errorf("OccurrenceCount = %d, want 1", got.OccurrenceCount)
	}
	if !got.ScheduledAt.After(time.Now()) {
		t.Errorf("next cron fire should be in the future, got %v", got.ScheduledAt)
	}
}
