//go:build integration

package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
)

func setupPostgresScheduler(t *testing.T, tr *mockTransport, opts ...Option) (*PostgresScheduler, func()) {
	t.Helper()

	db, err := sql.Open("postgres", getPostgresDSN())
	if err != nil {
		t.Skipf("PostgreSQL not available: %v", err)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := db.PingContext(ctx); err != nil {
		db.Close()
		t.Skipf("PostgreSQL not reachable: %v", err)
	}

	tableName := fmt.Sprintf("sched_test_%d", time.Now().UnixNano())
	allOpts := append([]Option{WithPollInterval(50 * time.Millisecond), WithTable(tableName)}, opts...)
	sched, err := NewPostgresScheduler(db, tr, allOpts...)
	if err != nil {
		db.Close()
		t.Fatalf("failed to create scheduler: %v", err)
	}

	if err := sched.EnsureTable(context.Background()); err != nil {
		db.Close()
		t.Fatalf("failed to create table: %v", err)
	}

	cleanup := func() {
		ctx := context.Background()
		_, _ = db.ExecContext(ctx, "DROP TABLE IF EXISTS "+tableName)
		db.Close()
	}

	return sched, cleanup
}

func TestPostgres_Integration_ScheduleAndGet(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("pg-get-1", "test.event", time.Now().Add(time.Hour))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	got, err := sched.Get(ctx, "pg-get-1")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}

	if got.ID != "pg-get-1" {
		t.Errorf("expected ID 'pg-get-1', got %q", got.ID)
	}
	if got.EventName != "test.event" {
		t.Errorf("expected EventName 'test.event', got %q", got.EventName)
	}
	if string(got.Payload) != `{"id":"pg-get-1"}` {
		t.Errorf("unexpected payload: %s", got.Payload)
	}
	if got.Metadata["test"] != "true" {
		t.Error("expected metadata test=true")
	}
}

func TestPostgres_Integration_ScheduleAndCancel(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("pg-cancel-1", "test.event", time.Now().Add(time.Hour))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	if err := sched.Cancel(ctx, "pg-cancel-1"); err != nil {
		t.Fatalf("Cancel() error: %v", err)
	}

	_, err := sched.Get(ctx, "pg-cancel-1")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after cancel, got %v", err)
	}
}

func TestPostgres_Integration_ListWithFilters(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	now := time.Now()

	msgs := []Message{
		makeMessage("pf-1", "orders.created", now.Add(1*time.Hour)),
		makeMessage("pf-2", "orders.shipped", now.Add(2*time.Hour)),
		makeMessage("pf-3", "orders.created", now.Add(3*time.Hour)),
		makeMessage("pf-4", "payments.received", now.Add(4*time.Hour)),
		makeMessage("pf-5", "orders.created", now.Add(5*time.Hour)),
	}
	for _, m := range msgs {
		if err := sched.Schedule(ctx, m); err != nil {
			t.Fatalf("Schedule() error: %v", err)
		}
	}

	// Filter by event name
	list, err := sched.List(ctx, Filter{EventName: "orders.created"})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(list) != 3 {
		t.Errorf("expected 3 orders.created, got %d", len(list))
	}

	// Filter by time range
	list, err = sched.List(ctx, Filter{
		After:  now.Add(90 * time.Minute),
		Before: now.Add(4*time.Hour + 30*time.Minute),
	})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(list) != 3 {
		t.Errorf("expected 3 messages in time range, got %d", len(list))
	}

	// Filter with limit
	list, err = sched.List(ctx, Filter{Limit: 2})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(list) != 2 {
		t.Errorf("expected 2 messages with limit, got %d", len(list))
	}
}

func TestPostgres_Integration_CancelNonexistent(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	err := sched.Cancel(context.Background(), "nonexistent-pg")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestPostgres_Integration_GetNonexistent(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	_, err := sched.Get(context.Background(), "nonexistent-pg")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestPostgres_Integration_ProcessDueMessage(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("pg-due-1", "test.delivery", time.Now().Add(-time.Second))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sched.Start(ctx)

	pub := mt.WaitForPublish(t, 5*time.Second)
	if pub.EventName != "test.delivery" {
		t.Errorf("expected event 'test.delivery', got %q", pub.EventName)
	}

	// Verify message removed
	waitFor(t, 2*time.Second, func() bool {
		_, err := sched.Get(context.Background(), "pg-due-1")
		return errors.Is(err, ErrNotFound)
	}, "message to be removed after delivery")
}

func TestPostgres_Integration_BatchProcessing(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		msg := makeMessage(fmt.Sprintf("pg-batch-%d", i), "batch.event", time.Now().Add(-time.Second))
		if err := sched.Schedule(ctx, msg); err != nil {
			t.Fatalf("Schedule() error: %v", err)
		}
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sched.Start(ctx)

	pubs := mt.WaitForPublishes(t, 5, 10*time.Second)
	if len(pubs) != 5 {
		t.Errorf("expected 5 deliveries, got %d", len(pubs))
	}
}

func TestPostgres_Integration_FutureMessageNotDelivered(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("pg-future-1", "future.event", time.Now().Add(time.Hour))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sched.Start(ctx)

	time.Sleep(500 * time.Millisecond)

	pubs := mt.Published()
	if len(pubs) != 0 {
		t.Errorf("expected 0 deliveries for future message, got %d", len(pubs))
	}
}

func TestPostgres_Integration_RetryWithBackoff(t *testing.T) {
	mt := newMockTransport()
	mt.SetFailures(2, fmt.Errorf("temporary error"))

	backoff := &fixedBackoff{delay: 100 * time.Millisecond}
	sched, cleanup := setupPostgresScheduler(t, mt, WithBackoff(backoff), WithMaxRetries(5))
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("pg-retry-1", "retry.event", time.Now().Add(-time.Second))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sched.Start(ctx)

	pub := mt.WaitForPublish(t, 10*time.Second)
	if pub.EventName != "retry.event" {
		t.Errorf("expected event 'retry.event', got %q", pub.EventName)
	}
}

func TestPostgres_Integration_MaxRetriesExceeded(t *testing.T) {
	mt := newMockTransport()
	mt.SetFailures(100, fmt.Errorf("permanent error"))

	backoff := &fixedBackoff{delay: 50 * time.Millisecond}
	sched, cleanup := setupPostgresScheduler(t, mt, WithBackoff(backoff), WithMaxRetries(2))
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("pg-maxretry-1", "maxretry.event", time.Now().Add(-time.Second))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sched.Start(ctx)

	waitFor(t, 10*time.Second, func() bool {
		count, _ := sched.CountPending(context.Background())
		return count == 0
	}, "message to be discarded after max retries")

	pubs := mt.Published()
	if len(pubs) != 0 {
		t.Errorf("expected 0 deliveries, got %d", len(pubs))
	}
}

func TestPostgres_Integration_DLQ(t *testing.T) {
	mt := newMockTransport()
	mt.SetFailures(100, fmt.Errorf("permanent error"))

	dlq := newMockDLQ()
	backoff := &fixedBackoff{delay: 50 * time.Millisecond}
	sched, cleanup := setupPostgresScheduler(t, mt, WithBackoff(backoff), WithMaxRetries(2), WithDLQ(dlq))
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("pg-dlq-1", "dlq.event", time.Now().Add(-time.Second))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sched.Start(ctx)

	dlq.WaitForStore(t, 10*time.Second)

	entries := dlq.Entries()
	if len(entries) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(entries))
	}
	if entries[0].EventName != "dlq.event" {
		t.Errorf("expected DLQ event 'dlq.event', got %q", entries[0].EventName)
	}
	if entries[0].OriginalID != "pg-dlq-1" {
		t.Errorf("expected DLQ ID 'pg-dlq-1', got %q", entries[0].OriginalID)
	}
	if entries[0].Source != "scheduler" {
		t.Errorf("expected DLQ source 'scheduler', got %q", entries[0].Source)
	}
}

func TestPostgres_Integration_DLQStoreFailure(t *testing.T) {
	mt := newMockTransport()
	mt.SetFailures(100, fmt.Errorf("permanent error"))

	dlq := newMockDLQ()
	dlq.mu.Lock()
	dlq.failNext = true
	dlq.mu.Unlock()

	backoff := &fixedBackoff{delay: 50 * time.Millisecond}
	sched, cleanup := setupPostgresScheduler(t, mt, WithBackoff(backoff), WithMaxRetries(2), WithDLQ(dlq))
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("pg-dlqfail-1", "dlqfail.event", time.Now().Add(-time.Second))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sched.Start(ctx)

	waitFor(t, 10*time.Second, func() bool {
		count, _ := sched.CountPending(context.Background())
		return count == 0
	}, "message to be discarded after DLQ failure")
}

func TestPostgres_Integration_ConcurrentScheduleCancel(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	var wg sync.WaitGroup

	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			msg := makeMessage(fmt.Sprintf("pg-conc-%d", id), "conc.event", time.Now().Add(time.Hour))
			_ = sched.Schedule(ctx, msg)
		}(i)
	}
	wg.Wait()

	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = sched.Cancel(ctx, fmt.Sprintf("pg-conc-%d", id))
		}(i)
	}
	wg.Wait()

	list, err := sched.List(ctx, Filter{})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(list) != 10 {
		t.Errorf("expected 10 remaining messages, got %d", len(list))
	}
}

func TestPostgres_Integration_DoubleStop(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Start(ctx)
	}()
	time.Sleep(50 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sched.Stop(stopCtx); err != nil {
		t.Fatalf("first Stop() error: %v", err)
	}
	<-errCh

	if err := sched.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop() error: %v", err)
	}
}

func TestPostgres_Integration_GracefulShutdown(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Start(ctx)
	}()

	msg := makeMessage("pg-shutdown-1", "shutdown.event", time.Now().Add(-time.Second))
	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	mt.WaitForPublish(t, 5*time.Second)

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sched.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
}
