//go:build integration

package scheduler

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/redis/go-redis/v9"
)

func setupRedisScheduler(t *testing.T, tr *mockTransport, opts ...Option) (*RedisScheduler, func()) {
	t.Helper()

	client := redis.NewClient(&redis.Options{Addr: getRedisAddr()})
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := client.Ping(ctx).Err(); err != nil {
		t.Skipf("Redis not available: %v", err)
	}

	prefix := testPrefix()
	allOpts := append([]Option{WithKeyPrefix(prefix), WithPollInterval(50 * time.Millisecond)}, opts...)
	sched := NewRedisScheduler(client, tr, allOpts...)

	cleanup := func() {
		ctx := context.Background()
		iter := client.Scan(ctx, 0, prefix+"*", 100).Iterator()
		var keys []string
		for iter.Next(ctx) {
			keys = append(keys, iter.Val())
		}
		if len(keys) > 0 {
			client.Del(ctx, keys...)
		}
		client.Close()
	}

	return sched, cleanup
}

func TestRedis_Integration_ScheduleAndGet(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	now := time.Now()
	msg := makeMessage("redis-get-1", "test.event", now.Add(time.Hour))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	got, err := sched.Get(ctx, "redis-get-1")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}

	if got.ID != "redis-get-1" {
		t.Errorf("expected ID 'redis-get-1', got %q", got.ID)
	}
	if got.EventName != "test.event" {
		t.Errorf("expected EventName 'test.event', got %q", got.EventName)
	}
	if string(got.Payload) != `{"id":"redis-get-1"}` {
		t.Errorf("unexpected payload: %s", got.Payload)
	}
	if got.Metadata["test"] != "true" {
		t.Error("expected metadata test=true")
	}
}

func TestRedis_Integration_ScheduleAndCancel(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("redis-cancel-1", "test.event", time.Now().Add(time.Hour))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	if err := sched.Cancel(ctx, "redis-cancel-1"); err != nil {
		t.Fatalf("Cancel() error: %v", err)
	}

	_, err := sched.Get(ctx, "redis-cancel-1")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound after cancel, got %v", err)
	}
}

func TestRedis_Integration_ListWithFilters(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	now := time.Now()

	// Schedule messages with different events and times
	msgs := []Message{
		makeMessage("rf-1", "orders.created", now.Add(1*time.Hour)),
		makeMessage("rf-2", "orders.shipped", now.Add(2*time.Hour)),
		makeMessage("rf-3", "orders.created", now.Add(3*time.Hour)),
		makeMessage("rf-4", "payments.received", now.Add(4*time.Hour)),
		makeMessage("rf-5", "orders.created", now.Add(5*time.Hour)),
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

func TestRedis_Integration_CancelNonexistent(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	err := sched.Cancel(context.Background(), "nonexistent-redis")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRedis_Integration_GetNonexistent(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	_, err := sched.Get(context.Background(), "nonexistent-redis")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestRedis_Integration_ProcessDueMessage(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("redis-due-1", "test.delivery", time.Now().Add(-time.Second))

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
		_, err := sched.Get(ctx, "redis-due-1")
		return errors.Is(err, ErrNotFound)
	}, "message to be removed after delivery")
}

func TestRedis_Integration_BatchProcessing(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	for i := 0; i < 5; i++ {
		msg := makeMessage(fmt.Sprintf("redis-batch-%d", i), "batch.event", time.Now().Add(-time.Second))
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

func TestRedis_Integration_FutureMessageNotDelivered(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("redis-future-1", "future.event", time.Now().Add(time.Hour))

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

func TestRedis_Integration_RetryWithBackoff(t *testing.T) {
	mt := newMockTransport()
	mt.SetFailures(2, fmt.Errorf("temporary error"))

	backoff := &fixedBackoff{delay: 100 * time.Millisecond}
	sched, cleanup := setupRedisScheduler(t, mt, WithBackoff(backoff), WithMaxRetries(5))
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("redis-retry-1", "retry.event", time.Now().Add(-time.Second))

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

func TestRedis_Integration_MaxRetriesExceeded(t *testing.T) {
	mt := newMockTransport()
	mt.SetFailures(100, fmt.Errorf("permanent error")) // always fail

	backoff := &fixedBackoff{delay: 50 * time.Millisecond}
	sched, cleanup := setupRedisScheduler(t, mt, WithBackoff(backoff), WithMaxRetries(2))
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("redis-maxretry-1", "maxretry.event", time.Now().Add(-time.Second))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sched.Start(ctx)

	// Wait for message to be discarded (retries exhausted)
	waitFor(t, 10*time.Second, func() bool {
		count, _ := sched.CountPending(ctx)
		return count == 0
	}, "message to be discarded after max retries")

	// Should have zero successful deliveries
	pubs := mt.Published()
	if len(pubs) != 0 {
		t.Errorf("expected 0 deliveries, got %d", len(pubs))
	}
}

func TestRedis_Integration_DLQ(t *testing.T) {
	mt := newMockTransport()
	mt.SetFailures(100, fmt.Errorf("permanent error"))

	dlq := newMockDLQ()
	backoff := &fixedBackoff{delay: 50 * time.Millisecond}
	sched, cleanup := setupRedisScheduler(t, mt, WithBackoff(backoff), WithMaxRetries(2), WithDLQ(dlq))
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("redis-dlq-1", "dlq.event", time.Now().Add(-time.Second))

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
	if entries[0].OriginalID != "redis-dlq-1" {
		t.Errorf("expected DLQ ID 'redis-dlq-1', got %q", entries[0].OriginalID)
	}
	if entries[0].Source != "scheduler" {
		t.Errorf("expected DLQ source 'scheduler', got %q", entries[0].Source)
	}
}

func TestRedis_Integration_DLQStoreFailure(t *testing.T) {
	mt := newMockTransport()
	mt.SetFailures(100, fmt.Errorf("permanent error"))

	dlq := newMockDLQ()
	dlq.mu.Lock()
	dlq.failNext = true
	dlq.mu.Unlock()

	backoff := &fixedBackoff{delay: 50 * time.Millisecond}
	sched, cleanup := setupRedisScheduler(t, mt, WithBackoff(backoff), WithMaxRetries(2), WithDLQ(dlq))
	defer cleanup()

	ctx := context.Background()
	msg := makeMessage("redis-dlqfail-1", "dlqfail.event", time.Now().Add(-time.Second))

	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sched.Start(ctx)

	// Message should still be discarded even if DLQ fails
	waitFor(t, 10*time.Second, func() bool {
		count, _ := sched.CountPending(ctx)
		return count == 0
	}, "message to be discarded after DLQ failure")
}

func TestRedis_Integration_StuckRecovery(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()
	sched.WithStuckDuration(1 * time.Second)

	ctx := context.Background()

	// Manually insert into processing set with old timestamp
	msg := makeMessage("redis-stuck-1", "stuck.event", time.Now().Add(-time.Minute))
	data, err := json.Marshal(msg)
	if err != nil {
		t.Fatalf("marshal error: %v", err)
	}

	client := redis.NewClient(&redis.Options{Addr: getRedisAddr()})
	defer client.Close()

	err = client.ZAdd(ctx, sched.processingKey(), redis.Z{
		Score:  float64(time.Now().Add(-2 * time.Second).Unix()),
		Member: data,
	}).Err()
	if err != nil {
		t.Fatalf("ZAdd error: %v", err)
	}

	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go sched.Start(ctx)

	// Should recover and deliver
	pub := mt.WaitForPublish(t, 15*time.Second)
	if pub.EventName != "stuck.event" {
		t.Errorf("expected event 'stuck.event', got %q", pub.EventName)
	}
}

func TestRedis_Integration_ConcurrentScheduleCancel(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()
	var wg sync.WaitGroup

	// Schedule 20 messages
	for i := 0; i < 20; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			msg := makeMessage(fmt.Sprintf("redis-conc-%d", id), "conc.event", time.Now().Add(time.Hour))
			_ = sched.Schedule(ctx, msg)
		}(i)
	}

	wg.Wait()

	// Cancel 10 of them concurrently
	for i := 0; i < 10; i++ {
		wg.Add(1)
		go func(id int) {
			defer wg.Done()
			_ = sched.Cancel(ctx, fmt.Sprintf("redis-conc-%d", id))
		}(i)
	}

	wg.Wait()

	// Verify roughly 10 remain
	list, err := sched.List(ctx, Filter{})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(list) < 5 || len(list) > 15 {
		t.Errorf("expected ~10 remaining messages, got %d", len(list))
	}
}

func TestRedis_Integration_DoubleStop(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
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

	// Second Stop should not panic
	if err := sched.Stop(stopCtx); err != nil {
		t.Fatalf("second Stop() error: %v", err)
	}
}

func TestRedis_Integration_GracefulShutdown(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	ctx := context.Background()

	errCh := make(chan error, 1)
	go func() {
		errCh <- sched.Start(ctx)
	}()

	// Schedule a message while running
	msg := makeMessage("redis-shutdown-1", "shutdown.event", time.Now().Add(-time.Second))
	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	// Wait for delivery
	mt.WaitForPublish(t, 5*time.Second)

	// Stop gracefully
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	if err := sched.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
}
