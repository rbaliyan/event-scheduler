package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/redis/go-redis/v9"
)

// fakeTransport is an in-memory transport.Transport for deterministic
// (non-Docker) lifecycle tests. It can be told to fail the next N publishes.
type fakeTransport struct {
	mu        sync.Mutex
	published []string // event names of successfully published messages
	failCount int
	failErr   error
}

func newFakeTransport() *fakeTransport { return &fakeTransport{} }

func (f *fakeTransport) failNext(n int, err error) {
	f.mu.Lock()
	defer f.mu.Unlock()
	f.failCount = n
	f.failErr = err
}

func (f *fakeTransport) count() int {
	f.mu.Lock()
	defer f.mu.Unlock()
	return len(f.published)
}

func (f *fakeTransport) Publish(ctx context.Context, name string, msg transport.Message) error {
	f.mu.Lock()
	defer f.mu.Unlock()
	if f.failCount > 0 {
		f.failCount--
		return f.failErr
	}
	f.published = append(f.published, name)
	return nil
}

func (f *fakeTransport) RegisterEvent(ctx context.Context, name string) error   { return nil }
func (f *fakeTransport) UnregisterEvent(ctx context.Context, name string) error { return nil }
func (f *fakeTransport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	return nil, errors.New("not implemented")
}
func (f *fakeTransport) Close(ctx context.Context) error { return nil }

var _ transport.Transport = (*fakeTransport)(nil)

// newMiniRedisScheduler spins up an in-memory Redis and returns a scheduler
// wired to it, plus the transport and a cleanup func.
func newMiniRedisScheduler(t *testing.T, opts ...Option) (*RedisScheduler, *fakeTransport, func()) {
	t.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		t.Fatalf("miniredis.Run: %v", err)
	}
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	ft := newFakeTransport()
	sched, err := NewRedisScheduler(client, ft, opts...)
	if err != nil {
		t.Fatalf("NewRedisScheduler: %v", err)
	}
	cleanup := func() {
		_ = client.Close()
		mr.Close()
	}
	return sched, ft, cleanup
}

func TestRedisLifecycle_DeliversAndDeletes(t *testing.T) {
	sched, ft, cleanup := newMiniRedisScheduler(t)
	defer cleanup()
	ctx := context.Background()

	msg := Message{ID: "m1", EventName: "due.event", Payload: []byte("p"), ScheduledAt: time.Now().Add(-time.Second)}
	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	if n := sched.processDue(ctx); n != 1 {
		t.Fatalf("processDue returned %d, want 1", n)
	}
	if ft.count() != 1 {
		t.Errorf("expected 1 published message, got %d", ft.count())
	}
	// Terminal one-shot delivery must remove the message from storage.
	if _, err := sched.Get(ctx, "m1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected message deleted after delivery, got err=%v", err)
	}
}

func TestRedisLifecycle_NotYetDueIsSkipped(t *testing.T) {
	sched, ft, cleanup := newMiniRedisScheduler(t)
	defer cleanup()
	ctx := context.Background()

	msg := Message{ID: "future", EventName: "future.event", Payload: []byte("p"), ScheduledAt: time.Now().Add(time.Hour)}
	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	if n := sched.processDue(ctx); n != 0 {
		t.Errorf("processDue returned %d, want 0 for a future message", n)
	}
	if ft.count() != 0 {
		t.Errorf("expected 0 published, got %d", ft.count())
	}
	if _, err := sched.Get(ctx, "future"); err != nil {
		t.Errorf("future message should remain scheduled, got err=%v", err)
	}
}

func TestRedisLifecycle_RetryOnPublishFailure(t *testing.T) {
	// A backoff pushes the retry into the future so it is not re-claimed within
	// the same processDue cycle, letting us observe the rescheduled state.
	backoff := &mockBackoff{delay: time.Hour}
	sched, ft, cleanup := newMiniRedisScheduler(t, WithMaxRetries(5), WithBackoff(backoff))
	defer cleanup()
	ctx := context.Background()
	ft.failNext(1, errors.New("transport down"))

	msg := Message{ID: "r1", EventName: "retry.event", Payload: []byte("p"), ScheduledAt: time.Now().Add(-time.Second)}
	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// Publish fails -> message rescheduled with retry_count=1, still present,
	// and pushed ~1h into the future by the backoff.
	sched.processDue(ctx)
	if ft.count() != 0 {
		t.Errorf("expected 0 successful publishes, got %d", ft.count())
	}
	got, err := sched.Get(ctx, "r1")
	if err != nil {
		t.Fatalf("expected message to survive a failed publish, got err=%v", err)
	}
	if got.RetryCount != 1 {
		t.Errorf("expected RetryCount 1 after one failure, got %d", got.RetryCount)
	}
	if !got.ScheduledAt.After(time.Now().Add(30 * time.Minute)) {
		t.Errorf("expected retry pushed into the future by backoff, got %v", got.ScheduledAt)
	}
}

func TestRedisLifecycle_DLQOnMaxRetries(t *testing.T) {
	dlq := &mockDLQ{}
	sched, ft, cleanup := newMiniRedisScheduler(t, WithMaxRetries(2), WithDLQ(dlq))
	defer cleanup()
	ctx := context.Background()
	ft.failNext(100, errors.New("permanent failure"))

	msg := Message{ID: "d1", EventName: "dlq.event", Payload: []byte("p"), ScheduledAt: time.Now().Add(-time.Second)}
	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// Drive cycles until the message is gone. maxRetries=2 => attempts at
	// RetryCount 0,1 retry, RetryCount 2 hits the limit -> DLQ + discard.
	for range 5 {
		sched.processDue(ctx)
		if _, err := sched.Get(ctx, "d1"); errors.Is(err, ErrNotFound) {
			break
		}
	}

	if _, err := sched.Get(ctx, "d1"); !errors.Is(err, ErrNotFound) {
		t.Fatalf("expected message discarded after max retries, still present")
	}
	if len(dlq.messages) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(dlq.messages))
	}
	if dlq.messages[0].OriginalID != "d1" {
		t.Errorf("expected DLQ OriginalID 'd1', got %q", dlq.messages[0].OriginalID)
	}
	if dlq.messages[0].RetryCount != 2 {
		t.Errorf("expected DLQ RetryCount 2 (== maxRetries), got %d", dlq.messages[0].RetryCount)
	}
	if ft.count() != 0 {
		t.Errorf("expected 0 successful deliveries for a permanently failing transport, got %d", ft.count())
	}
}

func TestRedisLifecycle_RecurringReschedules(t *testing.T) {
	sched, ft, cleanup := newMiniRedisScheduler(t)
	defer cleanup()
	ctx := context.Background()

	msg := Message{
		ID:          "rec1",
		EventName:   "recurring.event",
		Payload:     []byte("p"),
		ScheduledAt: time.Now().Add(-time.Second),
		Recurrence:  &Recurrence{Type: RecurrenceInterval, Interval: time.Hour, MaxOccurrences: 3},
	}
	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// First delivery: published, NOT deleted, occurrence_count incremented, next fire in the future.
	sched.processDue(ctx)
	if ft.count() != 1 {
		t.Fatalf("expected 1 delivery, got %d", ft.count())
	}
	got, err := sched.Get(ctx, "rec1")
	if err != nil {
		t.Fatalf("recurring message should still exist after delivery, got err=%v", err)
	}
	if got.OccurrenceCount != 1 {
		t.Errorf("expected OccurrenceCount 1, got %d", got.OccurrenceCount)
	}
	if !got.ScheduledAt.After(time.Now()) {
		t.Errorf("expected next fire time in the future, got %v", got.ScheduledAt)
	}
	if got.RetryCount != 0 {
		t.Errorf("expected RetryCount reset to 0 on reschedule, got %d", got.RetryCount)
	}
}

func TestRedisLifecycle_RecoverStuck(t *testing.T) {
	sched, _, cleanup := newMiniRedisScheduler(t, WithStuckDuration(time.Millisecond))
	defer cleanup()
	ctx := context.Background()

	msg := Message{ID: "stuck1", EventName: "stuck.event", Payload: []byte("p"), ScheduledAt: time.Now().Add(-time.Second)}
	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// Claim the message (moves it to the processing set) but never finalize it,
	// simulating a crash between claim and delete.
	if _, _, err := sched.claimDueMessage(ctx, time.Now().Unix()); err != nil {
		t.Fatalf("claimDueMessage: %v", err)
	}

	// Before recovery, it should be counted as stuck (claimed beyond stuckDuration).
	time.Sleep(10 * time.Millisecond)
	stuck, err := sched.countStuck(ctx)
	if err != nil {
		t.Fatalf("countStuck: %v", err)
	}
	if stuck != 1 {
		t.Fatalf("expected 1 stuck message, got %d", stuck)
	}

	sched.recoverStuck(ctx)

	// After recovery it is pending again and processable.
	if n := sched.processDue(ctx); n != 1 {
		t.Errorf("expected recovered message to be processed, processDue returned %d", n)
	}
}
