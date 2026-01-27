package scheduler

import (
	"context"
	"fmt"
	"sync"
	"testing"
	"time"
)

// --- Options tests ---

func TestDefaultOptions(t *testing.T) {
	o := defaultOptions()

	if o.pollInterval != 100*time.Millisecond {
		t.Errorf("expected pollInterval 100ms, got %v", o.pollInterval)
	}
	if o.batchSize != 100 {
		t.Errorf("expected batchSize 100, got %d", o.batchSize)
	}
	if o.keyPrefix != "scheduler:" {
		t.Errorf("expected keyPrefix 'scheduler:', got %q", o.keyPrefix)
	}
	if o.metrics != nil {
		t.Error("expected nil metrics by default")
	}
	if o.backoff != nil {
		t.Error("expected nil backoff by default")
	}
	if o.maxRetries != 0 {
		t.Error("expected 0 maxRetries by default")
	}
}

func TestWithPollInterval(t *testing.T) {
	o := defaultOptions()
	WithPollInterval(500 * time.Millisecond)(o)

	if o.pollInterval != 500*time.Millisecond {
		t.Errorf("expected pollInterval 500ms, got %v", o.pollInterval)
	}
}

func TestWithPollInterval_IgnoresZeroAndNegative(t *testing.T) {
	o := defaultOptions()
	original := o.pollInterval

	WithPollInterval(0)(o)
	if o.pollInterval != original {
		t.Errorf("expected pollInterval unchanged at %v, got %v", original, o.pollInterval)
	}

	WithPollInterval(-1 * time.Second)(o)
	if o.pollInterval != original {
		t.Errorf("expected pollInterval unchanged at %v, got %v", original, o.pollInterval)
	}
}

func TestWithBatchSize(t *testing.T) {
	o := defaultOptions()
	WithBatchSize(50)(o)

	if o.batchSize != 50 {
		t.Errorf("expected batchSize 50, got %d", o.batchSize)
	}
}

func TestWithBatchSize_IgnoresZeroAndNegative(t *testing.T) {
	o := defaultOptions()
	original := o.batchSize

	WithBatchSize(0)(o)
	if o.batchSize != original {
		t.Errorf("expected batchSize unchanged at %d, got %d", original, o.batchSize)
	}

	WithBatchSize(-5)(o)
	if o.batchSize != original {
		t.Errorf("expected batchSize unchanged at %d, got %d", original, o.batchSize)
	}
}

func TestWithKeyPrefix(t *testing.T) {
	o := defaultOptions()
	WithKeyPrefix("myapp:")(o)

	if o.keyPrefix != "myapp:" {
		t.Errorf("expected keyPrefix 'myapp:', got %q", o.keyPrefix)
	}
}

func TestWithKeyPrefix_IgnoresEmpty(t *testing.T) {
	o := defaultOptions()
	original := o.keyPrefix

	WithKeyPrefix("")(o)
	if o.keyPrefix != original {
		t.Errorf("expected keyPrefix unchanged at %q, got %q", original, o.keyPrefix)
	}
}

func TestWithBackoff(t *testing.T) {
	o := defaultOptions()
	b := &mockBackoff{delay: time.Second}
	WithBackoff(b)(o)

	if o.backoff != b {
		t.Error("expected backoff to be set")
	}
}

func TestWithMaxRetries(t *testing.T) {
	o := defaultOptions()
	WithMaxRetries(5)(o)

	if o.maxRetries != 5 {
		t.Errorf("expected maxRetries 5, got %d", o.maxRetries)
	}
}

func TestWithMaxRetries_AllowsZero(t *testing.T) {
	o := defaultOptions()
	WithMaxRetries(5)(o)
	WithMaxRetries(0)(o)

	if o.maxRetries != 0 {
		t.Errorf("expected maxRetries 0, got %d", o.maxRetries)
	}
}

func TestMultipleOptionsApplied(t *testing.T) {
	o := defaultOptions()
	for _, opt := range []Option{
		WithPollInterval(200 * time.Millisecond),
		WithBatchSize(25),
		WithKeyPrefix("test:"),
		WithMaxRetries(3),
	} {
		opt(o)
	}

	if o.pollInterval != 200*time.Millisecond {
		t.Errorf("expected pollInterval 200ms, got %v", o.pollInterval)
	}
	if o.batchSize != 25 {
		t.Errorf("expected batchSize 25, got %d", o.batchSize)
	}
	if o.keyPrefix != "test:" {
		t.Errorf("expected keyPrefix 'test:', got %q", o.keyPrefix)
	}
	if o.maxRetries != 3 {
		t.Errorf("expected maxRetries 3, got %d", o.maxRetries)
	}
}

// --- Message tests ---

func TestMessageCreation(t *testing.T) {
	now := time.Now()
	msg := Message{
		ID:          "test-id",
		EventName:   "orders.created",
		Payload:     []byte(`{"order_id":"123"}`),
		Metadata:    map[string]string{"key": "value"},
		ScheduledAt: now.Add(time.Hour),
		CreatedAt:   now,
	}

	if msg.ID != "test-id" {
		t.Errorf("expected ID 'test-id', got %q", msg.ID)
	}
	if msg.EventName != "orders.created" {
		t.Errorf("expected EventName 'orders.created', got %q", msg.EventName)
	}
	if string(msg.Payload) != `{"order_id":"123"}` {
		t.Errorf("unexpected payload: %s", msg.Payload)
	}
	if msg.Metadata["key"] != "value" {
		t.Errorf("expected metadata key 'key' to have value 'value'")
	}
	if msg.RetryCount != 0 {
		t.Errorf("expected RetryCount 0, got %d", msg.RetryCount)
	}
}

func TestMessageRetryCount(t *testing.T) {
	msg := Message{RetryCount: 3}
	if msg.RetryCount != 3 {
		t.Errorf("expected RetryCount 3, got %d", msg.RetryCount)
	}
}

// --- Filter tests ---

func TestFilterDefaults(t *testing.T) {
	f := Filter{}

	if f.EventName != "" {
		t.Errorf("expected empty EventName, got %q", f.EventName)
	}
	if !f.Before.IsZero() {
		t.Error("expected zero Before time")
	}
	if !f.After.IsZero() {
		t.Error("expected zero After time")
	}
	if f.Limit != 0 {
		t.Errorf("expected Limit 0, got %d", f.Limit)
	}
}

func TestFilterWithAllFields(t *testing.T) {
	now := time.Now()
	f := Filter{
		EventName: "test.event",
		Before:    now.Add(time.Hour),
		After:     now.Add(-time.Hour),
		Limit:     50,
	}

	if f.EventName != "test.event" {
		t.Errorf("expected EventName 'test.event', got %q", f.EventName)
	}
	if f.Limit != 50 {
		t.Errorf("expected Limit 50, got %d", f.Limit)
	}
	if f.Before.Before(now) {
		t.Error("expected Before to be in the future")
	}
	if f.After.After(now) {
		t.Error("expected After to be in the past")
	}
}

// --- BackoffStrategy tests ---

type mockBackoff struct {
	delay      time.Duration
	resetCount int
	mu         sync.Mutex
}

func (m *mockBackoff) NextDelay(attempt int) time.Duration {
	return m.delay * time.Duration(attempt+1)
}

func (m *mockBackoff) Reset() {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.resetCount++
}

func TestBackoffStrategyInterface(t *testing.T) {
	b := &mockBackoff{delay: 100 * time.Millisecond}

	// Attempt 0 (first retry)
	d := b.NextDelay(0)
	if d != 100*time.Millisecond {
		t.Errorf("expected 100ms for attempt 0, got %v", d)
	}

	// Attempt 1 (second retry)
	d = b.NextDelay(1)
	if d != 200*time.Millisecond {
		t.Errorf("expected 200ms for attempt 1, got %v", d)
	}

	// Reset
	b.Reset()
	if b.resetCount != 1 {
		t.Errorf("expected resetCount 1, got %d", b.resetCount)
	}
}

// --- Memory Scheduler for interface contract tests ---

// memoryScheduler is a simple in-memory implementation of Scheduler for testing.
type memoryScheduler struct {
	mu       sync.Mutex
	messages map[string]*Message
	stopCh   chan struct{}
	stopped  chan struct{}
}

func newMemoryScheduler() *memoryScheduler {
	return &memoryScheduler{
		messages: make(map[string]*Message),
		stopCh:   make(chan struct{}),
		stopped:  make(chan struct{}),
	}
}

func (m *memoryScheduler) Schedule(ctx context.Context, msg Message) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if msg.ID == "" {
		return fmt.Errorf("message ID is required")
	}
	if _, exists := m.messages[msg.ID]; exists {
		return fmt.Errorf("message already exists: %s", msg.ID)
	}
	if msg.CreatedAt.IsZero() {
		msg.CreatedAt = time.Now()
	}

	stored := msg // copy
	m.messages[msg.ID] = &stored
	return nil
}

func (m *memoryScheduler) Cancel(ctx context.Context, id string) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	if _, exists := m.messages[id]; !exists {
		return fmt.Errorf("message not found: %s", id)
	}
	delete(m.messages, id)
	return nil
}

func (m *memoryScheduler) Get(ctx context.Context, id string) (*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	msg, exists := m.messages[id]
	if !exists {
		return nil, fmt.Errorf("message not found: %s", id)
	}
	return msg, nil
}

func (m *memoryScheduler) List(ctx context.Context, filter Filter) ([]*Message, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	var result []*Message
	for _, msg := range m.messages {
		if filter.EventName != "" && msg.EventName != filter.EventName {
			continue
		}
		if !filter.After.IsZero() && msg.ScheduledAt.Before(filter.After) {
			continue
		}
		if !filter.Before.IsZero() && msg.ScheduledAt.After(filter.Before) {
			continue
		}
		result = append(result, msg)
		if filter.Limit > 0 && len(result) >= filter.Limit {
			break
		}
	}
	return result, nil
}

func (m *memoryScheduler) Start(ctx context.Context) error {
	select {
	case <-ctx.Done():
		close(m.stopped)
		return ctx.Err()
	case <-m.stopCh:
		close(m.stopped)
		return nil
	}
}

func (m *memoryScheduler) Stop(ctx context.Context) error {
	close(m.stopCh)
	select {
	case <-m.stopped:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

// Compile-time interface check
var _ Scheduler = (*memoryScheduler)(nil)

// --- Scheduler interface contract tests ---

func TestSchedulerContract_ScheduleAndGet(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	now := time.Now()
	msg := Message{
		ID:          "msg-1",
		EventName:   "test.event",
		Payload:     []byte("hello"),
		Metadata:    map[string]string{"key": "val"},
		ScheduledAt: now.Add(time.Hour),
		CreatedAt:   now,
	}

	if err := s.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	got, err := s.Get(ctx, "msg-1")
	if err != nil {
		t.Fatalf("Get() error: %v", err)
	}

	if got.ID != "msg-1" {
		t.Errorf("expected ID 'msg-1', got %q", got.ID)
	}
	if got.EventName != "test.event" {
		t.Errorf("expected EventName 'test.event', got %q", got.EventName)
	}
	if string(got.Payload) != "hello" {
		t.Errorf("expected payload 'hello', got %q", string(got.Payload))
	}
	if got.Metadata["key"] != "val" {
		t.Errorf("expected metadata key 'key' = 'val'")
	}
}

func TestSchedulerContract_Cancel(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	msg := Message{
		ID:          "cancel-me",
		EventName:   "test.event",
		Payload:     []byte("data"),
		ScheduledAt: time.Now().Add(time.Hour),
	}
	if err := s.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule() error: %v", err)
	}

	if err := s.Cancel(ctx, "cancel-me"); err != nil {
		t.Fatalf("Cancel() error: %v", err)
	}

	_, err := s.Get(ctx, "cancel-me")
	if err == nil {
		t.Error("expected error after cancel, got nil")
	}
}

func TestSchedulerContract_CancelNotFound(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	err := s.Cancel(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for cancelling nonexistent message")
	}
}

func TestSchedulerContract_GetNotFound(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	_, err := s.Get(ctx, "nonexistent")
	if err == nil {
		t.Error("expected error for getting nonexistent message")
	}
}

func TestSchedulerContract_DuplicateSchedule(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	msg := Message{
		ID:          "dup-id",
		EventName:   "test.event",
		Payload:     []byte("data"),
		ScheduledAt: time.Now().Add(time.Hour),
	}
	if err := s.Schedule(ctx, msg); err != nil {
		t.Fatalf("first Schedule() error: %v", err)
	}

	err := s.Schedule(ctx, msg)
	if err == nil {
		t.Error("expected error for duplicate schedule")
	}
}

func TestSchedulerContract_ListAll(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	now := time.Now()
	for i := 0; i < 5; i++ {
		msg := Message{
			ID:          fmt.Sprintf("msg-%d", i),
			EventName:   "test.event",
			Payload:     []byte("data"),
			ScheduledAt: now.Add(time.Duration(i) * time.Hour),
			CreatedAt:   now,
		}
		if err := s.Schedule(ctx, msg); err != nil {
			t.Fatalf("Schedule() error: %v", err)
		}
	}

	messages, err := s.List(ctx, Filter{})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(messages) != 5 {
		t.Errorf("expected 5 messages, got %d", len(messages))
	}
}

func TestSchedulerContract_ListWithEventFilter(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	now := time.Now()
	events := []string{"orders.created", "orders.shipped", "orders.created"}
	for i, ev := range events {
		msg := Message{
			ID:          fmt.Sprintf("msg-%d", i),
			EventName:   ev,
			Payload:     []byte("data"),
			ScheduledAt: now.Add(time.Hour),
			CreatedAt:   now,
		}
		if err := s.Schedule(ctx, msg); err != nil {
			t.Fatalf("Schedule() error: %v", err)
		}
	}

	messages, err := s.List(ctx, Filter{EventName: "orders.created"})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(messages) != 2 {
		t.Errorf("expected 2 messages for orders.created, got %d", len(messages))
	}
}

func TestSchedulerContract_ListWithLimit(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	now := time.Now()
	for i := 0; i < 10; i++ {
		msg := Message{
			ID:          fmt.Sprintf("msg-%d", i),
			EventName:   "test.event",
			Payload:     []byte("data"),
			ScheduledAt: now.Add(time.Duration(i) * time.Hour),
			CreatedAt:   now,
		}
		if err := s.Schedule(ctx, msg); err != nil {
			t.Fatalf("Schedule() error: %v", err)
		}
	}

	messages, err := s.List(ctx, Filter{Limit: 3})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(messages) > 3 {
		t.Errorf("expected at most 3 messages, got %d", len(messages))
	}
}

func TestSchedulerContract_ListWithTimeFilter(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	now := time.Now()
	// Schedule messages at 1h, 2h, 3h, 4h, 5h from now
	for i := 1; i <= 5; i++ {
		msg := Message{
			ID:          fmt.Sprintf("msg-%d", i),
			EventName:   "test.event",
			Payload:     []byte("data"),
			ScheduledAt: now.Add(time.Duration(i) * time.Hour),
			CreatedAt:   now,
		}
		if err := s.Schedule(ctx, msg); err != nil {
			t.Fatalf("Schedule() error: %v", err)
		}
	}

	// Filter: after 2h, before 4h (should get 3h)
	messages, err := s.List(ctx, Filter{
		After:  now.Add(2*time.Hour + time.Minute),
		Before: now.Add(4*time.Hour - time.Minute),
	})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(messages) != 1 {
		t.Errorf("expected 1 message in time range, got %d", len(messages))
	}
}

func TestSchedulerContract_ListEmpty(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	messages, err := s.List(ctx, Filter{EventName: "nonexistent"})
	if err != nil {
		t.Fatalf("List() error: %v", err)
	}
	if len(messages) != 0 {
		t.Errorf("expected 0 messages, got %d", len(messages))
	}
}

func TestSchedulerContract_StartStop(t *testing.T) {
	s := newMemoryScheduler()
	ctx := context.Background()

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Give Start a moment to begin
	time.Sleep(10 * time.Millisecond)

	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	if err := s.Stop(stopCtx); err != nil {
		t.Fatalf("Stop() error: %v", err)
	}

	if err := <-errCh; err != nil {
		t.Fatalf("Start() returned error: %v", err)
	}
}

func TestSchedulerContract_StartCancelledContext(t *testing.T) {
	s := newMemoryScheduler()
	ctx, cancel := context.WithCancel(context.Background())

	errCh := make(chan error, 1)
	go func() {
		errCh <- s.Start(ctx)
	}()

	// Give Start a moment to begin
	time.Sleep(10 * time.Millisecond)

	cancel()

	err := <-errCh
	if err != context.Canceled {
		t.Errorf("expected context.Canceled, got %v", err)
	}
}

// --- DLQ tests ---

// mockDLQ implements DeadLetterQueue for testing.
type mockDLQ struct {
	mu       sync.Mutex
	messages []dlqEntry
}

type dlqEntry struct {
	EventName  string
	OriginalID string
	Payload    []byte
	Metadata   map[string]string
	Err        error
	RetryCount int
	Source     string
}

func (m *mockDLQ) Store(ctx context.Context, eventName, originalID string, payload []byte, metadata map[string]string, err error, retryCount int, source string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.messages = append(m.messages, dlqEntry{
		EventName:  eventName,
		OriginalID: originalID,
		Payload:    payload,
		Metadata:   metadata,
		Err:        err,
		RetryCount: retryCount,
		Source:     source,
	})
	return nil
}

// Compile-time check that mockDLQ satisfies DeadLetterQueue
var _ DeadLetterQueue = (*mockDLQ)(nil)

func TestWithDLQ(t *testing.T) {
	o := defaultOptions()
	d := &mockDLQ{}
	WithDLQ(d)(o)
	if o.dlq == nil {
		t.Error("expected dlq to be set")
	}
}

func TestWithDLQ_Nil(t *testing.T) {
	o := defaultOptions()
	WithDLQ(nil)(o)
	if o.dlq != nil {
		t.Error("expected dlq to remain nil")
	}
}
