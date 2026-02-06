package scheduler

import (
	"context"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// mockBenchTransport is a minimal transport for benchmarks
type mockBenchTransport struct{}

func (m *mockBenchTransport) Publish(ctx context.Context, name string, msg transport.Message) error {
	return nil
}

func (m *mockBenchTransport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	return nil, nil
}

func (m *mockBenchTransport) RegisterEvent(ctx context.Context, name string) error {
	return nil
}

func (m *mockBenchTransport) UnregisterEvent(ctx context.Context, name string) error {
	return nil
}

func (m *mockBenchTransport) Close(ctx context.Context) error {
	return nil
}

// mockBenchScheduler wraps a mock store for benchmarking
type mockBenchStore struct {
	messages map[string]*Message
}

func newMockBenchStore() *mockBenchStore {
	return &mockBenchStore{
		messages: make(map[string]*Message),
	}
}

func (s *mockBenchStore) Schedule(ctx context.Context, msg Message) error {
	s.messages[msg.ID] = &msg
	return nil
}

func (s *mockBenchStore) Cancel(ctx context.Context, id string) error {
	delete(s.messages, id)
	return nil
}

func (s *mockBenchStore) Get(ctx context.Context, id string) (*Message, error) {
	if msg, ok := s.messages[id]; ok {
		return msg, nil
	}
	return nil, ErrNotFound
}

func (s *mockBenchStore) List(ctx context.Context, filter Filter) ([]*Message, error) {
	var result []*Message
	for _, msg := range s.messages {
		result = append(result, msg)
		if filter.Limit > 0 && len(result) >= filter.Limit {
			break
		}
	}
	return result, nil
}

// BenchmarkMessageCreation benchmarks creating Message structs
func BenchmarkMessageCreation(b *testing.B) {
	now := time.Now()
	payload := []byte(`{"order_id": "12345", "amount": 99.99}`)
	metadata := map[string]string{"source": "checkout"}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Message{
			ID:          "msg-" + string(rune(i)),
			EventName:   "orders.created",
			Payload:     payload,
			Metadata:    metadata,
			ScheduledAt: now.Add(time.Hour),
			CreatedAt:   now,
		}
	}
}

// BenchmarkMockStoreSchedule benchmarks the Schedule operation with mock store
func BenchmarkMockStoreSchedule(b *testing.B) {
	ctx := context.Background()
	store := newMockBenchStore()
	payload := []byte(`{"order_id": "12345"}`)
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		msg := Message{
			ID:          "msg-" + string(rune(i)),
			EventName:   "orders.created",
			Payload:     payload,
			ScheduledAt: now.Add(time.Hour),
			CreatedAt:   now,
		}
		_ = store.Schedule(ctx, msg)
	}
}

// BenchmarkMockStoreGet benchmarks the Get operation with mock store
func BenchmarkMockStoreGet(b *testing.B) {
	ctx := context.Background()
	store := newMockBenchStore()
	payload := []byte(`{"order_id": "12345"}`)
	now := time.Now()

	// Pre-populate store
	for i := 0; i < 1000; i++ {
		msg := Message{
			ID:          "msg-" + string(rune(i)),
			EventName:   "orders.created",
			Payload:     payload,
			ScheduledAt: now.Add(time.Hour),
			CreatedAt:   now,
		}
		_ = store.Schedule(ctx, msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.Get(ctx, "msg-"+string(rune(i%1000)))
	}
}

// BenchmarkMockStoreList benchmarks the List operation with mock store
func BenchmarkMockStoreList(b *testing.B) {
	ctx := context.Background()
	store := newMockBenchStore()
	payload := []byte(`{"order_id": "12345"}`)
	now := time.Now()

	// Pre-populate store
	for i := 0; i < 1000; i++ {
		msg := Message{
			ID:          "msg-" + string(rune(i)),
			EventName:   "orders.created",
			Payload:     payload,
			ScheduledAt: now.Add(time.Hour),
			CreatedAt:   now,
		}
		_ = store.Schedule(ctx, msg)
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_, _ = store.List(ctx, Filter{Limit: 100})
	}
}

// BenchmarkOptionsApply benchmarks applying options
func BenchmarkOptionsApply(b *testing.B) {
	opts := []Option{
		WithPollInterval(50 * time.Millisecond),
		WithBatchSize(200),
		WithKeyPrefix("bench:"),
		WithMaxRetries(5),
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		o := defaultOptions()
		for _, opt := range opts {
			opt(o)
		}
	}
}

// BenchmarkFilterCreation benchmarks creating Filter structs
func BenchmarkFilterCreation(b *testing.B) {
	now := time.Now()

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = Filter{
			EventName: "orders.created",
			Before:    now.Add(time.Hour),
			After:     now.Add(-time.Hour),
			Limit:     100,
		}
	}
}
