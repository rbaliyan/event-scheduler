//go:build integration

package scheduler

import (
	"context"
	"fmt"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/transport"
)

// --- Environment helpers ---

func getRedisAddr() string {
	if addr := os.Getenv("REDIS_ADDR"); addr != "" {
		return addr
	}
	return "localhost:6379"
}

func getMongoURI() string {
	if uri := os.Getenv("MONGO_URI"); uri != "" {
		return uri
	}
	return "mongodb://localhost:27017"
}

func getPostgresDSN() string {
	if dsn := os.Getenv("POSTGRES_DSN"); dsn != "" {
		return dsn
	}
	return "postgres://localhost:5432/test?sslmode=disable"
}

// --- Mock transport ---

type publishedMessage struct {
	EventName string
	MsgID     string
	Payload   []byte
	Metadata  map[string]string
}

type mockTransport struct {
	mu        sync.Mutex
	published []publishedMessage
	publishCh chan publishedMessage
	failCount int
	failErr   error
}

func newMockTransport() *mockTransport {
	return &mockTransport{
		publishCh: make(chan publishedMessage, 100),
	}
}

func (m *mockTransport) SetFailures(count int, err error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.failCount = count
	m.failErr = err
}

func (m *mockTransport) Publish(ctx context.Context, name string, msg transport.Message) error {
	m.mu.Lock()
	shouldFail := m.failCount > 0
	if shouldFail {
		m.failCount--
		err := m.failErr
		m.mu.Unlock()
		return err
	}
	pub := publishedMessage{
		EventName: name,
		MsgID:     msg.ID(),
		Payload:   msg.Payload(),
		Metadata:  msg.Metadata(),
	}
	m.published = append(m.published, pub)
	m.mu.Unlock()

	// Non-blocking send to channel
	select {
	case m.publishCh <- pub:
	default:
	}
	return nil
}

func (m *mockTransport) Published() []publishedMessage {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]publishedMessage, len(m.published))
	copy(result, m.published)
	return result
}

func (m *mockTransport) WaitForPublish(t *testing.T, timeout time.Duration) publishedMessage {
	t.Helper()
	select {
	case msg := <-m.publishCh:
		return msg
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for publish after %v", timeout)
		return publishedMessage{}
	}
}

func (m *mockTransport) WaitForPublishes(t *testing.T, count int, timeout time.Duration) []publishedMessage {
	t.Helper()
	var result []publishedMessage
	deadline := time.After(timeout)
	for i := 0; i < count; i++ {
		select {
		case msg := <-m.publishCh:
			result = append(result, msg)
		case <-deadline:
			t.Fatalf("timed out waiting for publish %d/%d after %v", i+1, count, timeout)
			return result
		}
	}
	return result
}

func (m *mockTransport) RegisterEvent(ctx context.Context, name string) error   { return nil }
func (m *mockTransport) UnregisterEvent(ctx context.Context, name string) error { return nil }
func (m *mockTransport) Subscribe(ctx context.Context, name string, opts ...transport.SubscribeOption) (transport.Subscription, error) {
	return nil, fmt.Errorf("not implemented")
}
func (m *mockTransport) Close(ctx context.Context) error { return nil }

// Compile-time check
var _ transport.Transport = (*mockTransport)(nil)

// --- Mock DLQ ---

type integrationDLQEntry struct {
	EventName  string
	OriginalID string
	Payload    []byte
	Metadata   map[string]string
	Err        error
	RetryCount int
	Source     string
}

type mockDLQIntegration struct {
	mu       sync.Mutex
	entries  []integrationDLQEntry
	failNext bool
	storeCh  chan struct{}
}

func newMockDLQ() *mockDLQIntegration {
	return &mockDLQIntegration{
		storeCh: make(chan struct{}, 10),
	}
}

func (m *mockDLQIntegration) Store(ctx context.Context, eventName, originalID string, payload []byte, metadata map[string]string, err error, retryCount int, source string) error {
	m.mu.Lock()
	if m.failNext {
		m.failNext = false
		m.mu.Unlock()
		return fmt.Errorf("DLQ store failure")
	}
	m.entries = append(m.entries, integrationDLQEntry{
		EventName:  eventName,
		OriginalID: originalID,
		Payload:    payload,
		Metadata:   metadata,
		Err:        err,
		RetryCount: retryCount,
		Source:     source,
	})
	m.mu.Unlock()

	select {
	case m.storeCh <- struct{}{}:
	default:
	}
	return nil
}

func (m *mockDLQIntegration) Entries() []integrationDLQEntry {
	m.mu.Lock()
	defer m.mu.Unlock()
	result := make([]integrationDLQEntry, len(m.entries))
	copy(result, m.entries)
	return result
}

func (m *mockDLQIntegration) WaitForStore(t *testing.T, timeout time.Duration) {
	t.Helper()
	select {
	case <-m.storeCh:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for DLQ store after %v", timeout)
	}
}

// Compile-time check
var _ DeadLetterQueue = (*mockDLQIntegration)(nil)

// --- Fixed backoff ---

type fixedBackoff struct {
	delay time.Duration
}

func (b *fixedBackoff) NextDelay(attempt int) time.Duration { return b.delay }
func (b *fixedBackoff) Reset()                              {}

// Compile-time check
var _ BackoffStrategy = (*fixedBackoff)(nil)

// --- Helper functions ---

func testPrefix() string {
	return fmt.Sprintf("test:sched:%d:", time.Now().UnixNano())
}

func makeMessage(id, eventName string, scheduledAt time.Time) Message {
	return Message{
		ID:          id,
		EventName:   eventName,
		Payload:     []byte(fmt.Sprintf(`{"id":%q}`, id)),
		Metadata:    map[string]string{"test": "true"},
		ScheduledAt: scheduledAt,
		CreatedAt:   time.Now(),
	}
}

func waitFor(t *testing.T, timeout time.Duration, condition func() bool, msg string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for: %s", msg)
}
