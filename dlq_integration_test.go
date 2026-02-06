//go:build integration

package scheduler

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"github.com/rbaliyan/event/v3/backoff"
	"github.com/rbaliyan/event/v3/transport/message"
)

// TestSchedulerWithDLQIntegration tests that the scheduler correctly sends
// messages to the DLQ after exhausting max retries.
//
// This test verifies the cross-project integration between:
//   - event-scheduler: Message scheduling and retry logic
//   - DeadLetterQueue interface: Message storage after max retries
//
// Run with: go test -tags=integration -run TestSchedulerWithDLQIntegration
func TestSchedulerWithDLQIntegration(t *testing.T) {
	ctx := context.Background()

	t.Run("messages sent to DLQ after max retries", func(t *testing.T) {
		// Create mock transport that always fails
		transport := newMockTransport()
		transport.SetFailures(100, errors.New("permanent failure"))

		// Create mock DLQ
		dlq := newMockDLQ()

		// Create in-memory scheduler with retry and DLQ
		scheduler := &inMemorySchedulerWithDLQ{
			messages:      make(map[string]*Message),
			transport:     transport,
			backoff:       &backoff.Constant{Delay: time.Millisecond},
			maxRetries:    3,
			dlq:           dlq,
			stuckDuration: 5 * time.Minute,
		}

		// Schedule a message
		msg := Message{
			ID:          "test-msg-1",
			EventName:   "orders.created",
			Payload:     []byte(`{"order_id": "12345"}`),
			Metadata:    map[string]string{"source": "test"},
			ScheduledAt: time.Now().Add(-time.Second), // Due immediately
			CreatedAt:   time.Now(),
		}

		if err := scheduler.Schedule(ctx, msg); err != nil {
			t.Fatalf("failed to schedule message: %v", err)
		}

		// Process the message multiple times to exhaust retries
		for i := 0; i < 5; i++ {
			scheduler.processDue(ctx)
			time.Sleep(5 * time.Millisecond) // Wait for backoff
		}

		// Wait for DLQ entry
		select {
		case <-dlq.storeCh:
			// Good, message was sent to DLQ
		case <-time.After(time.Second):
			t.Fatal("message was not sent to DLQ")
		}

		// Verify DLQ entry
		entries := dlq.Entries()
		if len(entries) != 1 {
			t.Fatalf("expected 1 DLQ entry, got %d", len(entries))
		}

		entry := entries[0]
		if entry.EventName != "orders.created" {
			t.Errorf("expected event name 'orders.created', got %s", entry.EventName)
		}
		if entry.OriginalID != "test-msg-1" {
			t.Errorf("expected original ID 'test-msg-1', got %s", entry.OriginalID)
		}
		if entry.RetryCount != 3 {
			t.Errorf("expected retry count 3, got %d", entry.RetryCount)
		}
		if entry.Source != "scheduler" {
			t.Errorf("expected source 'scheduler', got %s", entry.Source)
		}
	})

	t.Run("messages not sent to DLQ before max retries", func(t *testing.T) {
		// Create mock transport that fails twice then succeeds
		transport := newMockTransport()
		transport.SetFailures(2, errors.New("temporary failure"))

		// Create mock DLQ
		dlq := newMockDLQ()

		// Create in-memory scheduler with retry and DLQ
		scheduler := &inMemorySchedulerWithDLQ{
			messages:      make(map[string]*Message),
			transport:     transport,
			backoff:       &backoff.Constant{Delay: time.Millisecond},
			maxRetries:    5, // More retries than failures
			dlq:           dlq,
			stuckDuration: 5 * time.Minute,
		}

		// Schedule a message
		msg := Message{
			ID:          "test-msg-2",
			EventName:   "orders.created",
			Payload:     []byte(`{"order_id": "12345"}`),
			ScheduledAt: time.Now().Add(-time.Second),
			CreatedAt:   time.Now(),
		}

		if err := scheduler.Schedule(ctx, msg); err != nil {
			t.Fatalf("failed to schedule message: %v", err)
		}

		// Process until successful
		for i := 0; i < 5; i++ {
			scheduler.processDue(ctx)
			time.Sleep(10 * time.Millisecond)
		}

		// Verify no DLQ entries
		entries := dlq.Entries()
		if len(entries) != 0 {
			t.Fatalf("expected 0 DLQ entries, got %d", len(entries))
		}

		// Verify message was published
		published := transport.Published()
		if len(published) != 1 {
			t.Fatalf("expected 1 published message, got %d", len(published))
		}
	})
}

// inMemorySchedulerWithDLQ is a simple in-memory scheduler for testing
// the DLQ integration without requiring external services.
type inMemorySchedulerWithDLQ struct {
	mu            sync.Mutex
	messages      map[string]*Message
	transport     *mockTransport
	backoff       BackoffStrategy
	maxRetries    int
	dlq           DeadLetterQueue
	stuckDuration time.Duration
}

func (s *inMemorySchedulerWithDLQ) Schedule(ctx context.Context, msg Message) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.messages[msg.ID] = &msg
	return nil
}

func (s *inMemorySchedulerWithDLQ) processDue(ctx context.Context) {
	s.mu.Lock()
	defer s.mu.Unlock()

	now := time.Now()
	for id, msg := range s.messages {
		if msg.ScheduledAt.After(now) {
			continue
		}

		// Try to publish
		transportMsg := message.New(msg.ID, "scheduler", msg.Payload, msg.Metadata)

		if err := s.transport.Publish(ctx, msg.EventName, transportMsg); err != nil {
			// Failed to publish
			msg.RetryCount++

			if s.maxRetries > 0 && msg.RetryCount >= s.maxRetries {
				// Send to DLQ
				if s.dlq != nil {
					_ = s.dlq.Store(ctx, msg.EventName, msg.ID, msg.Payload, msg.Metadata, err, msg.RetryCount, "scheduler")
				}
				delete(s.messages, id)
				continue
			}

			// Schedule for retry
			if s.backoff != nil {
				msg.ScheduledAt = now.Add(s.backoff.NextDelay(msg.RetryCount))
			}
			continue
		}

		// Successfully published
		delete(s.messages, id)
	}
}

