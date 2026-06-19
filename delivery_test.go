package scheduler

import (
	"context"
	"errors"
	"io"
	"log/slog"
	"testing"
	"time"
)

// discardLogger returns a logger that drops all output, for quiet tests.
func discardLogger() *slog.Logger {
	return slog.New(slog.NewTextHandler(io.Discard, nil))
}

// runRetryLifecycle drives a message through handleDeliveryFailure exactly as
// the backends do: increment RetryCount from the decision until it is sent to
// the DLQ. It returns the total number of delivery attempts made.
func runRetryLifecycle(opts *options, msg *Message) int {
	ctx := context.Background()
	logger := discardLogger()
	publishErr := errors.New("transport down")

	attempts := 0
	for {
		attempts++
		decision := handleDeliveryFailure(ctx, opts, msg, publishErr, logger)
		if decision.sendToDLQ {
			return attempts
		}
		msg.RetryCount = decision.retryCount
		if attempts > 1000 {
			return attempts // safety valve so a logic bug can't hang the test
		}
	}
}

func TestHandleDeliveryFailure_MaxRetriesAttemptCount(t *testing.T) {
	t.Parallel()
	// WithMaxRetries(n) must allow exactly n+1 total attempts (1 initial + n retries).
	for _, maxRetries := range []int{1, 3, 5} {
		opts := defaultOptions()
		opts.maxRetries = maxRetries
		dlq := &mockDLQ{}
		opts.dlq = dlq

		msg := &Message{ID: "msg-1", EventName: "test.event", Payload: []byte("data")}
		attempts := runRetryLifecycle(opts, msg)

		wantAttempts := maxRetries + 1
		if attempts != wantAttempts {
			t.Errorf("maxRetries=%d: expected %d attempts, got %d", maxRetries, wantAttempts, attempts)
		}
		if len(dlq.messages) != 1 {
			t.Fatalf("maxRetries=%d: expected 1 DLQ entry, got %d", maxRetries, len(dlq.messages))
		}
		// The DLQ entry records the retry count at the moment of discard, which is maxRetries.
		if got := dlq.messages[0].RetryCount; got != maxRetries {
			t.Errorf("maxRetries=%d: expected DLQ RetryCount %d, got %d", maxRetries, maxRetries, got)
		}
	}
}

func TestHandleDeliveryFailure_InfiniteRetries(t *testing.T) {
	t.Parallel()
	// maxRetries=0 means retry indefinitely; the message must never be sent to DLQ.
	opts := defaultOptions()
	opts.maxRetries = 0
	dlq := &mockDLQ{}
	opts.dlq = dlq

	ctx := context.Background()
	logger := discardLogger()
	msg := &Message{ID: "msg-1", EventName: "test.event"}

	for i := range 50 {
		decision := handleDeliveryFailure(ctx, opts, msg, errors.New("boom"), logger)
		if decision.sendToDLQ {
			t.Fatalf("attempt %d: message sent to DLQ despite maxRetries=0", i)
		}
		if decision.retryCount != msg.RetryCount+1 {
			t.Fatalf("attempt %d: expected retryCount %d, got %d", i, msg.RetryCount+1, decision.retryCount)
		}
		msg.RetryCount = decision.retryCount
	}
	if len(dlq.messages) != 0 {
		t.Errorf("expected no DLQ entries with infinite retries, got %d", len(dlq.messages))
	}
}

func TestHandleDeliveryFailure_DLQParams(t *testing.T) {
	t.Parallel()
	opts := defaultOptions()
	opts.maxRetries = 1
	dlq := &mockDLQ{}
	opts.dlq = dlq

	publishErr := errors.New("transport exploded")
	msg := &Message{
		ID:         "order-42",
		EventName:  "orders.created",
		Payload:    []byte(`{"id":42}`),
		Metadata:   map[string]string{"region": "us-east"},
		RetryCount: 1, // already at the limit
	}

	decision := handleDeliveryFailure(context.Background(), opts, msg, publishErr, discardLogger())
	if !decision.sendToDLQ {
		t.Fatal("expected message at retry limit to be sent to DLQ")
	}
	if len(dlq.messages) != 1 {
		t.Fatalf("expected 1 DLQ entry, got %d", len(dlq.messages))
	}
	entry := dlq.messages[0]
	if entry.EventName != "orders.created" {
		t.Errorf("expected EventName 'orders.created', got %q", entry.EventName)
	}
	if entry.OriginalID != "order-42" {
		t.Errorf("expected OriginalID 'order-42', got %q", entry.OriginalID)
	}
	if string(entry.Payload) != `{"id":42}` {
		t.Errorf("unexpected DLQ payload: %s", entry.Payload)
	}
	if entry.Metadata["region"] != "us-east" {
		t.Errorf("expected metadata region 'us-east', got %q", entry.Metadata["region"])
	}
	if !errors.Is(entry.Err, publishErr) {
		t.Errorf("expected DLQ Err to wrap publish error, got %v", entry.Err)
	}
	if entry.Source != "scheduler" {
		t.Errorf("expected DLQ Source 'scheduler', got %q", entry.Source)
	}
}

func TestHandleDeliveryFailure_NoDLQDiscards(t *testing.T) {
	t.Parallel()
	// Without a DLQ, exceeding retries must still terminate cleanly (discard, no panic).
	opts := defaultOptions()
	opts.maxRetries = 2

	msg := &Message{ID: "msg-1", EventName: "test.event"}
	attempts := runRetryLifecycle(opts, msg)
	if attempts != 3 {
		t.Errorf("expected 3 attempts (1 + 2 retries), got %d", attempts)
	}
}

func TestHandleDeliveryFailure_BackoffDelay(t *testing.T) {
	t.Parallel()
	opts := defaultOptions()
	opts.maxRetries = 0
	backoff := &mockBackoff{delay: 100 * time.Millisecond}
	opts.backoff = backoff

	before := time.Now()
	msg := &Message{ID: "msg-1", EventName: "test.event", RetryCount: 2}
	decision := handleDeliveryFailure(context.Background(), opts, msg, errors.New("boom"), discardLogger())

	if decision.sendToDLQ {
		t.Fatal("did not expect DLQ with infinite retries")
	}
	// newRetryCount = 3, backoff.NextDelay(3-1=2) = 100ms * 3 = 300ms.
	wantDelay := 300 * time.Millisecond
	gotDelay := decision.nextRetryAt.Sub(before)
	if gotDelay < wantDelay-50*time.Millisecond || gotDelay > wantDelay+200*time.Millisecond {
		t.Errorf("expected next retry ~%v out, got %v", wantDelay, gotDelay)
	}
}

func TestHandleDeliveryFailure_NoBackoffImmediate(t *testing.T) {
	t.Parallel()
	opts := defaultOptions() // no backoff
	opts.maxRetries = 0

	before := time.Now()
	msg := &Message{ID: "msg-1", EventName: "test.event"}
	decision := handleDeliveryFailure(context.Background(), opts, msg, errors.New("boom"), discardLogger())

	// Without backoff, the next retry is scheduled for ~now (immediate on next poll).
	if decision.nextRetryAt.Sub(before) > time.Second {
		t.Errorf("expected immediate retry without backoff, got %v out", decision.nextRetryAt.Sub(before))
	}
}

func TestHandleSuccessfulDelivery_OneShotTerminal(t *testing.T) {
	t.Parallel()
	opts := defaultOptions()
	msg := &Message{ID: "msg-1", EventName: "test.event"} // no recurrence

	outcome := handleSuccessfulDelivery(context.Background(), opts, msg, time.Now())
	if !outcome.terminal {
		t.Error("expected one-shot message to be terminal after delivery")
	}
}

func TestHandleSuccessfulDelivery_IntervalRecurring(t *testing.T) {
	t.Parallel()
	opts := defaultOptions()
	now := time.Now()
	msg := &Message{
		ID:        "msg-1",
		EventName: "test.event",
		Recurrence: &Recurrence{
			Type:     RecurrenceInterval,
			Interval: time.Hour,
		},
		OccurrenceCount: 0,
	}

	outcome := handleSuccessfulDelivery(context.Background(), opts, msg, now)
	if outcome.terminal {
		t.Fatal("expected recurring message to be non-terminal")
	}
	if outcome.newCount != 1 {
		t.Errorf("expected occurrence count 1, got %d", outcome.newCount)
	}
	if !outcome.nextAt.Equal(now.Add(time.Hour)) {
		t.Errorf("expected next fire at now+1h (%v), got %v", now.Add(time.Hour), outcome.nextAt)
	}
}

func TestHandleSuccessfulDelivery_MaxOccurrencesTerminal(t *testing.T) {
	t.Parallel()
	opts := defaultOptions()
	msg := &Message{
		ID:        "msg-1",
		EventName: "test.event",
		Recurrence: &Recurrence{
			Type:           RecurrenceInterval,
			Interval:       time.Hour,
			MaxOccurrences: 3,
		},
		OccurrenceCount: 2, // delivering the 3rd (final) occurrence now
	}

	outcome := handleSuccessfulDelivery(context.Background(), opts, msg, time.Now())
	if !outcome.terminal {
		t.Error("expected terminal after reaching MaxOccurrences")
	}
}
