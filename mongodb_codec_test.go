package scheduler

import (
	"testing"
	"time"
)

// These tests cover the pure mongoMessage <-> Message codec without any MongoDB
// server, so the conversion logic ships with real (non-integration) coverage.

func TestMongoCodec_OneShotRoundTrip(t *testing.T) {
	t.Parallel()
	now := time.Now().Truncate(time.Millisecond)
	orig := &Message{
		ID:          "m1",
		EventName:   "orders.created",
		Payload:     []byte(`{"id":1}`),
		Metadata:    map[string]string{"region": "us"},
		ScheduledAt: now.Add(time.Hour),
		CreatedAt:   now,
		RetryCount:  2,
	}

	got := fromMessage(orig).toMessage()

	if got.ID != orig.ID || got.EventName != orig.EventName {
		t.Errorf("id/event mismatch: %+v", got)
	}
	if string(got.Payload) != string(orig.Payload) {
		t.Errorf("payload = %q, want %q", got.Payload, orig.Payload)
	}
	if got.Metadata["region"] != "us" {
		t.Errorf("metadata lost: %+v", got.Metadata)
	}
	if !got.ScheduledAt.Equal(orig.ScheduledAt) || !got.CreatedAt.Equal(orig.CreatedAt) {
		t.Errorf("timestamps mismatch: %v / %v", got.ScheduledAt, got.CreatedAt)
	}
	if got.RetryCount != 2 {
		t.Errorf("retry_count = %d, want 2", got.RetryCount)
	}
	if got.Recurrence != nil {
		t.Errorf("expected nil recurrence for one-shot, got %+v", got.Recurrence)
	}
}

func TestMongoCodec_IntervalRecurrenceRoundTrip(t *testing.T) {
	t.Parallel()
	until := time.Now().Add(48 * time.Hour).Truncate(time.Millisecond)
	orig := &Message{
		ID:        "m2",
		EventName: "recurring",
		Recurrence: &Recurrence{
			Type:           RecurrenceInterval,
			Interval:       90 * time.Minute,
			MaxOccurrences: 10,
			Until:          until,
		},
		OccurrenceCount: 4,
	}

	got := fromMessage(orig).toMessage()

	if got.Recurrence == nil {
		t.Fatal("expected recurrence to survive round-trip")
	}
	if got.Recurrence.Type != RecurrenceInterval {
		t.Errorf("type = %q, want interval", got.Recurrence.Type)
	}
	if got.Recurrence.Interval != 90*time.Minute {
		t.Errorf("interval = %v, want 90m", got.Recurrence.Interval)
	}
	if got.Recurrence.MaxOccurrences != 10 {
		t.Errorf("max_occurrences = %d, want 10", got.Recurrence.MaxOccurrences)
	}
	if !got.Recurrence.Until.Equal(until) {
		t.Errorf("until = %v, want %v", got.Recurrence.Until, until)
	}
	if got.OccurrenceCount != 4 {
		t.Errorf("occurrence_count = %d, want 4", got.OccurrenceCount)
	}
}

func TestMongoCodec_CronRecurrenceRoundTrip(t *testing.T) {
	t.Parallel()
	orig := &Message{
		ID:         "m3",
		EventName:  "cron.job",
		Recurrence: &Recurrence{Type: RecurrenceCron, Cron: "0 9 * * 1-5"},
	}

	got := fromMessage(orig).toMessage()

	if got.Recurrence == nil {
		t.Fatal("expected recurrence")
	}
	if got.Recurrence.Type != RecurrenceCron {
		t.Errorf("type = %q, want cron", got.Recurrence.Type)
	}
	if got.Recurrence.Cron != "0 9 * * 1-5" {
		t.Errorf("cron = %q, want '0 9 * * 1-5'", got.Recurrence.Cron)
	}
	if !got.Recurrence.Until.IsZero() {
		t.Errorf("expected zero Until when unset, got %v", got.Recurrence.Until)
	}
}

func TestMongoCodec_StatusSetOnSchedule(t *testing.T) {
	t.Parallel()
	mm := fromMessage(&Message{ID: "m4", EventName: "e"})
	// fromMessage does not set status; Schedule sets statusPending explicitly.
	// Verify the zero value is the empty status so the pending filter ($exists/backward-compat) works.
	if mm.Status != "" {
		t.Errorf("expected empty status from fromMessage, got %q", mm.Status)
	}
}
