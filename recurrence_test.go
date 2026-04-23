package scheduler

import (
	"database/sql"
	"testing"
	"time"
)

// --- validateRecurrence tests ---

func TestValidateRecurrence_Nil(t *testing.T) {
	if err := validateRecurrence(nil); err != nil {
		t.Errorf("expected nil error for nil recurrence, got %v", err)
	}
}

func TestValidateRecurrence_Interval_Valid(t *testing.T) {
	r := &Recurrence{Type: RecurrenceInterval, Interval: time.Hour}
	if err := validateRecurrence(r); err != nil {
		t.Errorf("expected valid interval recurrence, got %v", err)
	}
}

func TestValidateRecurrence_Interval_Zero(t *testing.T) {
	r := &Recurrence{Type: RecurrenceInterval, Interval: 0}
	if err := validateRecurrence(r); err == nil {
		t.Error("expected error for zero interval")
	}
}

func TestValidateRecurrence_Interval_Negative(t *testing.T) {
	r := &Recurrence{Type: RecurrenceInterval, Interval: -time.Second}
	if err := validateRecurrence(r); err == nil {
		t.Error("expected error for negative interval")
	}
}

func TestValidateRecurrence_Cron_Valid(t *testing.T) {
	r := &Recurrence{Type: RecurrenceCron, Cron: "0 9 * * 1-5"}
	if err := validateRecurrence(r); err != nil {
		t.Errorf("expected valid cron recurrence, got %v", err)
	}
}

func TestValidateRecurrence_Cron_Invalid(t *testing.T) {
	r := &Recurrence{Type: RecurrenceCron, Cron: "not a cron expression"}
	if err := validateRecurrence(r); err == nil {
		t.Error("expected error for invalid cron expression")
	}
}

func TestValidateRecurrence_Cron_Empty(t *testing.T) {
	r := &Recurrence{Type: RecurrenceCron, Cron: ""}
	if err := validateRecurrence(r); err == nil {
		t.Error("expected error for empty cron expression")
	}
}

func TestValidateRecurrence_UnknownType(t *testing.T) {
	r := &Recurrence{Type: "unknown"}
	if err := validateRecurrence(r); err == nil {
		t.Error("expected error for unknown recurrence type")
	}
}

func TestValidateRecurrence_UntilInPast(t *testing.T) {
	r := &Recurrence{
		Type:     RecurrenceInterval,
		Interval: time.Hour,
		Until:    time.Now().Add(-time.Second),
	}
	if err := validateRecurrence(r); err == nil {
		t.Error("expected error when Until is in the past")
	}
}

func TestValidateRecurrence_UntilInFuture(t *testing.T) {
	r := &Recurrence{
		Type:     RecurrenceInterval,
		Interval: time.Hour,
		Until:    time.Now().Add(time.Hour),
	}
	if err := validateRecurrence(r); err != nil {
		t.Errorf("expected no error when Until is in the future, got %v", err)
	}
}

func TestValidateRecurrence_MaxOccurrences(t *testing.T) {
	r := &Recurrence{
		Type:           RecurrenceInterval,
		Interval:       time.Minute,
		MaxOccurrences: 5,
	}
	if err := validateRecurrence(r); err != nil {
		t.Errorf("expected no error with MaxOccurrences set, got %v", err)
	}
}

// --- computeNextSchedule tests ---

func TestComputeNextSchedule_OneShot(t *testing.T) {
	msg := &Message{EventName: "test", OccurrenceCount: 0}
	outcome := computeNextSchedule(msg, time.Now())
	if !outcome.terminal {
		t.Error("expected terminal=true for one-shot message")
	}
}

func TestComputeNextSchedule_Interval_Continues(t *testing.T) {
	now := time.Now()
	msg := &Message{
		EventName:       "test",
		OccurrenceCount: 0,
		Recurrence: &Recurrence{
			Type:     RecurrenceInterval,
			Interval: time.Hour,
		},
	}
	outcome := computeNextSchedule(msg, now)
	if outcome.terminal {
		t.Error("expected terminal=false for recurring interval message")
	}
	if outcome.newCount != 1 {
		t.Errorf("expected newCount=1, got %d", outcome.newCount)
	}
	expectedNext := now.Add(time.Hour)
	if diff := outcome.nextAt.Sub(expectedNext); diff < -time.Second || diff > time.Second {
		t.Errorf("expected nextAt ~%v, got %v (diff %v)", expectedNext, outcome.nextAt, diff)
	}
}

func TestComputeNextSchedule_Interval_MaxOccurrences_Reached(t *testing.T) {
	msg := &Message{
		EventName:       "test",
		OccurrenceCount: 4, // about to complete the 5th delivery
		Recurrence: &Recurrence{
			Type:           RecurrenceInterval,
			Interval:       time.Hour,
			MaxOccurrences: 5,
		},
	}
	outcome := computeNextSchedule(msg, time.Now())
	if !outcome.terminal {
		t.Error("expected terminal=true when MaxOccurrences reached")
	}
}

func TestComputeNextSchedule_Interval_MaxOccurrences_NotYet(t *testing.T) {
	msg := &Message{
		EventName:       "test",
		OccurrenceCount: 3, // 4th delivery, max is 5
		Recurrence: &Recurrence{
			Type:           RecurrenceInterval,
			Interval:       time.Hour,
			MaxOccurrences: 5,
		},
	}
	outcome := computeNextSchedule(msg, time.Now())
	if outcome.terminal {
		t.Error("expected terminal=false when MaxOccurrences not yet reached")
	}
	if outcome.newCount != 4 {
		t.Errorf("expected newCount=4, got %d", outcome.newCount)
	}
}

func TestComputeNextSchedule_Interval_Until_Expired(t *testing.T) {
	// next would be in 1h, but Until is in 30min
	now := time.Now()
	msg := &Message{
		EventName:       "test",
		OccurrenceCount: 0,
		Recurrence: &Recurrence{
			Type:     RecurrenceInterval,
			Interval: time.Hour,
			Until:    now.Add(30 * time.Minute),
		},
	}
	outcome := computeNextSchedule(msg, now)
	if !outcome.terminal {
		t.Error("expected terminal=true when next fire is after Until")
	}
}

func TestComputeNextSchedule_Interval_Until_NotExpired(t *testing.T) {
	now := time.Now()
	msg := &Message{
		EventName:       "test",
		OccurrenceCount: 0,
		Recurrence: &Recurrence{
			Type:     RecurrenceInterval,
			Interval: time.Hour,
			Until:    now.Add(2 * time.Hour),
		},
	}
	outcome := computeNextSchedule(msg, now)
	if outcome.terminal {
		t.Error("expected terminal=false when Until is after next fire")
	}
}

func TestComputeNextSchedule_Cron_Continues(t *testing.T) {
	// Use a cron that fires every minute
	msg := &Message{
		EventName:       "test",
		OccurrenceCount: 0,
		Recurrence: &Recurrence{
			Type: RecurrenceCron,
			Cron: "* * * * *",
		},
	}
	now := time.Now()
	outcome := computeNextSchedule(msg, now)
	if outcome.terminal {
		t.Error("expected terminal=false for cron with no termination")
	}
	if outcome.newCount != 1 {
		t.Errorf("expected newCount=1, got %d", outcome.newCount)
	}
	// Next cron fire should be within 2 minutes
	if outcome.nextAt.Before(now) {
		t.Error("expected nextAt to be in the future")
	}
	if outcome.nextAt.Sub(now) > 2*time.Minute {
		t.Errorf("expected nextAt within 2 minutes, got %v", outcome.nextAt.Sub(now))
	}
}

func TestComputeNextSchedule_Cron_MaxOccurrences(t *testing.T) {
	msg := &Message{
		EventName:       "test",
		OccurrenceCount: 2,
		Recurrence: &Recurrence{
			Type:           RecurrenceCron,
			Cron:           "* * * * *",
			MaxOccurrences: 3,
		},
	}
	outcome := computeNextSchedule(msg, time.Now())
	if !outcome.terminal {
		t.Error("expected terminal=true when cron MaxOccurrences reached")
	}
}

func TestComputeNextSchedule_Cron_Until(t *testing.T) {
	now := time.Now()
	msg := &Message{
		EventName:       "test",
		OccurrenceCount: 0,
		Recurrence: &Recurrence{
			Type:  RecurrenceCron,
			Cron:  "* * * * *",
			Until: now.Add(30 * time.Second), // next minute will be after Until
		},
	}
	outcome := computeNextSchedule(msg, now)
	// next cron fire (next minute) may be after the 30s Until
	if !outcome.terminal {
		// If next fire is within 30s, that's fine — it means Until is far enough
		// This is inherently time-dependent; just check that Until logic works
		if outcome.nextAt.After(msg.Recurrence.Until) {
			t.Error("nextAt is after Until but terminal=false")
		}
	}
}

func TestComputeNextSchedule_UnknownType(t *testing.T) {
	msg := &Message{
		EventName: "test",
		Recurrence: &Recurrence{
			Type: "unknown",
		},
	}
	outcome := computeNextSchedule(msg, time.Now())
	if !outcome.terminal {
		t.Error("expected terminal=true for unknown recurrence type")
	}
}

// --- encodeRecurrence / decodeRecurrence roundtrip ---

func TestEncodeDecodeRecurrence_Nil(t *testing.T) {
	recType, recVal, maxOcc, until := encodeRecurrence(nil)
	if recType != nil || recVal != nil || maxOcc != 0 || until != nil {
		t.Error("expected all nil/zero for nil recurrence")
	}
	r := decodeRecurrence(
		sql.NullString{},
		sql.NullString{},
		sql.NullInt32{},
		sql.NullTime{},
	)
	if r != nil {
		t.Errorf("expected nil recurrence from empty columns, got %+v", r)
	}
}

func TestEncodeDecodeRecurrence_Interval(t *testing.T) {
	original := &Recurrence{
		Type:           RecurrenceInterval,
		Interval:       90 * time.Minute,
		MaxOccurrences: 10,
	}
	recType, recVal, maxOcc, until := encodeRecurrence(original)

	if recType == nil || *recType != "interval" {
		t.Errorf("expected recType='interval', got %v", recType)
	}
	if recVal == nil || *recVal != "1h30m0s" {
		t.Errorf("expected recVal='1h30m0s', got %v", recVal)
	}
	if maxOcc != 10 {
		t.Errorf("expected maxOcc=10, got %d", maxOcc)
	}
	if until != nil {
		t.Errorf("expected until=nil, got %v", until)
	}

	decoded := decodeRecurrence(
		sql.NullString{String: *recType, Valid: true},
		sql.NullString{String: *recVal, Valid: true},
		sql.NullInt32{Int32: int32(maxOcc), Valid: true},
		sql.NullTime{},
	)
	if decoded == nil {
		t.Fatal("expected non-nil decoded recurrence")
	}
	if decoded.Type != RecurrenceInterval {
		t.Errorf("expected Type=interval, got %v", decoded.Type)
	}
	if decoded.Interval != 90*time.Minute {
		t.Errorf("expected Interval=90m, got %v", decoded.Interval)
	}
	if decoded.MaxOccurrences != 10 {
		t.Errorf("expected MaxOccurrences=10, got %d", decoded.MaxOccurrences)
	}
}

func TestEncodeDecodeRecurrence_Cron(t *testing.T) {
	until := time.Date(2026, 12, 31, 23, 59, 59, 0, time.UTC)
	original := &Recurrence{
		Type:  RecurrenceCron,
		Cron:  "0 9 * * 1-5",
		Until: until,
	}
	recType, recVal, maxOcc, recUntil := encodeRecurrence(original)

	if recType == nil || *recType != "cron" {
		t.Errorf("expected recType='cron', got %v", recType)
	}
	if recVal == nil || *recVal != "0 9 * * 1-5" {
		t.Errorf("expected recVal='0 9 * * 1-5', got %v", recVal)
	}
	if maxOcc != 0 {
		t.Errorf("expected maxOcc=0, got %d", maxOcc)
	}
	if recUntil == nil || !recUntil.Equal(until) {
		t.Errorf("expected until=%v, got %v", until, recUntil)
	}

	decoded := decodeRecurrence(
		sql.NullString{String: *recType, Valid: true},
		sql.NullString{String: *recVal, Valid: true},
		sql.NullInt32{Int32: int32(maxOcc), Valid: false},
		sql.NullTime{Time: *recUntil, Valid: true},
	)
	if decoded == nil {
		t.Fatal("expected non-nil decoded recurrence")
	}
	if decoded.Type != RecurrenceCron {
		t.Errorf("expected Type=cron, got %v", decoded.Type)
	}
	if decoded.Cron != "0 9 * * 1-5" {
		t.Errorf("expected Cron='0 9 * * 1-5', got %q", decoded.Cron)
	}
	if !decoded.Until.Equal(until) {
		t.Errorf("expected Until=%v, got %v", until, decoded.Until)
	}
}

// --- nextCronTime tests ---

func TestNextCronTime_ValidExpression(t *testing.T) {
	// "0 0 * * *" fires at midnight
	after := time.Date(2026, 1, 1, 12, 0, 0, 0, time.UTC)
	next, err := nextCronTime("0 0 * * *", after)
	if err != nil {
		t.Fatalf("nextCronTime error: %v", err)
	}
	expected := time.Date(2026, 1, 2, 0, 0, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Errorf("expected next=%v, got %v", expected, next)
	}
}

func TestNextCronTime_InvalidExpression(t *testing.T) {
	_, err := nextCronTime("invalid cron", time.Now())
	if err == nil {
		t.Error("expected error for invalid cron expression")
	}
}

func TestNextCronTime_EveryMinute(t *testing.T) {
	after := time.Date(2026, 1, 1, 12, 30, 0, 0, time.UTC)
	next, err := nextCronTime("* * * * *", after)
	if err != nil {
		t.Fatalf("nextCronTime error: %v", err)
	}
	expected := time.Date(2026, 1, 1, 12, 31, 0, 0, time.UTC)
	if !next.Equal(expected) {
		t.Errorf("expected next=%v, got %v", expected, next)
	}
}

// --- Integration: computeNextSchedule with multiple deliveries ---

func TestComputeNextSchedule_MultipleDeliveries(t *testing.T) {
	// Simulate 5 interval deliveries, stopping at MaxOccurrences=5
	now := time.Now()
	msg := &Message{
		EventName:       "test",
		OccurrenceCount: 0,
		ScheduledAt:     now,
		Recurrence: &Recurrence{
			Type:           RecurrenceInterval,
			Interval:       time.Hour,
			MaxOccurrences: 5,
		},
	}

	for i := 1; i <= 4; i++ {
		outcome := computeNextSchedule(msg, now)
		if outcome.terminal {
			t.Fatalf("expected terminal=false at delivery %d, got terminal=true", i)
		}
		msg.OccurrenceCount = outcome.newCount
		msg.ScheduledAt = outcome.nextAt
		now = outcome.nextAt
	}

	// 5th delivery should be terminal
	outcome := computeNextSchedule(msg, now)
	if !outcome.terminal {
		t.Error("expected terminal=true on 5th delivery (MaxOccurrences=5)")
	}
}
