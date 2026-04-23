package scheduler

import (
	"database/sql"
	"time"

	"github.com/robfig/cron/v3"
)

// encodeRecurrence converts a *Recurrence into SQL-storable scalars.
// Returns (recurrenceType, recurrenceValue, maxOccurrences, until).
// All values are nil/zero when r is nil (one-shot message).
func encodeRecurrence(r *Recurrence) (recType, recVal *string, maxOcc int, until *time.Time) {
	if r == nil {
		return nil, nil, 0, nil
	}
	rt := string(r.Type)
	recType = &rt
	switch r.Type {
	case RecurrenceInterval:
		v := r.Interval.String()
		recVal = &v
	case RecurrenceCron:
		recVal = &r.Cron
	}
	maxOcc = r.MaxOccurrences
	if !r.Until.IsZero() {
		u := r.Until
		until = &u
	}
	return recType, recVal, maxOcc, until
}

// decodeRecurrence reconstructs a *Recurrence from SQL nullable columns.
// Returns nil when recType is not set (one-shot message).
func decodeRecurrence(recType, recVal sql.NullString, maxOcc sql.NullInt32, until sql.NullTime) *Recurrence {
	if !recType.Valid || recType.String == "" {
		return nil
	}
	r := &Recurrence{
		Type: RecurrenceType(recType.String),
	}
	if recVal.Valid {
		switch r.Type {
		case RecurrenceInterval:
			if d, err := time.ParseDuration(recVal.String); err == nil {
				r.Interval = d
			}
		case RecurrenceCron:
			r.Cron = recVal.String
		}
	}
	if maxOcc.Valid {
		r.MaxOccurrences = int(maxOcc.Int32)
	}
	if until.Valid {
		r.Until = until.Time
	}
	return r
}

// nextCronTime returns the next time the cron expression fires after after.
// The expression must be a valid 5-field standard cron expression; it is
// assumed to have been validated by validateRecurrence at Schedule() time.
func nextCronTime(expr string, after time.Time) (time.Time, error) {
	s, err := cron.ParseStandard(expr)
	if err != nil {
		return time.Time{}, err
	}
	return s.Next(after), nil
}

// recurrenceOutcome is the result of evaluating a successfully delivered
// recurring message. terminal=true means the message should be deleted;
// terminal=false means it should be rescheduled at nextAt with newCount.
type recurrenceOutcome struct {
	terminal bool
	nextAt   time.Time
	newCount int
}

// computeNextSchedule decides what to do after a successful delivery.
//
// For one-shot messages (msg.Recurrence == nil) it always returns terminal=true.
// For recurring messages it computes the next fire time and checks termination
// conditions (MaxOccurrences, Until). The caller increments OccurrenceCount and
// updates ScheduledAt based on the returned outcome.
func computeNextSchedule(msg *Message, now time.Time) recurrenceOutcome {
	r := msg.Recurrence
	if r == nil {
		return recurrenceOutcome{terminal: true}
	}

	newCount := msg.OccurrenceCount + 1

	// Count-based termination: we just completed delivery newCount.
	if r.MaxOccurrences > 0 && newCount >= r.MaxOccurrences {
		return recurrenceOutcome{terminal: true}
	}

	// Compute next fire time.
	var next time.Time
	switch r.Type {
	case RecurrenceInterval:
		next = now.Add(r.Interval)
	case RecurrenceCron:
		var err error
		next, err = nextCronTime(r.Cron, now)
		if err != nil {
			// Expression was validated at Schedule() time; this path should be
			// unreachable. Terminate to avoid a permanently stuck message.
			return recurrenceOutcome{terminal: true}
		}
	default:
		return recurrenceOutcome{terminal: true}
	}

	// Time-based termination: next fire would be after the deadline.
	if !r.Until.IsZero() && next.After(r.Until) {
		return recurrenceOutcome{terminal: true}
	}

	return recurrenceOutcome{
		terminal: false,
		nextAt:   next,
		newCount: newCount,
	}
}
