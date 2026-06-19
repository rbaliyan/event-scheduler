package scheduler

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"testing"
	"time"
	"unicode/utf8"
)

// cronDictionary seeds the cron-parsing fuzz targets with structurally-diverse
// expressions (ranges, steps, lists, names, edge fields) so the fuzzer explores
// the parser's branches faster than from random bytes alone.
var cronDictionary = []string{
	"* * * * *",
	"*/5 * * * *",
	"0 9 * * 1-5",
	"0 0 1 1 *",
	"15,45 * * * *",
	"0 0 * * 0",
	"0 0 31 2 *", // valid parse, no future occurrence
	"@hourly",
	"@daily",
	"59 23 31 12 6",
}

// FuzzMessageJSONRoundTrip guards the Redis storage path: a Message is stored
// as JSON in the index hash and decoded on claim (redis.go). Marshal->unmarshal
// must preserve the fields the scheduler relies on, and decoding must never panic.
func FuzzMessageJSONRoundTrip(f *testing.F) {
	f.Add("id-1", "orders.created", []byte(`{"order":1}`), "region", "us-east", 2, 1)
	f.Add("", "", []byte(nil), "", "", 0, 0)
	f.Add("πid", "evént", []byte{0x00, 0xff, 0x7f}, "k\n", "v\t", -1, 99)

	f.Fuzz(func(t *testing.T, id, event string, payload []byte, mk, mv string, retry, occ int) {
		// JSON strings must be valid UTF-8; Go's encoder replaces invalid bytes
		// with U+FFFD, so exact round-trip is only defined for valid-UTF-8 input
		// (production string fields are always valid UTF-8). Payload is []byte and
		// JSON-encodes as base64, so arbitrary bytes there DO round-trip exactly.
		if !utf8.ValidString(id) || !utf8.ValidString(event) || !utf8.ValidString(mk) || !utf8.ValidString(mv) {
			t.Skip()
		}
		orig := Message{
			ID:              id,
			EventName:       event,
			Payload:         payload,
			Metadata:        map[string]string{mk: mv},
			RetryCount:      retry,
			OccurrenceCount: occ,
		}
		data, err := json.Marshal(orig)
		if err != nil {
			return // not all inputs are marshalable; that's fine
		}
		var got Message
		if err := json.Unmarshal(data, &got); err != nil {
			t.Fatalf("unmarshal of self-marshaled message failed: %v", err)
		}
		if got.ID != id || got.EventName != event || got.RetryCount != retry || got.OccurrenceCount != occ {
			t.Fatalf("scalar fields not preserved: got %+v", got)
		}
		if string(got.Payload) != string(payload) {
			t.Fatalf("payload not preserved: got %q want %q", got.Payload, payload)
		}
		if got.Metadata[mk] != mv {
			t.Fatalf("metadata not preserved: got %v", got.Metadata)
		}
	})
}

// FuzzValidateRecurrence asserts the cross-component invariant: any cron string
// that validateRecurrence accepts must also parse in nextCronTime (which is
// invoked later at delivery time). A mismatch would strand a message forever.
func FuzzValidateRecurrence(f *testing.F) {
	f.Add("cron", "not a cron")
	f.Add("interval", "1h30m")
	f.Add("bogus", "")
	for _, expr := range cronDictionary {
		f.Add("cron", expr)
	}

	f.Fuzz(func(t *testing.T, typ, val string) {
		r := &Recurrence{Type: RecurrenceType(typ)}
		switch RecurrenceType(typ) {
		case RecurrenceInterval:
			if d, err := time.ParseDuration(val); err == nil {
				r.Interval = d
			}
		case RecurrenceCron:
			r.Cron = val
		}

		err := validateRecurrence(r)
		if err == nil && RecurrenceType(typ) == RecurrenceCron {
			if _, e := nextCronTime(r.Cron, time.Now()); e != nil {
				t.Fatalf("validateRecurrence accepted cron %q but nextCronTime rejects it: %v", r.Cron, e)
			}
		}
	})
}

// FuzzComputeNextSchedule asserts the core scheduling invariant: a non-terminal
// outcome must always advance strictly into the future. Before the interval<=0
// guard this could return nextAt==now and hot-loop.
func FuzzComputeNextSchedule(f *testing.F) {
	f.Add(int64(3600), "", 0, 0)
	f.Add(int64(0), "", 0, 0) // zero interval must be terminal, never nextAt==now
	f.Add(int64(-5), "", 3, 1)
	f.Add(int64(0), "*/5 * * * *", 0, 0)

	f.Fuzz(func(t *testing.T, intervalSec int64, cron string, maxOcc, occCount int) {
		var r *Recurrence
		if cron != "" {
			// Only use cron that the validator would accept, mirroring production.
			if _, err := nextCronTime(cron, time.Now()); err != nil {
				return
			}
			r = &Recurrence{Type: RecurrenceCron, Cron: cron, MaxOccurrences: maxOcc}
		} else {
			r = &Recurrence{Type: RecurrenceInterval, Interval: time.Duration(intervalSec) * time.Second, MaxOccurrences: maxOcc}
		}
		msg := &Message{Recurrence: r, OccurrenceCount: occCount}
		now := time.Now()

		out := computeNextSchedule(msg, now)
		if !out.terminal && !out.nextAt.After(now) {
			t.Fatalf("non-terminal outcome did not advance: nextAt=%v now=%v recurrence=%+v", out.nextAt, now, r)
		}
	})
}

// FuzzDecodeRecurrence ensures decoding arbitrary stored column values never
// panics and that a successfully decoded value re-encodes to the same type.
func FuzzDecodeRecurrence(f *testing.F) {
	f.Add("interval", "1h", 5, false)
	f.Add("cron", "0 9 * * *", 0, true)
	f.Add("", "", 0, false)
	f.Add("garbage", "???", -1, true)

	f.Fuzz(func(t *testing.T, typ, val string, maxOcc int, hasUntil bool) {
		recType := sql.NullString{String: typ, Valid: typ != ""}
		recVal := sql.NullString{String: val, Valid: val != ""}
		mo := sql.NullInt32{Int32: int32(maxOcc), Valid: true}
		until := sql.NullTime{Time: time.Now(), Valid: hasUntil}

		r := decodeRecurrence(recType, recVal, mo, until)
		if r == nil {
			return
		}
		// A decoded recurrence must re-encode to the same type string.
		rt, _, _, _ := encodeRecurrence(r)
		if rt == nil || *rt != string(r.Type) {
			t.Fatalf("re-encode type mismatch: decoded %q, re-encoded %v", r.Type, rt)
		}
	})
}

// FuzzMongoCodecRoundTrip exercises the MongoDB decode boundary: a stored
// document's recurrence_type/recurrence_value (including a malformed duration)
// is decoded by mongoMessage.toMessage, which must never panic.
func FuzzMongoCodecRoundTrip(f *testing.F) {
	f.Add("e", "interval", "1h", 3, 1)
	f.Add("e", "cron", "0 9 * * *", 0, 0)
	f.Add("e", "interval", "not-a-duration", 0, 0)
	f.Add("", "garbage", "", -1, 7)

	f.Fuzz(func(t *testing.T, event, recType, recVal string, maxOcc, occ int) {
		mm := &mongoMessage{
			ID:              "x",
			EventName:       event,
			RecurrenceType:  recType,
			RecurrenceValue: recVal,
			MaxOccurrences:  maxOcc,
			OccurrenceCount: occ,
		}
		got := mm.toMessage() // must not panic on any stored value
		if got.EventName != event {
			t.Fatalf("event not preserved: %q", got.EventName)
		}
		// A recognized recurrence type must survive decoding with its value.
		switch RecurrenceType(recType) {
		case RecurrenceInterval:
			if got.Recurrence == nil {
				t.Fatalf("recurrence dropped for type %q", recType)
			}
			// Interval parses recVal as a duration; a valid one must round-trip.
			if d, err := time.ParseDuration(recVal); err == nil && got.Recurrence.Interval != d {
				t.Fatalf("interval not preserved: got %v want %v", got.Recurrence.Interval, d)
			}
		case RecurrenceCron:
			if got.Recurrence == nil || got.Recurrence.Cron != recVal {
				t.Fatalf("cron value not preserved: %+v", got.Recurrence)
			}
		}
	})
}

// FuzzHandleDeliveryFailure fuzzes the retry/DLQ decision boundary and asserts
// its invariants: a message at or beyond the retry limit goes to the DLQ;
// otherwise the retry count increments by exactly one.
func FuzzHandleDeliveryFailure(f *testing.F) {
	f.Add(0, 3)
	f.Add(5, 5)
	f.Add(2, 0) // maxRetries=0 => infinite
	f.Add(10, 3)

	f.Fuzz(func(t *testing.T, retryCount, maxRetries int) {
		if retryCount < 0 || maxRetries < 0 {
			return
		}
		opts := defaultOptions()
		opts.maxRetries = maxRetries
		msg := &Message{ID: "m", EventName: "e", RetryCount: retryCount}

		d := handleDeliveryFailure(context.Background(), opts, msg, errors.New("boom"), discardLogger())

		atLimit := maxRetries > 0 && retryCount >= maxRetries
		if atLimit {
			if !d.sendToDLQ {
				t.Fatalf("retryCount=%d maxRetries=%d: expected DLQ", retryCount, maxRetries)
			}
		} else {
			if d.sendToDLQ {
				t.Fatalf("retryCount=%d maxRetries=%d: unexpected DLQ", retryCount, maxRetries)
			}
			if d.retryCount != retryCount+1 {
				t.Fatalf("retryCount=%d: expected increment to %d, got %d", retryCount, retryCount+1, d.retryCount)
			}
		}
	})
}
