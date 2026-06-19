package service

import (
	"testing"
	"time"

	scheduler "github.com/rbaliyan/event-scheduler"
	schedulerpb "github.com/rbaliyan/event-scheduler/proto/scheduler/v1"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// FuzzProtoToFilter exercises the gateway/gRPC-facing decode of a ListRequest
// into a scheduler.Filter — an untrusted-input boundary that must never panic
// (including on extreme/overflow timestamps via AsTime) and must map fields
// faithfully.
func FuzzProtoToFilter(f *testing.F) {
	f.Add("orders.created", int32(100), int32(0), int64(0), int64(0))
	f.Add("", int32(-1), int32(-5), int64(1<<62), int64(-1))
	f.Add("x", int32(1<<30), int32(1<<30), int64(253402300799), int64(0))

	f.Fuzz(func(t *testing.T, eventName string, limit, offset int32, beforeSec, afterSec int64) {
		req := &schedulerpb.ListRequest{
			EventName: eventName,
			Limit:     limit,
			Offset:    offset,
			Before:    &timestamppb.Timestamp{Seconds: beforeSec},
			After:     &timestamppb.Timestamp{Seconds: afterSec},
		}
		filter := protoToFilter(req) // must not panic on any timestamp
		if filter.EventName != eventName {
			t.Fatalf("event name not mapped: %q", filter.EventName)
		}
		if filter.Limit != int(limit) || filter.Offset != int(offset) {
			t.Fatalf("limit/offset not mapped: %d/%d", filter.Limit, filter.Offset)
		}
	})
}

// FuzzMessageToProto ensures the domain->proto conversion never panics and
// preserves the scalar fields, across arbitrary recurrence shapes.
func FuzzMessageToProto(f *testing.F) {
	f.Add("id-1", "orders.created", []byte("p"), "interval", "1h", 3, 1)
	f.Add("", "", []byte(nil), "cron", "0 9 * * *", 0, 0)
	f.Add("x", "e", []byte{0xff}, "garbage", "", -1, 5)

	f.Fuzz(func(t *testing.T, id, event string, payload []byte, recType, recVal string, retry, occ int) {
		msg := &scheduler.Message{
			ID:              id,
			EventName:       event,
			Payload:         payload,
			ScheduledAt:     time.Now(),
			CreatedAt:       time.Now(),
			RetryCount:      retry,
			OccurrenceCount: occ,
		}
		switch scheduler.RecurrenceType(recType) {
		case scheduler.RecurrenceInterval:
			if d, err := time.ParseDuration(recVal); err == nil {
				msg.Recurrence = &scheduler.Recurrence{Type: scheduler.RecurrenceInterval, Interval: d}
			}
		case scheduler.RecurrenceCron:
			msg.Recurrence = &scheduler.Recurrence{Type: scheduler.RecurrenceCron, Cron: recVal}
		}

		pb := messageToProto(msg)
		if pb == nil {
			t.Fatal("messageToProto returned nil for non-nil message")
		}
		if pb.Id != id || pb.EventName != event {
			t.Fatalf("scalar fields not preserved: %+v", pb)
		}
		if int(pb.RetryCount) != retry || int(pb.OccurrenceCount) != occ {
			t.Fatalf("counts not preserved: retry=%d occ=%d", pb.RetryCount, pb.OccurrenceCount)
		}
	})
}
