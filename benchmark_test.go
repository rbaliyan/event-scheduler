package scheduler

import (
	"context"
	"errors"
	"strconv"
	"testing"
	"time"

	"github.com/alicebob/miniredis/v2"
	"github.com/rbaliyan/event/v3/transport"
	"github.com/redis/go-redis/v9"
)

// benchNoopTransport is a zero-allocation transport for isolating the cost of
// the code under benchmark from any transport work.
type benchNoopTransport struct{}

func (benchNoopTransport) Publish(context.Context, string, transport.Message) error { return nil }
func (benchNoopTransport) RegisterEvent(context.Context, string) error              { return nil }
func (benchNoopTransport) UnregisterEvent(context.Context, string) error            { return nil }
func (benchNoopTransport) Subscribe(context.Context, string, ...transport.SubscribeOption) (transport.Subscription, error) {
	return nil, errors.New("not implemented")
}
func (benchNoopTransport) Close(context.Context) error { return nil }

// benchEpoch is a fixed clock for pure benchmarks so absolute numbers are
// bit-stable across runs (Date.now() is unavailable here anyway).
var benchEpoch = time.Unix(1700000000, 0)

// --- Pure decision-logic benchmarks (no I/O, deterministic) ---

func BenchmarkComputeNextSchedule_Interval(b *testing.B) {
	msg := &Message{Recurrence: &Recurrence{Type: RecurrenceInterval, Interval: time.Hour}}
	b.ReportAllocs()
	for b.Loop() {
		_ = computeNextSchedule(msg, benchEpoch)
	}
}

func BenchmarkComputeNextSchedule_Cron(b *testing.B) {
	// Exposes the per-call cron.ParseStandard cost on the recurring hot path.
	msg := &Message{Recurrence: &Recurrence{Type: RecurrenceCron, Cron: "*/5 * * * *"}}
	b.ReportAllocs()
	for b.Loop() {
		_ = computeNextSchedule(msg, benchEpoch)
	}
}

func BenchmarkMongoCodecRoundTrip(b *testing.B) {
	// Backend codec parity with the Redis JSON path: BSON struct build + decode.
	msg := &Message{
		ID: "m", EventName: "orders.created", Payload: []byte("p"),
		Metadata:   map[string]string{"k": "v"},
		Recurrence: &Recurrence{Type: RecurrenceInterval, Interval: time.Hour, MaxOccurrences: 5},
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = fromMessage(msg).toMessage()
	}
}

func BenchmarkHandleDeliveryFailure_Retry(b *testing.B) {
	opts := defaultOptions()
	logger := discardLogger()
	pubErr := errors.New("boom")
	ctx := context.Background()
	msg := &Message{ID: "m", EventName: "e"}
	b.ReportAllocs()
	for b.Loop() {
		_ = handleDeliveryFailure(ctx, opts, msg, pubErr, logger)
	}
}

func BenchmarkHandleDeliveryFailure_Backoff(b *testing.B) {
	opts := defaultOptions()
	opts.backoff = &mockBackoff{delay: time.Second}
	logger := discardLogger()
	pubErr := errors.New("boom")
	ctx := context.Background()
	msg := &Message{ID: "m", EventName: "e", RetryCount: 3}
	b.ReportAllocs()
	for b.Loop() {
		_ = handleDeliveryFailure(ctx, opts, msg, pubErr, logger)
	}
}

func BenchmarkValidateRecurrence_Cron(b *testing.B) {
	r := &Recurrence{Type: RecurrenceCron, Cron: "0 9 * * 1-5"}
	b.ReportAllocs()
	for b.Loop() {
		_ = validateRecurrence(r)
	}
}

func BenchmarkEncodeDecodeRecurrence(b *testing.B) {
	r := &Recurrence{Type: RecurrenceInterval, Interval: 90 * time.Minute, MaxOccurrences: 5}
	b.ReportAllocs()
	for b.Loop() {
		rt, rv, mo, until := encodeRecurrence(r)
		_ = rt
		_ = rv
		_ = mo
		_ = until
	}
}

func BenchmarkAdaptivePollAdjust(b *testing.B) {
	s := newAdaptivePollState(100*time.Millisecond, 10*time.Millisecond, 5*time.Second)
	b.ReportAllocs()
	i := 0
	for b.Loop() {
		_ = s.adjust(i % 2) // alternate busy/idle
		i++
	}
}

func BenchmarkPublishScheduledMessage(b *testing.B) {
	t := benchNoopTransport{}
	ctx := context.Background()
	msg := &Message{
		ID: "m", EventName: "orders.created",
		Payload:  []byte(`{"order_id":"12345","amount":99.99}`),
		Metadata: map[string]string{"source": "checkout", "region": "us-east"},
	}
	b.ReportAllocs()
	for b.Loop() {
		_ = publishScheduledMessage(ctx, t, msg)
	}
}

// --- Redis backend benchmarks via in-memory miniredis (no Docker) ---
//
// miniredis measures serialization + the Lua claim logic, not real network
// RTT, so absolute numbers are a lower bound — use them for regression deltas.

func benchRedis(b *testing.B) *RedisScheduler {
	b.Helper()
	mr, err := miniredis.Run()
	if err != nil {
		b.Fatalf("miniredis.Run: %v", err)
	}
	b.Cleanup(mr.Close)
	client := redis.NewClient(&redis.Options{Addr: mr.Addr()})
	b.Cleanup(func() { _ = client.Close() })
	s, err := NewRedisScheduler(client, benchNoopTransport{})
	if err != nil {
		b.Fatalf("NewRedisScheduler: %v", err)
	}
	return s
}

func BenchmarkRedisSchedule(b *testing.B) {
	for _, size := range []int{64, 1024, 16384} {
		b.Run(strconv.Itoa(size)+"B", func(b *testing.B) {
			s := benchRedis(b)
			ctx := context.Background()
			payload := make([]byte, size)
			b.ReportAllocs()
			i := 0
			for b.Loop() {
				_ = s.Schedule(ctx, Message{
					ID: strconv.Itoa(i), EventName: "e", Payload: payload,
					ScheduledAt: time.Now().Add(time.Hour),
				})
				i++
			}
		})
	}
}

func BenchmarkRedisProcessDue(b *testing.B) {
	s := benchRedis(b)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		// Seed a batch of due messages, then measure one processDue pass.
		for i := range 100 {
			_ = s.Schedule(ctx, Message{
				ID: "due-" + strconv.Itoa(i), EventName: "e", Payload: []byte("p"),
				ScheduledAt: time.Now().Add(-time.Second),
			})
		}
		b.StartTimer()
		s.processDue(ctx)
		b.ReportMetric(100, "msgs/op") // each pass drains the 100-message batch
	}
}

func BenchmarkRedisClaim(b *testing.B) {
	s := benchRedis(b)
	ctx := context.Background()
	b.ReportAllocs()
	for b.Loop() {
		b.StopTimer()
		_ = s.Schedule(ctx, Message{
			ID: "c", EventName: "e", Payload: []byte("p"),
			ScheduledAt: time.Now().Add(-time.Second),
		})
		b.StartTimer()
		_, _, _ = s.claimDueMessage(ctx, time.Now().Unix())
	}
}

// BenchmarkRedisClaim_Parallel measures concurrent claim contention — the
// real HA pattern where multiple scheduler instances claim against one store.
func BenchmarkRedisClaim_Parallel(b *testing.B) {
	s := benchRedis(b)
	ctx := context.Background()
	const seed = 5000
	for i := range seed {
		_ = s.Schedule(ctx, Message{
			ID: strconv.Itoa(i), EventName: "e", Payload: []byte("p"),
			ScheduledAt: time.Now().Add(-time.Second),
		})
	}
	b.ReportAllocs()
	b.ResetTimer() // exclude seeding from the measurement
	b.RunParallel(func(pb *testing.PB) {
		for pb.Next() {
			_, _, _ = s.claimDueMessage(ctx, time.Now().Unix())
		}
	})
}
