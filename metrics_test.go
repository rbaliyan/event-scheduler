package scheduler

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/metric/metricdata"
)

// newTestMetrics builds a Metrics backed by a ManualReader so tests can collect
// and assert on the recorded instruments without a real exporter.
func newTestMetrics(t *testing.T, opts ...MetricsOption) (*Metrics, *metric.ManualReader) {
	t.Helper()
	reader := metric.NewManualReader()
	provider := metric.NewMeterProvider(metric.WithReader(reader))
	all := append([]MetricsOption{WithMeterProvider(provider)}, opts...)
	m, err := NewMetrics(all...)
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	t.Cleanup(func() { _ = m.Close() })
	return m, reader
}

// collect gathers all metrics and returns them keyed by instrument name.
func collect(t *testing.T, reader *metric.ManualReader) map[string]metricdata.Metrics {
	t.Helper()
	var rm metricdata.ResourceMetrics
	if err := reader.Collect(context.Background(), &rm); err != nil {
		t.Fatalf("Collect: %v", err)
	}
	out := make(map[string]metricdata.Metrics)
	for _, sm := range rm.ScopeMetrics {
		for _, m := range sm.Metrics {
			out[m.Name] = m
		}
	}
	return out
}

func sumInt64(t *testing.T, m metricdata.Metrics) int64 {
	t.Helper()
	s, ok := m.Data.(metricdata.Sum[int64])
	if !ok {
		t.Fatalf("metric %q is not an int64 sum (%T)", m.Name, m.Data)
	}
	var total int64
	for _, dp := range s.DataPoints {
		total += dp.Value
	}
	return total
}

func TestNewMetrics_Defaults(t *testing.T) {
	t.Parallel()
	m, err := NewMetrics()
	if err != nil {
		t.Fatalf("NewMetrics with default provider: %v", err)
	}
	defer func() { _ = m.Close() }()
	if m == nil {
		t.Fatal("expected non-nil metrics")
	}
}

func TestMetrics_Counters(t *testing.T) {
	t.Parallel()
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	m.RecordScheduled(ctx, "orders")
	m.RecordScheduled(ctx, "orders")
	m.RecordDelivered(ctx, "orders", time.Now().Add(-time.Second), time.Now())
	m.RecordFailed(ctx, "orders", "publish_error")
	m.RecordCancelled(ctx, "orders")
	m.RecordCancelled(ctx, "") // no-event variant must not panic
	m.RecordRecovered(ctx, 3)
	m.RecordRecovered(ctx, 0) // zero must be a no-op
	m.RecordDLQSent(ctx, "orders")
	m.RecordRescheduled(ctx, "orders")

	got := collect(t, reader)
	cases := map[string]int64{
		"scheduled_messages_total":             2,
		"scheduled_messages_delivered_total":   1,
		"scheduled_messages_failed_total":      1,
		"scheduled_messages_cancelled_total":   2,
		"scheduled_messages_recovered_total":   3,
		"scheduled_messages_dlq_total":         1,
		"scheduled_messages_rescheduled_total": 1,
	}
	for name, want := range cases {
		m, ok := got[name]
		if !ok {
			t.Errorf("metric %q not recorded", name)
			continue
		}
		if total := sumInt64(t, m); total != want {
			t.Errorf("%s = %d, want %d", name, total, want)
		}
	}
}

func TestMetrics_Histograms(t *testing.T) {
	t.Parallel()
	m, reader := newTestMetrics(t)
	ctx := context.Background()

	m.RecordDelivered(ctx, "orders", time.Now().Add(-2*time.Second), time.Now().Add(-100*time.Millisecond))

	got := collect(t, reader)
	for _, name := range []string{"schedule_delivery_delay_seconds", "schedule_processing_duration_seconds"} {
		hm, ok := got[name]
		if !ok {
			t.Errorf("histogram %q not recorded", name)
			continue
		}
		h, ok := hm.Data.(metricdata.Histogram[float64])
		if !ok {
			t.Errorf("%q is not a float64 histogram (%T)", name, hm.Data)
			continue
		}
		if len(h.DataPoints) == 0 || h.DataPoints[0].Count != 1 {
			t.Errorf("%q expected 1 observation, got %+v", name, h.DataPoints)
		}
	}
}

func TestMetrics_Namespace(t *testing.T) {
	t.Parallel()
	m, reader := newTestMetrics(t, WithNamespace("orders"))
	m.RecordScheduled(context.Background(), "e")

	got := collect(t, reader)
	if _, ok := got["orders_scheduled_messages_total"]; !ok {
		t.Errorf("expected namespaced metric orders_scheduled_messages_total, got names %v", keys(got))
	}
}

func TestMetrics_ObservableGauges(t *testing.T) {
	t.Parallel()
	m, reader := newTestMetrics(t)
	m.SetPendingCallback(func() int64 { return 7 })
	m.SetStuckCallback(func() int64 { return 2 })

	got := collect(t, reader)
	for name, want := range map[string]int64{
		"scheduled_messages_pending": 7,
		"scheduled_messages_stuck":   2,
	} {
		gm, ok := got[name]
		if !ok {
			t.Errorf("gauge %q not collected", name)
			continue
		}
		g, ok := gm.Data.(metricdata.Gauge[int64])
		if !ok {
			t.Errorf("%q is not an int64 gauge (%T)", name, gm.Data)
			continue
		}
		if len(g.DataPoints) == 0 || g.DataPoints[0].Value != want {
			t.Errorf("%q = %+v, want value %d", name, g.DataPoints, want)
		}
	}
}

func TestMetrics_NilSafe(t *testing.T) {
	t.Parallel()
	var m *Metrics // nil receiver — all methods must be safe
	ctx := context.Background()
	m.RecordScheduled(ctx, "e")
	m.RecordDelivered(ctx, "e", time.Now(), time.Now())
	m.RecordFailed(ctx, "e", "r")
	m.RecordCancelled(ctx, "e")
	m.RecordRecovered(ctx, 1)
	m.RecordDLQSent(ctx, "e")
	m.RecordRescheduled(ctx, "e")
	m.SetPendingCallback(func() int64 { return 0 })
	m.SetStuckCallback(func() int64 { return 0 })
	if err := m.Close(); err != nil {
		t.Errorf("nil Close() = %v, want nil", err)
	}
}

func keys(m map[string]metricdata.Metrics) []string {
	out := make([]string, 0, len(m))
	for k := range m {
		out = append(out, k)
	}
	return out
}
