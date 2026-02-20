package scheduler

import (
	"context"
	"sync"
	"time"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

// Metrics provides OpenTelemetry metrics for scheduled message operations.
// Metrics are optional and can be enabled by calling WithMetrics() on a scheduler.
//
// Available metrics:
//   - scheduled_messages_total: Counter of total messages scheduled
//   - scheduled_messages_delivered_total: Counter of messages successfully delivered
//   - scheduled_messages_failed_total: Counter of messages that failed delivery
//   - scheduled_messages_pending: Gauge of current pending scheduled messages
//   - scheduled_messages_stuck: Gauge of current stuck messages being recovered
//   - schedule_delivery_delay_seconds: Histogram of delay between scheduled and actual delivery
//   - schedule_processing_duration_seconds: Histogram of time to process and deliver a message
//
// Example:
//
//	metrics := scheduler.NewMetrics()
//	scheduler := scheduler.NewRedisScheduler(client, transport).WithMetrics(metrics)
type Metrics struct {
	meter metric.Meter

	// Counters
	scheduledTotal metric.Int64Counter
	deliveredTotal metric.Int64Counter
	failedTotal    metric.Int64Counter
	cancelledTotal metric.Int64Counter
	recoveredTotal metric.Int64Counter
	dlqSentTotal   metric.Int64Counter

	// Gauges (using UpDownCounter for gauge-like behavior)
	pendingMessages metric.Int64ObservableGauge
	stuckMessages   metric.Int64ObservableGauge

	// Histograms
	deliveryDelay      metric.Float64Histogram
	processingDuration metric.Float64Histogram

	// Callbacks for observable gauges
	pendingCallback func() int64
	stuckCallback   func() int64

	// Registration for cleanup
	registration metric.Registration
	mu           sync.RWMutex
}

// MetricsOption configures the Metrics instance.
type MetricsOption func(*metricsOptions)

type metricsOptions struct {
	meterProvider metric.MeterProvider
	namespace     string
}

// WithMeterProvider sets a custom meter provider for metrics.
// By default, uses the global OpenTelemetry meter provider.
func WithMeterProvider(provider metric.MeterProvider) MetricsOption {
	return func(o *metricsOptions) {
		if provider != nil {
			o.meterProvider = provider
		}
	}
}

// WithNamespace sets a namespace prefix for all metrics.
// This is useful for distinguishing metrics from different scheduler instances.
//
// Example:
//
//	metrics := scheduler.NewMetrics(scheduler.WithNamespace("orders"))
//	// Metrics will be: orders_scheduled_messages_total, etc.
func WithNamespace(namespace string) MetricsOption {
	return func(o *metricsOptions) {
		if namespace != "" {
			o.namespace = namespace + "_"
		}
	}
}

// NewMetrics creates a new Metrics instance for recording scheduler metrics.
//
// By default, uses the global OpenTelemetry meter provider. Use WithMeterProvider
// to specify a custom provider.
//
// Example:
//
//	// Using global provider
//	metrics := scheduler.NewMetrics()
//
//	// Using custom provider
//	metrics := scheduler.NewMetrics(scheduler.WithMeterProvider(myProvider))
//
//	// With namespace
//	metrics := scheduler.NewMetrics(scheduler.WithNamespace("orders"))
func NewMetrics(opts ...MetricsOption) (*Metrics, error) {
	o := &metricsOptions{
		meterProvider: otel.GetMeterProvider(),
	}
	for _, opt := range opts {
		opt(o)
	}

	meter := o.meterProvider.Meter("github.com/rbaliyan/event-scheduler")
	prefix := o.namespace

	m := &Metrics{
		meter: meter,
	}

	var err error

	// Create counters
	m.scheduledTotal, err = meter.Int64Counter(
		prefix+"scheduled_messages_total",
		metric.WithDescription("Total number of messages scheduled"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.deliveredTotal, err = meter.Int64Counter(
		prefix+"scheduled_messages_delivered_total",
		metric.WithDescription("Total number of messages successfully delivered"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.failedTotal, err = meter.Int64Counter(
		prefix+"scheduled_messages_failed_total",
		metric.WithDescription("Total number of messages that failed delivery"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.cancelledTotal, err = meter.Int64Counter(
		prefix+"scheduled_messages_cancelled_total",
		metric.WithDescription("Total number of messages cancelled before delivery"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.recoveredTotal, err = meter.Int64Counter(
		prefix+"scheduled_messages_recovered_total",
		metric.WithDescription("Total number of stuck messages recovered"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.dlqSentTotal, err = meter.Int64Counter(
		prefix+"scheduled_messages_dlq_total",
		metric.WithDescription("Total number of messages sent to dead-letter queue"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	// Create histograms
	m.deliveryDelay, err = meter.Float64Histogram(
		prefix+"schedule_delivery_delay_seconds",
		metric.WithDescription("Delay between scheduled time and actual delivery time"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10, 30, 60),
	)
	if err != nil {
		return nil, err
	}

	m.processingDuration, err = meter.Float64Histogram(
		prefix+"schedule_processing_duration_seconds",
		metric.WithDescription("Time to process and deliver a scheduled message"),
		metric.WithUnit("s"),
		metric.WithExplicitBucketBoundaries(0.001, 0.005, 0.01, 0.025, 0.05, 0.1, 0.25, 0.5, 1, 2.5, 5, 10),
	)
	if err != nil {
		return nil, err
	}

	// Create observable gauges
	m.pendingMessages, err = meter.Int64ObservableGauge(
		prefix+"scheduled_messages_pending",
		metric.WithDescription("Current number of pending scheduled messages"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	m.stuckMessages, err = meter.Int64ObservableGauge(
		prefix+"scheduled_messages_stuck",
		metric.WithDescription("Current number of stuck messages being recovered"),
		metric.WithUnit("{message}"),
	)
	if err != nil {
		return nil, err
	}

	// Register callbacks for observable gauges
	m.registration, err = meter.RegisterCallback(
		func(ctx context.Context, o metric.Observer) error {
			m.mu.RLock()
			defer m.mu.RUnlock()

			if m.pendingCallback != nil {
				o.ObserveInt64(m.pendingMessages, m.pendingCallback())
			}
			if m.stuckCallback != nil {
				o.ObserveInt64(m.stuckMessages, m.stuckCallback())
			}
			return nil
		},
		m.pendingMessages,
		m.stuckMessages,
	)
	if err != nil {
		return nil, err
	}

	return m, nil
}

// SetPendingCallback sets the callback function for the pending messages gauge.
// The callback is called on each metrics collection to get the current count.
//
// Example:
//
//	metrics.SetPendingCallback(func() int64 {
//	    count, _ := scheduler.CountPending(ctx)
//	    return count
//	})
func (m *Metrics) SetPendingCallback(fn func() int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.pendingCallback = fn
}

// SetStuckCallback sets the callback function for the stuck messages gauge.
// The callback is called on each metrics collection to get the current count.
func (m *Metrics) SetStuckCallback(fn func() int64) {
	if m == nil {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.stuckCallback = fn
}

// RecordScheduled records that a message was scheduled.
//
// Parameters:
//   - ctx: context for the operation
//   - eventName: the event name for the scheduled message
func (m *Metrics) RecordScheduled(ctx context.Context, eventName string) {
	if m == nil {
		return
	}
	m.scheduledTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event", eventName),
	))
}

// RecordDelivered records that a message was successfully delivered.
//
// Parameters:
//   - ctx: context for the operation
//   - eventName: the event name of the delivered message
//   - scheduledAt: when the message was originally scheduled for
//   - processingStart: when processing of this message started
func (m *Metrics) RecordDelivered(ctx context.Context, eventName string, scheduledAt time.Time, processingStart time.Time) {
	if m == nil {
		return
	}
	now := time.Now()

	m.deliveredTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event", eventName),
	))

	// Record delivery delay (time from scheduled time to actual delivery)
	deliveryDelay := now.Sub(scheduledAt).Seconds()
	if deliveryDelay < 0 {
		deliveryDelay = 0 // Shouldn't happen, but protect against clock issues
	}
	m.deliveryDelay.Record(ctx, deliveryDelay, metric.WithAttributes(
		attribute.String("event", eventName),
	))

	// Record processing duration (time from claim to delivery)
	processingDuration := now.Sub(processingStart).Seconds()
	if processingDuration < 0 {
		processingDuration = 0
	}
	m.processingDuration.Record(ctx, processingDuration, metric.WithAttributes(
		attribute.String("event", eventName),
	))
}

// RecordFailed records that a message failed to be delivered.
//
// Parameters:
//   - ctx: context for the operation
//   - eventName: the event name of the failed message
//   - reason: short description of the failure reason
func (m *Metrics) RecordFailed(ctx context.Context, eventName string, reason string) {
	if m == nil {
		return
	}
	m.failedTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event", eventName),
		attribute.String("reason", reason),
	))
}

// RecordCancelled records that a message was cancelled before delivery.
//
// Parameters:
//   - ctx: context for the operation
//   - eventName: the event name of the cancelled message (empty string if unknown)
func (m *Metrics) RecordCancelled(ctx context.Context, eventName string) {
	if m == nil {
		return
	}
	attrs := []attribute.KeyValue{}
	if eventName != "" {
		attrs = append(attrs, attribute.String("event", eventName))
	}
	m.cancelledTotal.Add(ctx, 1, metric.WithAttributes(attrs...))
}

// RecordRecovered records that stuck messages were recovered.
//
// Parameters:
//   - ctx: context for the operation
//   - count: number of messages recovered
func (m *Metrics) RecordRecovered(ctx context.Context, count int64) {
	if m == nil {
		return
	}
	if count > 0 {
		m.recoveredTotal.Add(ctx, count)
	}
}

// RecordDLQSent records that a message was sent to the dead-letter queue.
func (m *Metrics) RecordDLQSent(ctx context.Context, eventName string) {
	if m == nil {
		return
	}
	m.dlqSentTotal.Add(ctx, 1, metric.WithAttributes(
		attribute.String("event", eventName),
	))
}

// Close unregisters the metrics callbacks.
// Call this when the scheduler is stopped to clean up resources.
func (m *Metrics) Close() error {
	if m == nil {
		return nil
	}
	if m.registration != nil {
		return m.registration.Unregister()
	}
	return nil
}
