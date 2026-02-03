package scheduler

import (
	"context"
	"time"

	"github.com/rbaliyan/event/v3/transport"
	"github.com/rbaliyan/event/v3/transport/message"
	"go.opentelemetry.io/otel/trace"
)

// publishScheduledMessage publishes a scheduled message to a transport.
// It adds scheduler metadata (scheduled_message_id, scheduled_at) to the message.
func publishScheduledMessage(ctx context.Context, t transport.Transport, msg *Message) error {
	metadata := make(map[string]string)
	for k, v := range msg.Metadata {
		metadata[k] = v
	}
	metadata["scheduled_message_id"] = msg.ID
	metadata["scheduled_at"] = msg.ScheduledAt.Format(time.RFC3339)

	// Propagate trace context from the current span
	var opts []message.Option
	if sc := trace.SpanContextFromContext(ctx); sc.IsValid() {
		opts = append(opts, message.WithSpanContext(sc))
	}

	transportMsg := message.New(
		msg.ID,
		"scheduler",
		msg.Payload,
		metadata,
		opts...,
	)

	return t.Publish(ctx, msg.EventName, transportMsg)
}
