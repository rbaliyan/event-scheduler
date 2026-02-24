package scheduler

import (
	"context"
	"log/slog"
	"time"
)

// retryDecision contains the result of evaluating a delivery failure.
// The caller is responsible for the actual storage operation (reschedule or delete).
type retryDecision struct {
	// sendToDLQ is true when the message has exhausted all retries.
	// The caller should delete/discard the message from storage.
	sendToDLQ bool

	// nextRetryAt is the time to reschedule the message for retry.
	// Only meaningful when sendToDLQ is false.
	nextRetryAt time.Time

	// retryCount is the new retry count after incrementing.
	// Only meaningful when sendToDLQ is false.
	retryCount int
}

// handleDeliveryFailure processes a failed message delivery.
// It determines whether to retry (with backoff) or send to DLQ,
// records metrics, and returns the retry decision.
//
// The caller is responsible for the actual storage update (reschedule or delete).
func handleDeliveryFailure(ctx context.Context, opts *options, msg *Message, publishErr error, logger *slog.Logger) retryDecision {
	// Record failure metrics
	if opts.metrics != nil {
		opts.metrics.RecordFailed(ctx, msg.EventName, "publish_error")
	}

	// Check if max retries exceeded
	if opts.maxRetries > 0 && msg.RetryCount >= opts.maxRetries {
		logger.Error("scheduled message exceeded max retries, discarding",
			"id", msg.ID,
			"event", msg.EventName,
			"retry_count", msg.RetryCount,
			"max_retries", opts.maxRetries)

		// Send to DLQ if configured
		if opts.dlq != nil {
			if dlqErr := opts.dlq.Store(ctx, DLQStoreParams{
				EventName:  msg.EventName,
				OriginalID: msg.ID,
				Payload:    msg.Payload,
				Metadata:   msg.Metadata,
				Err:        publishErr,
				RetryCount: msg.RetryCount,
				Source:     "scheduler",
			}); dlqErr != nil {
				logger.Error("failed to send message to DLQ",
					"id", msg.ID,
					"error", dlqErr)
			} else if opts.metrics != nil {
				opts.metrics.RecordDLQSent(ctx, msg.EventName)
			}
		}

		return retryDecision{sendToDLQ: true}
	}

	// Increment retry count and calculate next retry time
	newRetryCount := msg.RetryCount + 1
	nextRetryAt := time.Now()
	if opts.backoff != nil {
		backoffDelay := opts.backoff.NextDelay(newRetryCount - 1)
		nextRetryAt = nextRetryAt.Add(backoffDelay)
		logger.Debug("scheduling retry with backoff",
			"id", msg.ID,
			"retry_count", newRetryCount,
			"backoff_delay", backoffDelay,
			"next_retry_at", nextRetryAt)
	}

	return retryDecision{
		sendToDLQ:   false,
		nextRetryAt: nextRetryAt,
		retryCount:  newRetryCount,
	}
}
