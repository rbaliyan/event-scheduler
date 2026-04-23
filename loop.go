package scheduler

import (
	"log/slog"
	"time"
)

// runSchedulerLoop runs the shared ticker-based polling loop for all scheduler backends.
//
// It creates tickers for processing and (optionally) recovery, and manages adaptive
// polling if enabled. The loop runs until stopCh or doneCh fires.
//
// Parameters:
//   - logger: structured logger for startup messages
//   - opts: scheduler options (pollInterval, adaptive polling config, etc.)
//   - doneCh: context.Done() channel; fires when the parent context is cancelled
//   - stopCh: stop channel; fires when Stop() is called
//   - processFn: called on each process tick; returns the number of messages processed
//   - recoverFn: called on each recovery tick; nil to disable recovery (e.g., PostgreSQL)
//   - notifyCh: optional channel for push-based wake-ups (e.g., PG LISTEN/NOTIFY); nil disables
//   - onExit: called once when the loop exits (e.g., close(stoppedCh))
func runSchedulerLoop(
	logger *slog.Logger,
	opts *options,
	doneCh <-chan struct{},
	stopCh <-chan struct{},
	processFn func() int,
	recoverFn func(),
	notifyCh <-chan struct{},
	onExit func(),
) {
	defer onExit()

	currentInterval := opts.pollInterval
	ticker := time.NewTicker(currentInterval)
	defer ticker.Stop()

	// Initialize adaptive polling state if enabled
	var adaptiveState *adaptivePollState
	if opts.adaptivePolling {
		adaptiveState = newAdaptivePollState(
			opts.pollInterval,
			opts.minPollInterval,
			opts.maxPollInterval,
		)
		logger.Info("scheduler started with adaptive polling",
			"initial_interval", opts.pollInterval,
			"min_interval", opts.minPollInterval,
			"max_interval", opts.maxPollInterval,
			"batch_size", opts.batchSize)
	} else {
		logger.Info("scheduler started",
			"poll_interval", opts.pollInterval,
			"batch_size", opts.batchSize)
	}

	// Set up optional recovery ticker
	var recoveryC <-chan time.Time
	if recoverFn != nil {
		recoveryTicker := time.NewTicker(time.Minute)
		defer recoveryTicker.Stop()
		recoveryC = recoveryTicker.C

		// Recover any stuck messages at startup
		recoverFn()
	}

	process := func() {
		processed := processFn()

		// Adjust poll interval if adaptive polling is enabled
		if adaptiveState != nil {
			newInterval := adaptiveState.adjust(processed)
			if newInterval != currentInterval {
				currentInterval = newInterval
				ticker.Reset(currentInterval)
			}
		}
	}

	for {
		select {
		case <-doneCh:
			return
		case <-stopCh:
			return
		case <-notifyCh:
			process()
		case <-ticker.C:
			process()
		case <-recoveryC:
			recoverFn()
		}
	}
}
