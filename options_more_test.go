package scheduler

import (
	"io"
	"log/slog"
	"testing"
	"time"
)

// TestOptions_Table covers the option functions not exercised elsewhere, in one
// parameterized pass.
func TestOptions_Table(t *testing.T) {
	t.Parallel()

	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	metrics, err := NewMetrics()
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}

	cases := []struct {
		name string
		opt  Option
		ok   func(*options) bool
	}{
		{"adaptive", WithAdaptivePolling(true), func(o *options) bool { return o.adaptivePolling }},
		{"min-poll", WithMinPollInterval(7 * time.Millisecond), func(o *options) bool { return o.minPollInterval == 7*time.Millisecond }},
		{"min-poll-ignores-zero", WithMinPollInterval(0), func(o *options) bool { return o.minPollInterval == defaultOptions().minPollInterval }},
		{"max-poll", WithMaxPollInterval(9 * time.Second), func(o *options) bool { return o.maxPollInterval == 9*time.Second }},
		{"stuck", WithStuckDuration(3 * time.Minute), func(o *options) bool { return o.stuckDuration == 3*time.Minute }},
		{"stuck-ignores-zero", WithStuckDuration(0), func(o *options) bool { return o.stuckDuration == defaultOptions().stuckDuration }},
		{"logger", WithLogger(logger), func(o *options) bool { return o.logger == logger }},
		{"logger-ignores-nil", WithLogger(nil), func(o *options) bool { return o.logger != nil }},
		{"metrics", WithMetrics(metrics), func(o *options) bool { return o.metrics == metrics }},
		{"metrics-ignores-nil", WithMetrics(nil), func(o *options) bool { return o.metrics == nil }},
		{"notify-ignores-nil", WithNotifyListener(nil, "ch"), func(o *options) bool { return o.listener == nil }},
	}

	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			t.Parallel()
			o := defaultOptions()
			c.opt(o)
			if !c.ok(o) {
				t.Errorf("option %q did not apply as expected", c.name)
			}
		})
	}
}
