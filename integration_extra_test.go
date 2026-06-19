//go:build integration

package scheduler

import (
	"context"
	"database/sql"
	"fmt"
	"testing"
	"time"

	"github.com/lib/pq"
	"github.com/rbaliyan/event/v3/health"
	sdkmetric "go.opentelemetry.io/otel/sdk/metric"
)

// --- PostgreSQL: stuck recovery (the heart of the 2-phase claim refactor) ---

func TestPostgres_Integration_StuckRecovery(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt, WithStuckDuration(time.Second))
	defer cleanup()
	ctx := context.Background()

	// Directly insert a row stuck in 'processing' with an old claimed_at,
	// simulating a crash between claim and finalize.
	_, err := sched.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, event_name, payload, metadata, scheduled_at, created_at, retry_count, status, claimed_at)
		VALUES ($1, $2, $3, $4, $5, $6, 0, 'processing', $7)`, sched.table),
		"pg-stuck-1", "stuck.event", []byte(`{"id":"pg-stuck-1"}`), []byte(`{}`),
		time.Now().Add(-time.Minute), time.Now().Add(-time.Minute), time.Now().Add(-5*time.Second))
	if err != nil {
		t.Fatalf("insert stuck row: %v", err)
	}

	cctx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = sched.Start(cctx) }()

	pub := mt.WaitForPublish(t, 15*time.Second)
	if pub.EventName != "stuck.event" {
		t.Errorf("expected recovered delivery of 'stuck.event', got %q", pub.EventName)
	}
}

func TestPostgres_Integration_CountStuck(t *testing.T) {
	sched, cleanup := setupPostgresScheduler(t, newMockTransport(), WithStuckDuration(time.Second))
	defer cleanup()
	ctx := context.Background()

	old := time.Now().Add(-10 * time.Second)
	for i := 0; i < 3; i++ {
		_, err := sched.db.ExecContext(ctx, fmt.Sprintf(`
			INSERT INTO %s (id, event_name, payload, scheduled_at, created_at, status, claimed_at)
			VALUES ($1, 'e', $2, $3, $3, 'processing', $4)`, sched.table),
			fmt.Sprintf("stuck-%d", i), []byte(`{}`), old, old)
		if err != nil {
			t.Fatalf("insert: %v", err)
		}
	}
	// One ordinary pending row.
	if err := sched.Schedule(ctx, makeMessage("pending-1", "e", time.Now().Add(time.Hour))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	stuck, err := sched.countStuck(ctx)
	if err != nil || stuck != 3 {
		t.Errorf("countStuck = %d, %v; want 3, nil", stuck, err)
	}
	pending, err := sched.countPending(ctx)
	if err != nil || pending != 1 {
		t.Errorf("countPending = %d, %v; want 1 (processing excluded), nil", pending, err)
	}
}

// --- Health checks against real backends ---

func TestPostgres_Integration_Health(t *testing.T) {
	sched, cleanup := setupPostgresScheduler(t, newMockTransport(), WithStuckDuration(time.Second))
	defer cleanup()
	ctx := context.Background()

	res := sched.Health(ctx)
	if res.Status != health.StatusHealthy {
		t.Errorf("fresh scheduler should be healthy, got %v (%s)", res.Status, res.Message)
	}

	// Insert a stuck row -> health degrades.
	old := time.Now().Add(-10 * time.Second)
	if _, err := sched.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, event_name, payload, scheduled_at, created_at, status, claimed_at)
		VALUES ('h-stuck', 'e', $1, $2, $2, 'processing', $3)`, sched.table),
		[]byte(`{}`), old, old); err != nil {
		t.Fatalf("insert: %v", err)
	}
	if res := sched.Health(ctx); res.Status != health.StatusDegraded {
		t.Errorf("expected degraded health with a stuck message, got %v", res.Status)
	}
}

func TestRedis_Integration_Health(t *testing.T) {
	sched, cleanup := setupRedisScheduler(t, newMockTransport())
	defer cleanup()
	res := sched.Health(context.Background())
	if res.Status != health.StatusHealthy {
		t.Errorf("expected healthy Redis scheduler, got %v (%s)", res.Status, res.Message)
	}
}

func TestMongo_Integration_Health(t *testing.T) {
	sched, cleanup := setupMongoScheduler(t, newMockTransport())
	defer cleanup()
	res := sched.Health(context.Background())
	if res.Status != health.StatusHealthy {
		t.Errorf("expected healthy Mongo scheduler, got %v (%s)", res.Status, res.Message)
	}
}

// --- PostgreSQL LISTEN/NOTIFY wakes the loop faster than the poll interval ---

func TestPostgres_Integration_NotifyWakesLoop(t *testing.T) {
	dsn := getPostgresDSN()
	listener := pq.NewListener(dsn, 10*time.Millisecond, time.Second, nil)
	defer func() { _ = listener.Close() }()

	mt := newMockTransport()
	// Long poll interval: only a NOTIFY can deliver promptly.
	sched, cleanup := setupPostgresScheduler(t, mt,
		WithPollInterval(30*time.Second),
		WithNotifyListener(listener, "scheduler_due_test"))
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	go func() { _ = sched.Start(ctx) }()

	// Give the listener a moment to subscribe, then schedule a due message.
	time.Sleep(200 * time.Millisecond)
	if err := sched.Schedule(ctx, makeMessage("notify-1", "notify.event", time.Now().Add(-time.Second))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// Delivered well within the 30s poll interval => NOTIFY woke the loop.
	pub := mt.WaitForPublish(t, 5*time.Second)
	if pub.EventName != "notify.event" {
		t.Errorf("expected 'notify.event', got %q", pub.EventName)
	}
}

// --- Metric assertions over a real delivery cycle ---

func TestPostgres_Integration_MetricsRecorded(t *testing.T) {
	reader := sdkmetric.NewManualReader()
	provider := sdkmetric.NewMeterProvider(sdkmetric.WithReader(reader))
	m, err := NewMetrics(WithMeterProvider(provider))
	if err != nil {
		t.Fatalf("NewMetrics: %v", err)
	}
	defer func() { _ = m.Close() }()

	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt, WithMetrics(m))
	defer cleanup()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if err := sched.Schedule(ctx, makeMessage("metric-1", "metric.event", time.Now().Add(-time.Second))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}
	go func() { _ = sched.Start(ctx) }()
	mt.WaitForPublish(t, 10*time.Second)

	// collect/sumInt64 are defined in metrics_test.go (same package).
	waitFor(t, 5*time.Second, func() bool {
		got := collect(t, reader)
		d, ok := got["scheduled_messages_delivered_total"]
		return ok && sumInt64(t, d) >= 1
	}, "delivered metric to be recorded")

	got := collect(t, reader)
	if s, ok := got["scheduled_messages_total"]; !ok || sumInt64(t, s) < 1 {
		t.Error("expected scheduled_messages_total >= 1")
	}
}

// --- Multi-instance: the claim primitive must not double-deliver ---

func TestPostgres_Integration_TwoSchedulersNoDoubleClaim(t *testing.T) {
	mt := newMockTransport()
	sched1, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()

	// A second scheduler on the SAME table with its own connection.
	db2, err := sql.Open("postgres", getPostgresDSN())
	if err != nil {
		t.Fatalf("open db2: %v", err)
	}
	defer func() { _ = db2.Close() }()
	sched2, err := NewPostgresScheduler(db2, mt, WithPollInterval(50*time.Millisecond), WithTable(sched1.table))
	if err != nil {
		t.Fatalf("NewPostgresScheduler #2: %v", err)
	}

	const m = 50
	ctx := context.Background()
	for i := 0; i < m; i++ {
		if err := sched1.Schedule(ctx, makeMessage(fmt.Sprintf("multi-%d", i), "multi.event", time.Now().Add(-time.Second))); err != nil {
			t.Fatalf("Schedule: %v", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = sched1.Start(runCtx) }()
	go func() { _ = sched2.Start(runCtx) }()

	pubs := mt.WaitForPublishes(t, m, 20*time.Second)
	assertUniqueDeliveries(t, pubs, m)

	// Let any erroneous duplicate settle, then confirm the total is exactly m.
	assertNoExtraPublish(t, mt, 300*time.Millisecond)
	if total := len(mt.Published()); total != m {
		t.Errorf("expected exactly %d deliveries across both schedulers, got %d (double-claim?)", m, total)
	}
}

func TestRedis_Integration_TwoSchedulersNoDoubleClaim(t *testing.T) {
	mt := newMockTransport()
	sched1, cleanup := setupRedisScheduler(t, mt)
	defer cleanup()

	// Second scheduler sharing the same client and key prefix.
	sched2, err := NewRedisScheduler(sched1.client, mt,
		WithPollInterval(50*time.Millisecond), WithKeyPrefix(sched1.opts.keyPrefix))
	if err != nil {
		t.Fatalf("NewRedisScheduler #2: %v", err)
	}

	const m = 50
	ctx := context.Background()
	for i := 0; i < m; i++ {
		if err := sched1.Schedule(ctx, makeMessage(fmt.Sprintf("rmulti-%d", i), "rmulti.event", time.Now().Add(-time.Second))); err != nil {
			t.Fatalf("Schedule: %v", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = sched1.Start(runCtx) }()
	go func() { _ = sched2.Start(runCtx) }()

	pubs := mt.WaitForPublishes(t, m, 20*time.Second)
	assertUniqueDeliveries(t, pubs, m)

	assertNoExtraPublish(t, mt, 300*time.Millisecond)
	if total := len(mt.Published()); total != m {
		t.Errorf("expected exactly %d deliveries, got %d (double-claim?)", m, total)
	}
}

func TestMongo_Integration_TwoSchedulersNoDoubleClaim(t *testing.T) {
	mt := newMockTransport()
	sched1, cleanup := setupMongoScheduler(t, mt)
	defer cleanup()

	// Second scheduler on the same collection (FindOneAndUpdate claim must dedup).
	sched2, err := NewMongoScheduler(sched1.collection.Database(), mt,
		WithPollInterval(50*time.Millisecond), WithCollection(sched1.collection.Name()))
	if err != nil {
		t.Fatalf("NewMongoScheduler #2: %v", err)
	}

	const m = 50
	ctx := context.Background()
	for i := 0; i < m; i++ {
		if err := sched1.Schedule(ctx, makeMessage(fmt.Sprintf("mmulti-%d", i), "mmulti.event", time.Now().Add(-time.Second))); err != nil {
			t.Fatalf("Schedule: %v", err)
		}
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = sched1.Start(runCtx) }()
	go func() { _ = sched2.Start(runCtx) }()

	pubs := mt.WaitForPublishes(t, m, 20*time.Second)
	assertUniqueDeliveries(t, pubs, m)
	assertNoExtraPublish(t, mt, 300*time.Millisecond)
	if total := len(mt.Published()); total != m {
		t.Errorf("expected exactly %d deliveries, got %d (double-claim?)", m, total)
	}
}

func TestPostgres_Integration_CronDelivery(t *testing.T) {
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt)
	defer cleanup()
	ctx := context.Background()

	// "* * * * *" fires every minute; first delivery is immediate (scheduled in
	// the past), and the message must persist (recurring) with occurrence_count++.
	msg := makeMessage("pg-cron-1", "cron.event", time.Now().Add(-time.Second))
	msg.Recurrence = &Recurrence{Type: RecurrenceCron, Cron: "* * * * *"}
	if err := sched.Schedule(ctx, msg); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = sched.Start(runCtx) }()

	mt.WaitForPublish(t, 10*time.Second)
	waitFor(t, 5*time.Second, func() bool {
		got, err := sched.Get(ctx, "pg-cron-1")
		return err == nil && got.OccurrenceCount >= 1 && got.ScheduledAt.After(time.Now())
	}, "cron message to be rescheduled with occurrence_count incremented")
}

func TestPostgres_Integration_AtLeastOnceRedelivery(t *testing.T) {
	// A message claimed-and-published but not finalized before a crash (a row
	// left in 'processing') must be redelivered — the documented at-least-once
	// guarantee. We seed that exact state and assert the same ID is delivered.
	mt := newMockTransport()
	sched, cleanup := setupPostgresScheduler(t, mt, WithStuckDuration(time.Second))
	defer cleanup()
	ctx := context.Background()

	old := time.Now().Add(-10 * time.Second)
	if _, err := sched.db.ExecContext(ctx, fmt.Sprintf(`
		INSERT INTO %s (id, event_name, payload, metadata, scheduled_at, created_at, retry_count, status, claimed_at)
		VALUES ('alo-1', 'alo.event', $1, $2, $3, $3, 0, 'processing', $4)`, sched.table),
		[]byte(`{"id":"alo-1"}`), []byte(`{}`), old, old); err != nil {
		t.Fatalf("seed processing row: %v", err)
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	go func() { _ = sched.Start(runCtx) }()

	pub := mt.WaitForPublish(t, 15*time.Second)
	if pub.MsgID != "alo-1" {
		t.Errorf("expected redelivery of 'alo-1', got %q", pub.MsgID)
	}
}

// TestPostgres_Integration_BatchFinalizeFailureKeepsClaimed proves the DB-side
// failure path: if finalize (applyBatchResults) fails after a claim, the row
// stays in 'processing' (not lost) and is recoverable — upholding at-least-once.
func TestPostgres_Integration_BatchFinalizeFailureKeepsClaimed(t *testing.T) {
	sched, cleanup := setupPostgresScheduler(t, newMockTransport(), WithStuckDuration(time.Second))
	defer cleanup()
	ctx := context.Background()

	if err := sched.Schedule(ctx, makeMessage("bf-1", "bf.event", time.Now().Add(-time.Second))); err != nil {
		t.Fatalf("Schedule: %v", err)
	}

	// Claim it (status -> processing), then force finalize to fail by passing an
	// already-cancelled context so BeginTx/Commit errors.
	claimed, err := sched.claimDue(ctx)
	if err != nil || len(claimed) != 1 {
		t.Fatalf("claimDue = %d, %v; want 1, nil", len(claimed), err)
	}
	cancelled, cancel := context.WithCancel(ctx)
	cancel()
	if err := sched.applyBatchResults(cancelled, []string{"bf-1"}, nil, nil, nil); err == nil {
		t.Fatal("expected applyBatchResults to fail on a cancelled context")
	}

	// The message must NOT have been deleted — it remains claimed for recovery.
	var status string
	if err := sched.db.QueryRowContext(ctx,
		fmt.Sprintf("SELECT status FROM %s WHERE id = 'bf-1'", sched.table)).Scan(&status); err != nil {
		t.Fatalf("message lost after finalize failure: %v", err)
	}
	if status != "processing" {
		t.Errorf("expected status 'processing' after finalize failure, got %q", status)
	}

	// recoverStuck (after backdating claimed_at) returns it to pending.
	if _, err := sched.db.ExecContext(ctx,
		fmt.Sprintf("UPDATE %s SET claimed_at = $1 WHERE id = 'bf-1'", sched.table),
		time.Now().Add(-time.Minute)); err != nil {
		t.Fatalf("backdate: %v", err)
	}
	sched.recoverStuck(ctx)
	pending, _ := sched.countPending(ctx)
	if pending != 1 {
		t.Errorf("expected message recovered to pending, countPending=%d", pending)
	}
}

// assertNoExtraPublish fails if any further publish arrives within the window,
// replacing fixed "settle" sleeps with a quiet-window check on the publish channel.
func assertNoExtraPublish(t *testing.T, mt *mockTransport, within time.Duration) {
	t.Helper()
	select {
	case extra := <-mt.publishCh:
		t.Errorf("unexpected extra delivery: %s", extra.MsgID)
	case <-time.After(within):
	}
}

// assertUniqueDeliveries fails if any message ID was delivered more than once.
func assertUniqueDeliveries(t *testing.T, pubs []publishedMessage, want int) {
	t.Helper()
	seen := make(map[string]int, len(pubs))
	for _, p := range pubs {
		seen[p.MsgID]++
	}
	if len(seen) != want {
		t.Errorf("expected %d unique delivered IDs, got %d", want, len(seen))
	}
	for id, n := range seen {
		if n > 1 {
			t.Errorf("message %q delivered %d times (expected once)", id, n)
		}
	}
}
