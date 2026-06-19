package scheduler

import (
	"context"
	"database/sql"
	"errors"
	"testing"
	"time"

	"github.com/DATA-DOG/go-sqlmock"
	"github.com/rbaliyan/event/v3/health"
)

// These tests exercise the real PostgreSQL backend logic (claim, deliver,
// finalize, retry, recover, health, migrations) against a mocked *sql.DB, so
// the SQL paths ship with non-integration coverage. sqlmock's default query
// matcher is regexp (substring), so patterns below match distinctive fragments.

func newSqlmockScheduler(t *testing.T, ping bool, opts ...Option) (*PostgresScheduler, sqlmock.Sqlmock, *fakeTransport) {
	t.Helper()
	sqlDB, mock, err := sqlmock.New(sqlmock.MonitorPingsOption(ping))
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	ft := newFakeTransport()
	sched, err := NewPostgresScheduler(sqlDB, ft, opts...)
	if err != nil {
		t.Fatalf("NewPostgresScheduler: %v", err)
	}
	t.Cleanup(func() {
		if err := mock.ExpectationsWereMet(); err != nil {
			t.Errorf("unmet sqlmock expectations: %v", err)
		}
		_ = sqlDB.Close()
	})
	return sched, mock, ft
}

func dueRow() *sqlmock.Rows {
	return sqlmock.NewRows([]string{
		"id", "event_name", "payload", "metadata", "scheduled_at", "created_at",
		"retry_count", "recurrence_type", "recurrence_value", "max_occurrences", "until", "occurrence_count",
	}).AddRow("m1", "e", []byte("payload"), nil, time.Now().Add(-time.Second), time.Now(), 0, nil, nil, 0, nil, 0)
}

func rowsWith(ids ...string) *sqlmock.Rows {
	r := sqlmock.NewRows([]string{
		"id", "event_name", "payload", "metadata", "scheduled_at", "created_at",
		"retry_count", "recurrence_type", "recurrence_value", "max_occurrences", "until", "occurrence_count",
	})
	for _, id := range ids {
		r.AddRow(id, "e", []byte("p"), nil, time.Now(), time.Now(), 0, nil, nil, 0, nil, 0)
	}
	return r
}

func TestPostgresSqlmock_List(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, false)
	mock.ExpectQuery("FROM scheduled_messages").WillReturnRows(rowsWith("a", "b"))

	msgs, err := sched.List(context.Background(), Filter{EventName: "e", Limit: 10})
	if err != nil {
		t.Fatalf("List: %v", err)
	}
	if len(msgs) != 2 {
		t.Errorf("List returned %d messages, want 2", len(msgs))
	}
}

func TestPostgresSqlmock_StartStop(t *testing.T) {
	t.Parallel()
	// Long poll interval so only the startup recoverStuck runs deterministically.
	sched, mock, _ := newSqlmockScheduler(t, false, WithPollInterval(time.Hour))
	mock.ExpectExec("WHERE status = 'processing'").WillReturnResult(sqlmock.NewResult(0, 0))

	errCh := make(chan error, 1)
	go func() { errCh <- sched.Start(context.Background()) }()

	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := sched.Stop(stopCtx); err != nil {
		t.Fatalf("Stop: %v", err)
	}
	if err := <-errCh; err != nil {
		t.Fatalf("Start returned: %v", err)
	}
}

func TestPostgresSqlmock_Schedule(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, false)
	mock.ExpectExec("INSERT INTO scheduled_messages").WillReturnResult(sqlmock.NewResult(0, 1))

	err := sched.Schedule(context.Background(), Message{
		ID: "m1", EventName: "e", Payload: []byte("p"), ScheduledAt: time.Now().Add(time.Hour),
	})
	if err != nil {
		t.Fatalf("Schedule: %v", err)
	}
}

func TestPostgresSqlmock_Get(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, false)
	mock.ExpectQuery("FROM scheduled_messages").WillReturnRows(dueRow())

	got, err := sched.Get(context.Background(), "m1")
	if err != nil {
		t.Fatalf("Get: %v", err)
	}
	if got.ID != "m1" || got.EventName != "e" {
		t.Errorf("unexpected message: %+v", got)
	}
}

func TestPostgresSqlmock_GetNotFound(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, false)
	mock.ExpectQuery("FROM scheduled_messages").WillReturnError(sql.ErrNoRows)

	_, err := sched.Get(context.Background(), "missing")
	if !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound, got %v", err)
	}
}

func TestPostgresSqlmock_Cancel(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, false)
	mock.ExpectExec("DELETE FROM scheduled_messages WHERE id").WillReturnResult(sqlmock.NewResult(0, 1))

	if err := sched.Cancel(context.Background(), "m1"); err != nil {
		t.Fatalf("Cancel: %v", err)
	}
}

func TestPostgresSqlmock_CancelNotFound(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, false)
	mock.ExpectExec("DELETE FROM scheduled_messages WHERE id").WillReturnResult(sqlmock.NewResult(0, 0))

	if err := sched.Cancel(context.Background(), "m1"); !errors.Is(err, ErrNotFound) {
		t.Errorf("expected ErrNotFound for 0 rows affected, got %v", err)
	}
}

func TestPostgresSqlmock_ProcessDue_DeliversAndDeletes(t *testing.T) {
	t.Parallel()
	sched, mock, ft := newSqlmockScheduler(t, false)

	mock.ExpectQuery("SET status = 'processing'").WillReturnRows(dueRow())
	mock.ExpectBegin()
	mock.ExpectExec("DELETE FROM scheduled_messages WHERE id = ANY").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if n := sched.processDue(context.Background()); n != 1 {
		t.Errorf("processDue = %d, want 1", n)
	}
	if ft.count() != 1 {
		t.Errorf("expected 1 publish, got %d", ft.count())
	}
}

func TestPostgresSqlmock_ProcessDue_RetryOnFailure(t *testing.T) {
	t.Parallel()
	// Default maxRetries=0 (infinite) -> a failed publish reschedules for retry.
	sched, mock, ft := newSqlmockScheduler(t, false)
	ft.failNext(1, errors.New("transport down"))

	mock.ExpectQuery("SET status = 'processing'").WillReturnRows(dueRow())
	mock.ExpectBegin()
	mock.ExpectExec("UPDATE scheduled_messages SET retry_count").WillReturnResult(sqlmock.NewResult(0, 1))
	mock.ExpectCommit()

	if n := sched.processDue(context.Background()); n != 1 {
		t.Errorf("processDue = %d, want 1 (retried message still counts as activity)", n)
	}
	if ft.count() != 0 {
		t.Errorf("expected 0 successful publishes, got %d", ft.count())
	}
}

func TestPostgresSqlmock_ProcessDue_NoDueMessages(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, false)
	emptyRows := sqlmock.NewRows([]string{
		"id", "event_name", "payload", "metadata", "scheduled_at", "created_at",
		"retry_count", "recurrence_type", "recurrence_value", "max_occurrences", "until", "occurrence_count",
	})
	mock.ExpectQuery("SET status = 'processing'").WillReturnRows(emptyRows)

	if n := sched.processDue(context.Background()); n != 0 {
		t.Errorf("processDue = %d, want 0 when nothing is due", n)
	}
}

func TestPostgresSqlmock_RecoverStuck(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, false)
	mock.ExpectExec("WHERE status = 'processing' AND claimed_at").WillReturnResult(sqlmock.NewResult(0, 3))

	sched.recoverStuck(context.Background()) // must not panic; mock asserts the query ran
}

func TestPostgresSqlmock_CountPendingAndStuck(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, false)
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(5))
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(2))

	pending, err := sched.countPending(context.Background())
	if err != nil || pending != 5 {
		t.Errorf("countPending = %d, %v; want 5, nil", pending, err)
	}
	stuck, err := sched.countStuck(context.Background())
	if err != nil || stuck != 2 {
		t.Errorf("countStuck = %d, %v; want 2, nil", stuck, err)
	}
}

func TestPostgresSqlmock_Health(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, true)
	mock.ExpectPing()
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(4)) // pending
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(0)) // stuck

	res := sched.Health(context.Background())
	if res.Status != health.StatusHealthy {
		t.Errorf("expected healthy status with no stuck messages, got %v", res.Status)
	}
	if res.Details["pending_messages"].(int64) != 4 {
		t.Errorf("pending_messages = %v, want 4", res.Details["pending_messages"])
	}
}

func TestPostgresSqlmock_HealthDegradedOnStuck(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, true)
	mock.ExpectPing()
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(1)) // pending
	mock.ExpectQuery("SELECT COUNT").WillReturnRows(sqlmock.NewRows([]string{"count"}).AddRow(3)) // stuck > 0

	res := sched.Health(context.Background())
	if res.Details["stuck_messages"].(int64) != 3 {
		t.Errorf("stuck_messages = %v, want 3", res.Details["stuck_messages"])
	}
}

func TestPostgresSqlmock_EnsureTableAndMigrations(t *testing.T) {
	t.Parallel()
	sched, mock, _ := newSqlmockScheduler(t, false)
	mock.ExpectExec("CREATE TABLE IF NOT EXISTS scheduled_messages").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ADD COLUMN IF NOT EXISTS retry_count").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ADD COLUMN IF NOT EXISTS recurrence_type").WillReturnResult(sqlmock.NewResult(0, 0))
	mock.ExpectExec("ADD COLUMN IF NOT EXISTS status").WillReturnResult(sqlmock.NewResult(0, 0))

	ctx := context.Background()
	if err := sched.EnsureTable(ctx); err != nil {
		t.Fatalf("EnsureTable: %v", err)
	}
	if err := sched.MigrateAddRetryCount(ctx); err != nil {
		t.Fatalf("MigrateAddRetryCount: %v", err)
	}
	if err := sched.MigrateAddRecurrence(ctx); err != nil {
		t.Fatalf("MigrateAddRecurrence: %v", err)
	}
	if err := sched.MigrateAddProcessingState(ctx); err != nil {
		t.Fatalf("MigrateAddProcessingState: %v", err)
	}
}

func TestPostgresSqlmock_InvalidTableName(t *testing.T) {
	t.Parallel()
	db, _, err := sqlmock.New()
	if err != nil {
		t.Fatalf("sqlmock.New: %v", err)
	}
	defer func() { _ = db.Close() }()
	if _, err := NewPostgresScheduler(db, newFakeTransport(), WithTable("bad name!")); err == nil {
		t.Error("expected error for invalid table name")
	}
}
