package gateway

import (
	"context"
	"crypto/tls"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	scheduler "github.com/rbaliyan/event-scheduler"
	schedulerpb "github.com/rbaliyan/event-scheduler/proto/scheduler/v1"
	"github.com/rbaliyan/event-scheduler/service"
	"github.com/rbaliyan/event/v3/health"
	"google.golang.org/grpc"
)

// mockScheduler implements scheduler.Scheduler and health.Checker for gateway tests.
type mockScheduler struct {
	messages map[string]*scheduler.Message
}

func newMockScheduler() *mockScheduler {
	return &mockScheduler{
		messages: make(map[string]*scheduler.Message),
	}
}

func (m *mockScheduler) Schedule(_ context.Context, msg scheduler.Message) error {
	m.messages[msg.ID] = &msg
	return nil
}

func (m *mockScheduler) Cancel(_ context.Context, id string) error {
	delete(m.messages, id)
	return nil
}

func (m *mockScheduler) Get(_ context.Context, id string) (*scheduler.Message, error) {
	msg, ok := m.messages[id]
	if !ok {
		return nil, scheduler.NewNotFoundError(id)
	}
	return msg, nil
}

func (m *mockScheduler) List(_ context.Context, _ scheduler.Filter) ([]*scheduler.Message, error) {
	msgs := make([]*scheduler.Message, 0, len(m.messages))
	for _, msg := range m.messages {
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (m *mockScheduler) Start(_ context.Context) error { return nil }
func (m *mockScheduler) Stop(_ context.Context) error  { return nil }

func (m *mockScheduler) Health(_ context.Context) *health.Result {
	return &health.Result{
		Status:    health.StatusHealthy,
		Message:   "ok",
		Latency:   time.Millisecond,
		CheckedAt: time.Now(),
	}
}

func TestNewHandler_InvalidAddr(t *testing.T) {
	ctx := context.Background()

	_, err := NewHandler(ctx, "", WithInsecure())
	if err == nil {
		t.Fatal("expected error for empty address")
	}
}

func TestNewHandler_WithInsecure(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler, err := NewHandler(ctx, "localhost:19999", WithInsecure())
	if err != nil {
		t.Fatalf("NewHandler failed: %v", err)
	}
	defer handler.Close()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewHandler_WithTLS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler, err := NewHandler(ctx, "localhost:19999", WithTLS(nil))
	if err != nil {
		t.Fatalf("NewHandler with TLS failed: %v", err)
	}
	defer handler.Close()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewHandler_WithCustomTLS(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	cfg := &tls.Config{MinVersion: tls.VersionTLS13}
	handler, err := NewHandler(ctx, "localhost:19999", WithTLS(cfg))
	if err != nil {
		t.Fatalf("NewHandler with custom TLS failed: %v", err)
	}
	defer handler.Close()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewHandler_WithDialOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler, err := NewHandler(ctx, "localhost:19999",
		WithInsecure(),
		WithDialOptions(grpc.WithAuthority("custom-authority")),
	)
	if err != nil {
		t.Fatalf("NewHandler with dial options failed: %v", err)
	}
	defer handler.Close()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewHandler_WithMuxOptions(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	handler, err := NewHandler(ctx, "localhost:19999",
		WithInsecure(),
		WithMuxOptions(runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler)),
	)
	if err != nil {
		t.Fatalf("NewHandler with mux options failed: %v", err)
	}
	defer handler.Close()
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func newTestService(t *testing.T) *service.Service {
	t.Helper()
	svc, err := service.New(newMockScheduler())
	if err != nil {
		t.Fatalf("service.New failed: %v", err)
	}
	return svc
}

func TestNewInProcessHandler(t *testing.T) {
	ctx := context.Background()

	handler, err := NewInProcessHandler(ctx, newTestService(t))
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewInProcessHandler_WithOptions(t *testing.T) {
	ctx := context.Background()

	handler, err := NewInProcessHandler(ctx, newTestService(t),
		WithMuxOptions(runtime.WithErrorHandler(runtime.DefaultHTTPErrorHandler)),
	)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}
	if handler == nil {
		t.Fatal("expected non-nil handler")
	}
}

func TestNewInProcessHandler_GetMessage(t *testing.T) {
	ctx := context.Background()

	mock := newMockScheduler()
	now := time.Now()
	mock.messages["msg-1"] = &scheduler.Message{
		ID:          "msg-1",
		EventName:   "orders.created",
		Payload:     []byte(`{"id":"123"}`),
		Metadata:    map[string]string{"source": "api"},
		ScheduledAt: now.Add(time.Hour),
		CreatedAt:   now,
	}

	svc, err := service.New(mock)
	if err != nil {
		t.Fatalf("service.New failed: %v", err)
	}
	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/messages/msg-1", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("GET /v1/messages/msg-1 status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
}

func TestNewInProcessHandler_GetMessage_NotFound(t *testing.T) {
	ctx := context.Background()

	handler, err := NewInProcessHandler(ctx, newTestService(t))
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/messages/nonexistent", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusNotFound {
		t.Errorf("GET nonexistent status = %d, want 404; body: %s", rec.Code, rec.Body.String())
	}
}

func TestNewInProcessHandler_ListMessages(t *testing.T) {
	ctx := context.Background()

	mock := newMockScheduler()
	now := time.Now()
	mock.messages["msg-1"] = &scheduler.Message{
		ID:          "msg-1",
		EventName:   "orders.created",
		Payload:     []byte(`{}`),
		ScheduledAt: now.Add(time.Hour),
		CreatedAt:   now,
	}

	svc, err := service.New(mock)
	if err != nil {
		t.Fatalf("service.New failed: %v", err)
	}
	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/messages", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("GET /v1/messages status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
}

func TestNewInProcessHandler_Health(t *testing.T) {
	ctx := context.Background()

	handler, err := NewInProcessHandler(ctx, newTestService(t))
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/health", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("GET /v1/health status = %d, want 200; body: %s", rec.Code, rec.Body.String())
	}
}

func TestNewInProcessHandler_InvalidRoute(t *testing.T) {
	ctx := context.Background()

	handler, err := NewInProcessHandler(ctx, newTestService(t))
	if err != nil {
		t.Fatalf("NewInProcessHandler failed: %v", err)
	}

	req := httptest.NewRequest(http.MethodGet, "/v1/nonexistent", nil)
	rec := httptest.NewRecorder()
	handler.ServeHTTP(rec, req)

	if rec.Code == http.StatusOK {
		t.Errorf("expected non-200 status for invalid route, got %d", rec.Code)
	}
}

func TestHandler_Close(t *testing.T) {
	h := &Handler{
		Handler: http.NotFoundHandler(),
		done:    make(chan struct{}),
	}

	// First close should succeed
	if err := h.Close(); err != nil {
		t.Fatalf("first Close failed: %v", err)
	}

	// Second close should be idempotent
	if err := h.Close(); err != nil {
		t.Fatalf("second Close failed: %v", err)
	}
}

func TestOptions(t *testing.T) {
	t.Run("WithInsecure", func(t *testing.T) {
		o := &options{secure: true}
		WithInsecure()(o)
		if o.secure {
			t.Error("secure should be false after WithInsecure")
		}
		if o.tlsConfig != nil {
			t.Error("tlsConfig should be nil after WithInsecure")
		}
	})

	t.Run("WithTLS_nil", func(t *testing.T) {
		o := &options{}
		WithTLS(nil)(o)
		if !o.secure {
			t.Error("secure should be true after WithTLS")
		}
	})

	t.Run("WithTLS_config", func(t *testing.T) {
		cfg := &tls.Config{MinVersion: tls.VersionTLS13}
		o := &options{}
		WithTLS(cfg)(o)
		if !o.secure {
			t.Error("secure should be true after WithTLS")
		}
		if o.tlsConfig != cfg {
			t.Error("tlsConfig should be the provided config")
		}
	})

	t.Run("WithDialOptions", func(t *testing.T) {
		o := &options{}
		WithDialOptions()(o) // no-op call
		if len(o.dialOpts) != 0 {
			t.Errorf("dialOpts length = %d, want 0", len(o.dialOpts))
		}
	})

	t.Run("WithDialOptions_multiple", func(t *testing.T) {
		o := &options{}
		opt1 := grpc.WithAuthority("auth1")
		opt2 := grpc.WithAuthority("auth2")
		WithDialOptions(opt1, opt2)(o)
		if len(o.dialOpts) != 2 {
			t.Errorf("dialOpts length = %d, want 2", len(o.dialOpts))
		}
	})

	t.Run("WithMuxOptions", func(t *testing.T) {
		o := &options{}
		WithMuxOptions()(o)
		if len(o.muxOpts) != 0 {
			t.Errorf("muxOpts length = %d, want 0", len(o.muxOpts))
		}
	})

	t.Run("buildDialOpts_insecure", func(t *testing.T) {
		o := &options{secure: false}
		opts := o.buildDialOpts()
		if len(opts) == 0 {
			t.Fatal("expected at least one dial option")
		}
	})

	t.Run("buildDialOpts_tls_default", func(t *testing.T) {
		o := &options{secure: true}
		opts := o.buildDialOpts()
		if len(opts) == 0 {
			t.Fatal("expected at least one dial option")
		}
	})

	t.Run("buildDialOpts_tls_custom", func(t *testing.T) {
		cfg := &tls.Config{MinVersion: tls.VersionTLS13}
		o := &options{secure: true, tlsConfig: cfg}
		opts := o.buildDialOpts()
		if len(opts) == 0 {
			t.Fatal("expected at least one dial option")
		}
	})

	t.Run("buildDialOpts_with_user_opts", func(t *testing.T) {
		o := &options{
			secure:   false,
			dialOpts: []grpc.DialOption{grpc.WithAuthority("custom")},
		}
		opts := o.buildDialOpts()
		if len(opts) < 2 {
			t.Fatalf("buildDialOpts() returned %d opts, want at least 2", len(opts))
		}
	})
}

// Ensure the service is registrable - compile-time check.
var _ schedulerpb.SchedulerServiceServer = (*service.Service)(nil)
