package service

import (
	"context"
	"errors"
	"testing"
	"time"

	scheduler "github.com/rbaliyan/event-scheduler"
	schedulerpb "github.com/rbaliyan/event-scheduler/proto/scheduler/v1"
	"github.com/rbaliyan/event/v3/health"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// mockScheduler implements scheduler.Scheduler for testing.
type mockScheduler struct {
	messages map[string]*scheduler.Message
	listMsgs []*scheduler.Message
	listErr  error
	getErr   error
	health   *health.Result
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
	if _, ok := m.messages[id]; !ok {
		return scheduler.ErrNotFound
	}
	delete(m.messages, id)
	return nil
}

func (m *mockScheduler) Get(_ context.Context, id string) (*scheduler.Message, error) {
	if m.getErr != nil {
		return nil, m.getErr
	}
	msg, ok := m.messages[id]
	if !ok {
		return nil, scheduler.NewNotFoundError(id)
	}
	return msg, nil
}

func (m *mockScheduler) List(_ context.Context, _ scheduler.Filter) ([]*scheduler.Message, error) {
	if m.listErr != nil {
		return nil, m.listErr
	}
	if m.listMsgs != nil {
		return m.listMsgs, nil
	}
	msgs := make([]*scheduler.Message, 0, len(m.messages))
	for _, msg := range m.messages {
		msgs = append(msgs, msg)
	}
	return msgs, nil
}

func (m *mockScheduler) Start(_ context.Context) error { return nil }
func (m *mockScheduler) Stop(_ context.Context) error  { return nil }

// mockHealthScheduler combines Scheduler and HealthChecker.
type mockHealthScheduler struct {
	*mockScheduler
	healthResult *health.Result
}

func (m *mockHealthScheduler) Health(_ context.Context) *health.Result {
	return m.healthResult
}

func TestNew_NilScheduler(t *testing.T) {
	_, err := New(nil)
	if err == nil {
		t.Fatal("expected error for nil scheduler")
	}
}

func TestNew(t *testing.T) {
	svc, err := New(newMockScheduler())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
	if svc == nil {
		t.Fatal("expected non-nil service")
	}
}

func TestService_Get(t *testing.T) {
	ctx := context.Background()
	mock := newMockScheduler()
	svc, err := New(mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Now()
	mock.messages["msg-1"] = &scheduler.Message{
		ID:          "msg-1",
		EventName:   "orders.created",
		Payload:     []byte(`{"order_id":"123"}`),
		Metadata:    map[string]string{"source": "api"},
		ScheduledAt: now.Add(time.Hour),
		CreatedAt:   now,
		RetryCount:  2,
	}

	resp, err := svc.Get(ctx, &schedulerpb.GetRequest{Id: "msg-1"})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	msg := resp.GetMessage()
	if msg == nil {
		t.Fatal("expected message, got nil")
	}
	if msg.Id != "msg-1" {
		t.Errorf("id = %q, want %q", msg.Id, "msg-1")
	}
	if msg.EventName != "orders.created" {
		t.Errorf("event_name = %q, want %q", msg.EventName, "orders.created")
	}
	if msg.RetryCount != 2 {
		t.Errorf("retry_count = %d, want 2", msg.RetryCount)
	}
	if msg.Metadata["source"] != "api" {
		t.Errorf("metadata[source] = %q, want %q", msg.Metadata["source"], "api")
	}
}

func TestService_Get_NotFound(t *testing.T) {
	ctx := context.Background()
	svc, err := New(newMockScheduler())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = svc.Get(ctx, &schedulerpb.GetRequest{Id: "nonexistent"})
	if err == nil {
		t.Fatal("expected error for nonexistent message")
	}

	st, ok := status.FromError(err)
	if !ok {
		t.Fatalf("expected gRPC status error, got: %v", err)
	}
	if st.Code() != codes.NotFound {
		t.Errorf("expected NotFound, got: %v", st.Code())
	}
}

func TestService_Get_EmptyID(t *testing.T) {
	ctx := context.Background()
	svc, err := New(newMockScheduler())
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = svc.Get(ctx, &schedulerpb.GetRequest{})
	if err == nil {
		t.Fatal("expected error for empty id")
	}

	st, _ := status.FromError(err)
	if st.Code() != codes.InvalidArgument {
		t.Errorf("expected InvalidArgument, got: %v", st.Code())
	}
}

func TestService_List(t *testing.T) {
	ctx := context.Background()
	mock := newMockScheduler()
	svc, err := New(mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Now()
	mock.messages["msg-1"] = &scheduler.Message{
		ID:          "msg-1",
		EventName:   "orders.created",
		Payload:     []byte(`{}`),
		ScheduledAt: now.Add(time.Hour),
		CreatedAt:   now,
	}
	mock.messages["msg-2"] = &scheduler.Message{
		ID:          "msg-2",
		EventName:   "orders.shipped",
		Payload:     []byte(`{}`),
		ScheduledAt: now.Add(2 * time.Hour),
		CreatedAt:   now,
	}

	resp, err := svc.List(ctx, &schedulerpb.ListRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if len(resp.GetMessages()) != 2 {
		t.Errorf("expected 2 messages, got %d", len(resp.GetMessages()))
	}
	if resp.GetTotalCount() != 2 {
		t.Errorf("total_count = %d, want 2", resp.GetTotalCount())
	}
}

func TestService_List_WithFilter(t *testing.T) {
	ctx := context.Background()
	mock := newMockScheduler()
	svc, err := New(mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	now := time.Now()
	before := now.Add(time.Hour)
	after := now.Add(-time.Hour)

	resp, err := svc.List(ctx, &schedulerpb.ListRequest{
		EventName: "orders.created",
		Before:    timestamppb.New(before),
		After:     timestamppb.New(after),
		Limit:     50,
	})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp == nil {
		t.Fatal("expected non-nil response")
	}
}

func TestService_List_LimitClamped(t *testing.T) {
	ctx := context.Background()
	mock := newMockScheduler()
	svc, err := New(mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Zero limit should be clamped to maxListLimit
	_, err = svc.List(ctx, &schedulerpb.ListRequest{Limit: 0})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	// Over maxListLimit should be clamped
	_, err = svc.List(ctx, &schedulerpb.ListRequest{Limit: 5000})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}
}

func TestService_List_Error(t *testing.T) {
	ctx := context.Background()
	mock := newMockScheduler()
	mock.listErr = errors.New("database error")
	svc, err := New(mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	_, err = svc.List(ctx, &schedulerpb.ListRequest{})
	if err == nil {
		t.Fatal("expected error")
	}

	st, _ := status.FromError(err)
	if st.Code() != codes.Internal {
		t.Errorf("expected Internal, got: %v", st.Code())
	}
}

func TestService_Health_WithChecker(t *testing.T) {
	ctx := context.Background()
	mock := &mockHealthScheduler{
		mockScheduler: newMockScheduler(),
		healthResult: &health.Result{
			Status:  health.StatusHealthy,
			Message: "all good",
			Latency: 5 * time.Millisecond,
			Details: map[string]any{
				"pending_messages": int64(10),
				"stuck_messages":   int64(0),
			},
		},
	}
	svc, err := New(mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resp, err := svc.Health(ctx, &schedulerpb.HealthRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Status != schedulerpb.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Errorf("status = %v, want HEALTHY", resp.Status)
	}
	if resp.Message != "all good" {
		t.Errorf("message = %q, want %q", resp.Message, "all good")
	}
	if resp.LatencyMs != 5 {
		t.Errorf("latency_ms = %d, want 5", resp.LatencyMs)
	}
	if resp.Details["pending_messages"] != "10" {
		t.Errorf("details[pending_messages] = %q, want %q", resp.Details["pending_messages"], "10")
	}
	if resp.Details["stuck_messages"] != "0" {
		t.Errorf("details[stuck_messages] = %q, want %q", resp.Details["stuck_messages"], "0")
	}
}

func TestService_Health_WithoutChecker(t *testing.T) {
	ctx := context.Background()
	svc, err := New(newMockScheduler()) // plain mock does not implement HealthChecker
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resp, err := svc.Health(ctx, &schedulerpb.HealthRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Status != schedulerpb.HealthStatus_HEALTH_STATUS_HEALTHY {
		t.Errorf("status = %v, want HEALTHY", resp.Status)
	}
}

func TestService_Health_Degraded(t *testing.T) {
	ctx := context.Background()
	mock := &mockHealthScheduler{
		mockScheduler: newMockScheduler(),
		healthResult: &health.Result{
			Status:  health.StatusDegraded,
			Message: "stuck messages detected",
			Latency: 50 * time.Millisecond,
			Details: map[string]any{
				"pending_messages": int64(100),
				"stuck_messages":   int64(5),
			},
		},
	}
	svc, err := New(mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resp, err := svc.Health(ctx, &schedulerpb.HealthRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Status != schedulerpb.HealthStatus_HEALTH_STATUS_DEGRADED {
		t.Errorf("status = %v, want DEGRADED", resp.Status)
	}
}

func TestService_Health_NilResult(t *testing.T) {
	ctx := context.Background()
	mock := &mockHealthScheduler{
		mockScheduler: newMockScheduler(),
		healthResult:  nil,
	}
	svc, err := New(mock)
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	resp, err := svc.Health(ctx, &schedulerpb.HealthRequest{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	if resp.Status != schedulerpb.HealthStatus_HEALTH_STATUS_UNHEALTHY {
		t.Errorf("status = %v, want UNHEALTHY", resp.Status)
	}
}

func TestToGRPCError(t *testing.T) {
	tests := []struct {
		name string
		err  error
		code codes.Code
	}{
		{"nil", nil, codes.OK},
		{"not found", scheduler.ErrNotFound, codes.NotFound},
		{"not found detail", scheduler.NewNotFoundError("msg-1"), codes.NotFound},
		{"context cancelled", context.Canceled, codes.Canceled},
		{"deadline exceeded", context.DeadlineExceeded, codes.DeadlineExceeded},
		{"unknown", errors.New("unknown"), codes.Internal},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := toGRPCError(tt.err)
			if tt.err == nil {
				if got != nil {
					t.Errorf("toGRPCError(nil) = %v, want nil", got)
				}
				return
			}
			st, ok := status.FromError(got)
			if !ok {
				t.Fatalf("expected gRPC status error, got: %v", got)
			}
			if st.Code() != tt.code {
				t.Errorf("toGRPCError(%v) code = %v, want %v", tt.err, st.Code(), tt.code)
			}
		})
	}
}

func TestMessageToProto(t *testing.T) {
	t.Run("nil message", func(t *testing.T) {
		pb := messageToProto(nil)
		if pb != nil {
			t.Errorf("expected nil, got %v", pb)
		}
	})

	t.Run("full message", func(t *testing.T) {
		now := time.Now()
		scheduled := now.Add(time.Hour)
		msg := &scheduler.Message{
			ID:          "msg-1",
			EventName:   "orders.created",
			Payload:     []byte(`{"id":"123"}`),
			Metadata:    map[string]string{"key": "value"},
			ScheduledAt: scheduled,
			CreatedAt:   now,
			RetryCount:  3,
		}

		pb := messageToProto(msg)
		if pb.Id != "msg-1" {
			t.Errorf("id = %q, want %q", pb.Id, "msg-1")
		}
		if pb.EventName != "orders.created" {
			t.Errorf("event_name = %q, want %q", pb.EventName, "orders.created")
		}
		if string(pb.Payload) != `{"id":"123"}` {
			t.Errorf("payload = %q", string(pb.Payload))
		}
		if pb.Metadata["key"] != "value" {
			t.Errorf("metadata[key] = %q, want %q", pb.Metadata["key"], "value")
		}
		if pb.RetryCount != 3 {
			t.Errorf("retry_count = %d, want 3", pb.RetryCount)
		}
		if pb.ScheduledAt == nil {
			t.Error("expected non-nil scheduled_at")
		}
		if pb.CreatedAt == nil {
			t.Error("expected non-nil created_at")
		}
	})

	t.Run("zero timestamps", func(t *testing.T) {
		msg := &scheduler.Message{ID: "msg-1"}
		pb := messageToProto(msg)
		if pb.ScheduledAt != nil {
			t.Error("expected nil scheduled_at for zero time")
		}
		if pb.CreatedAt != nil {
			t.Error("expected nil created_at for zero time")
		}
	})
}

func TestProtoToFilter(t *testing.T) {
	now := time.Now()
	before := now.Add(time.Hour)
	after := now.Add(-time.Hour)

	req := &schedulerpb.ListRequest{
		EventName: "orders.created",
		Before:    timestamppb.New(before),
		After:     timestamppb.New(after),
		Limit:     50,
	}

	f := protoToFilter(req)
	if f.EventName != "orders.created" {
		t.Errorf("event_name = %q, want %q", f.EventName, "orders.created")
	}
	if f.Limit != 50 {
		t.Errorf("limit = %d, want 50", f.Limit)
	}
	if f.Before.IsZero() {
		t.Error("expected non-zero before")
	}
	if f.After.IsZero() {
		t.Error("expected non-zero after")
	}
}

func TestProtoToFilter_Empty(t *testing.T) {
	req := &schedulerpb.ListRequest{}
	f := protoToFilter(req)
	if f.EventName != "" {
		t.Errorf("event_name = %q, want empty", f.EventName)
	}
	if f.Limit != 0 {
		t.Errorf("limit = %d, want 0", f.Limit)
	}
	if !f.Before.IsZero() {
		t.Error("expected zero before")
	}
	if !f.After.IsZero() {
		t.Error("expected zero after")
	}
}

func TestHealthToProto(t *testing.T) {
	t.Run("nil result", func(t *testing.T) {
		resp := healthToProto(nil)
		if resp.Status != schedulerpb.HealthStatus_HEALTH_STATUS_UNHEALTHY {
			t.Errorf("status = %v, want UNHEALTHY", resp.Status)
		}
	})

	t.Run("healthy with details", func(t *testing.T) {
		result := &health.Result{
			Status:  health.StatusHealthy,
			Message: "ok",
			Latency: 10 * time.Millisecond,
			Details: map[string]any{
				"pending_messages": int64(5),
			},
		}
		resp := healthToProto(result)
		if resp.Status != schedulerpb.HealthStatus_HEALTH_STATUS_HEALTHY {
			t.Errorf("status = %v, want HEALTHY", resp.Status)
		}
		if resp.LatencyMs != 10 {
			t.Errorf("latency_ms = %d, want 10", resp.LatencyMs)
		}
		if resp.Details["pending_messages"] != "5" {
			t.Errorf("details[pending_messages] = %q, want %q", resp.Details["pending_messages"], "5")
		}
	})

	t.Run("no details", func(t *testing.T) {
		result := &health.Result{
			Status: health.StatusHealthy,
		}
		resp := healthToProto(result)
		if len(resp.Details) != 0 {
			t.Errorf("expected empty details, got %v", resp.Details)
		}
	})
}

func TestDetailValueToString(t *testing.T) {
	tests := []struct {
		name string
		val  any
		want string
	}{
		{"string", "hello", "hello"},
		{"int64", int64(42), "42"},
		{"float64", float64(3.14), "3.14"},
		{"bool", true, "true"},
		{"error", errors.New("fail"), "fail"},
		{"slice", []string{"a", "b"}, `["a","b"]`},
		{"map", map[string]int{"x": 1}, `{"x":1}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := detailValueToString(tt.val)
			if got != tt.want {
				t.Errorf("detailValueToString(%v) = %q, want %q", tt.val, got, tt.want)
			}
		})
	}
}
