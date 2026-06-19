package gateway

import (
	"context"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	scheduler "github.com/rbaliyan/event-scheduler"
	"github.com/rbaliyan/event-scheduler/service"
)

// TestSmoke_GatewayInProcessRoundTrip drives the HTTP/JSON gateway end-to-end
// in-process (no network) across the health, get, and not-found routes.
func TestSmoke_GatewayInProcessRoundTrip(t *testing.T) {
	t.Parallel()
	ctx := context.Background()

	mock := newMockScheduler()
	mock.messages["msg-1"] = &scheduler.Message{
		ID: "msg-1", EventName: "orders.created", Payload: []byte("p"),
		ScheduledAt: time.Now().Add(time.Hour),
	}
	svc, err := service.New(mock)
	if err != nil {
		t.Fatalf("service.New: %v", err)
	}
	handler, err := NewInProcessHandler(ctx, svc)
	if err != nil {
		t.Fatalf("NewInProcessHandler: %v", err)
	}
	t.Cleanup(func() { _ = handler.Close() })

	cases := []struct {
		name, path string
		wantCode   int
		wantBody   string
	}{
		{"health", "/v1/health", http.StatusOK, ""},
		{"get", "/v1/messages/msg-1", http.StatusOK, "msg-1"},
		{"not-found", "/v1/messages/nope", http.StatusNotFound, ""},
	}
	for _, c := range cases {
		t.Run(c.name, func(t *testing.T) {
			rec := httptest.NewRecorder()
			handler.ServeHTTP(rec, httptest.NewRequest(http.MethodGet, c.path, nil))
			if rec.Code != c.wantCode {
				t.Errorf("GET %s = %d, want %d; body: %s", c.path, rec.Code, c.wantCode, rec.Body.String())
			}
			if c.wantBody != "" && !strings.Contains(rec.Body.String(), c.wantBody) {
				t.Errorf("GET %s body missing %q: %s", c.path, c.wantBody, rec.Body.String())
			}
		})
	}
}
