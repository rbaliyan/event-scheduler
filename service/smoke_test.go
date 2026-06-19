package service

import (
	"context"
	"net"
	"testing"
	"time"

	scheduler "github.com/rbaliyan/event-scheduler"
	schedulerpb "github.com/rbaliyan/event-scheduler/proto/scheduler/v1"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
	"google.golang.org/grpc/test/bufconn"
)

// dialBufconn stands up the gRPC service over an in-process bufconn listener and
// returns a connected client — the real serialization/interceptor path, no TCP.
func dialBufconn(t *testing.T, sched scheduler.Scheduler) schedulerpb.SchedulerServiceClient {
	t.Helper()
	svc, err := New(sched)
	if err != nil {
		t.Fatalf("New: %v", err)
	}
	lis := bufconn.Listen(1 << 20)
	srv := grpc.NewServer()
	svc.Register(srv)
	go func() { _ = srv.Serve(lis) }()
	t.Cleanup(srv.Stop)

	conn, err := grpc.NewClient(
		"passthrough:///bufnet",
		grpc.WithContextDialer(func(ctx context.Context, _ string) (net.Conn, error) { return lis.DialContext(ctx) }),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
	)
	if err != nil {
		t.Fatalf("NewClient: %v", err)
	}
	t.Cleanup(func() { _ = conn.Close() })
	return schedulerpb.NewSchedulerServiceClient(conn)
}

// TestSmoke_ServiceOverBufconn exercises Get/List end-to-end over the wire.
func TestSmoke_ServiceOverBufconn(t *testing.T) {
	t.Parallel()
	mock := newMockScheduler()
	mock.messages["msg-1"] = &scheduler.Message{
		ID: "msg-1", EventName: "orders.created", Payload: []byte("p"),
		ScheduledAt: time.Now().Add(time.Hour),
	}
	mock.listMsgs = []*scheduler.Message{mock.messages["msg-1"]}

	client := dialBufconn(t, mock)
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	getResp, err := client.Get(ctx, &schedulerpb.GetRequest{Id: "msg-1"})
	if err != nil {
		t.Fatalf("Get over wire: %v", err)
	}
	if getResp.GetMessage().GetId() != "msg-1" {
		t.Errorf("Get returned id %q, want msg-1", getResp.GetMessage().GetId())
	}

	listResp, err := client.List(ctx, &schedulerpb.ListRequest{EventName: "orders.created", Limit: 10})
	if err != nil {
		t.Fatalf("List over wire: %v", err)
	}
	if len(listResp.GetMessages()) != 1 {
		t.Errorf("List returned %d messages, want 1", len(listResp.GetMessages()))
	}
}

// TestSmoke_ServiceNotFoundOverBufconn verifies the ErrNotFound -> gRPC
// codes.NotFound mapping across the real wire (the service's error contract).
func TestSmoke_ServiceNotFoundOverBufconn(t *testing.T) {
	t.Parallel()
	client := dialBufconn(t, newMockScheduler())
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	_, err := client.Get(ctx, &schedulerpb.GetRequest{Id: "nope"})
	if status.Code(err) != codes.NotFound {
		t.Errorf("Get(missing) code = %v, want NotFound", status.Code(err))
	}
}
