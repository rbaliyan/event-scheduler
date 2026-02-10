// Package gateway provides an HTTP handler for the SchedulerService using gRPC-Gateway.
package gateway

import (
	"context"
	"errors"
	"net/http"
	"sync"

	"github.com/grpc-ecosystem/grpc-gateway/v2/runtime"
	schedulerpb "github.com/rbaliyan/event-scheduler/proto/scheduler/v1"
	"google.golang.org/grpc"
)

// Handler is an HTTP handler for the SchedulerService gateway.
// For handlers created by NewHandler, Close releases the underlying gRPC
// connection. For handlers created by NewInProcessHandler, Close is a no-op.
type Handler struct {
	http.Handler
	closeOnce sync.Once
	closeErr  error
	conn      *grpc.ClientConn // nil for in-process handlers
	done      chan struct{}
}

// Close releases resources held by the handler.
// For remote handlers, this closes the gRPC connection to the backend.
// Close is safe to call multiple times; all calls return the same error.
func (h *Handler) Close() error {
	h.closeOnce.Do(func() {
		close(h.done)
		if h.conn != nil {
			h.closeErr = h.conn.Close()
		}
	})
	return h.closeErr
}

// NewHandler creates a Handler that proxies to the gRPC service.
// The handler can be mounted on any HTTP router.
//
// The caller should call Close when the handler is no longer needed to release
// the gRPC connection. Alternatively, cancelling ctx will also close the
// connection, but calling Close is preferred for deterministic cleanup.
//
// By default, connections are insecure. Use WithTLS for production.
func NewHandler(ctx context.Context, grpcAddr string, opts ...Option) (*Handler, error) {
	if grpcAddr == "" {
		return nil, errors.New("gateway: gRPC address must not be empty")
	}

	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	dialOpts := o.buildDialOpts()

	conn, err := grpc.NewClient(grpcAddr, dialOpts...)
	if err != nil {
		return nil, err
	}

	mux := runtime.NewServeMux(o.muxOpts...)

	if err := schedulerpb.RegisterSchedulerServiceHandler(ctx, mux, conn); err != nil {
		_ = conn.Close()
		return nil, err
	}

	h := &Handler{
		Handler: mux,
		conn:    conn,
		done:    make(chan struct{}),
	}

	go func() {
		select {
		case <-ctx.Done():
		case <-h.done:
		}
		_ = h.Close()
	}()

	return h, nil
}

// NewInProcessHandler creates a handler that calls the service directly
// without going through a network connection. This is more efficient
// when the gRPC service and HTTP gateway run in the same process.
func NewInProcessHandler(ctx context.Context, svc schedulerpb.SchedulerServiceServer, opts ...Option) (*Handler, error) {
	o := &options{}
	for _, opt := range opts {
		opt(o)
	}

	mux := runtime.NewServeMux(o.muxOpts...)

	if err := schedulerpb.RegisterSchedulerServiceHandlerServer(ctx, mux, svc); err != nil {
		return nil, err
	}

	return &Handler{
		Handler: mux,
		done:    make(chan struct{}),
	}, nil
}
