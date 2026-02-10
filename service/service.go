// Package service provides a gRPC implementation of the SchedulerService.
package service

import (
	"context"
	"errors"

	scheduler "github.com/rbaliyan/event-scheduler"
	schedulerpb "github.com/rbaliyan/event-scheduler/proto/scheduler/v1"
	"github.com/rbaliyan/event/v3/health"
	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// maxListLimit is the maximum number of messages that can be returned by List.
const maxListLimit = 1000

// Service implements the SchedulerService gRPC server.
type Service struct {
	schedulerpb.UnimplementedSchedulerServiceServer

	scheduler scheduler.Scheduler
}

// New creates a new SchedulerService.
func New(sched scheduler.Scheduler) (*Service, error) {
	if sched == nil {
		return nil, errors.New("scheduler-service: scheduler must not be nil")
	}
	return &Service{
		scheduler: sched,
	}, nil
}

// Register registers the service with a gRPC server.
func (s *Service) Register(server *grpc.Server) {
	schedulerpb.RegisterSchedulerServiceServer(server, s)
}

// Get retrieves a scheduled message by ID.
func (s *Service) Get(ctx context.Context, req *schedulerpb.GetRequest) (*schedulerpb.GetResponse, error) {
	if req.GetId() == "" {
		return nil, status.Error(codes.InvalidArgument, "id is required")
	}

	msg, err := s.scheduler.Get(ctx, req.GetId())
	if err != nil {
		return nil, toGRPCError(err)
	}

	return &schedulerpb.GetResponse{
		Message: messageToProto(msg),
	}, nil
}

// List returns scheduled messages matching a filter.
func (s *Service) List(ctx context.Context, req *schedulerpb.ListRequest) (*schedulerpb.ListResponse, error) {
	filter := protoToFilter(req)

	// Clamp limit
	if filter.Limit <= 0 || filter.Limit > maxListLimit {
		filter.Limit = maxListLimit
	}

	msgs, err := s.scheduler.List(ctx, filter)
	if err != nil {
		return nil, toGRPCError(err)
	}

	pbMsgs := make([]*schedulerpb.Message, len(msgs))
	for i, m := range msgs {
		pbMsgs[i] = messageToProto(m)
	}

	return &schedulerpb.ListResponse{
		Messages:   pbMsgs,
		TotalCount: int32(len(pbMsgs)),
	}, nil
}

// Health returns the scheduler health status.
func (s *Service) Health(ctx context.Context, _ *schedulerpb.HealthRequest) (*schedulerpb.HealthResponse, error) {
	checker, ok := s.scheduler.(health.Checker)
	if !ok {
		return &schedulerpb.HealthResponse{
			Status:  schedulerpb.HealthStatus_HEALTH_STATUS_HEALTHY,
			Message: "health check not supported by this scheduler implementation",
		}, nil
	}

	result := checker.Health(ctx)
	return healthToProto(result), nil
}
