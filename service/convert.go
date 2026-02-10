package service

import (
	"encoding/json"
	"fmt"

	scheduler "github.com/rbaliyan/event-scheduler"
	schedulerpb "github.com/rbaliyan/event-scheduler/proto/scheduler/v1"
	"github.com/rbaliyan/event/v3/health"
	"google.golang.org/protobuf/types/known/timestamppb"
)

// healthStatusToProto maps health.Status to the proto HealthStatus enum.
var healthStatusToProto = map[health.Status]schedulerpb.HealthStatus{
	health.StatusHealthy:   schedulerpb.HealthStatus_HEALTH_STATUS_HEALTHY,
	health.StatusDegraded:  schedulerpb.HealthStatus_HEALTH_STATUS_DEGRADED,
	health.StatusUnhealthy: schedulerpb.HealthStatus_HEALTH_STATUS_UNHEALTHY,
}

// messageToProto converts a scheduler.Message to a proto Message.
func messageToProto(m *scheduler.Message) *schedulerpb.Message {
	if m == nil {
		return nil
	}

	pb := &schedulerpb.Message{
		Id:         m.ID,
		EventName:  m.EventName,
		Payload:    m.Payload,
		Metadata:   m.Metadata,
		RetryCount: int32(m.RetryCount),
	}

	if !m.ScheduledAt.IsZero() {
		pb.ScheduledAt = timestamppb.New(m.ScheduledAt)
	}
	if !m.CreatedAt.IsZero() {
		pb.CreatedAt = timestamppb.New(m.CreatedAt)
	}

	return pb
}

// protoToFilter converts a proto ListRequest to a scheduler.Filter.
func protoToFilter(req *schedulerpb.ListRequest) scheduler.Filter {
	f := scheduler.Filter{
		EventName: req.GetEventName(),
		Limit:     int(req.GetLimit()),
	}

	if req.GetBefore() != nil {
		f.Before = req.GetBefore().AsTime()
	}
	if req.GetAfter() != nil {
		f.After = req.GetAfter().AsTime()
	}

	return f
}

// healthToProto converts a health.Result to a proto HealthResponse.
func healthToProto(r *health.Result) *schedulerpb.HealthResponse {
	if r == nil {
		return &schedulerpb.HealthResponse{
			Status: schedulerpb.HealthStatus_HEALTH_STATUS_UNHEALTHY,
		}
	}

	resp := &schedulerpb.HealthResponse{
		Status:    healthStatusToProto[r.Status],
		Message:   r.Message,
		LatencyMs: r.Latency.Milliseconds(),
	}

	if len(r.Details) > 0 {
		resp.Details = make(map[string]string, len(r.Details))
		for k, v := range r.Details {
			resp.Details[k] = detailValueToString(v)
		}
	}

	return resp
}

// detailValueToString converts a health detail value to a string representation.
func detailValueToString(v any) string {
	switch val := v.(type) {
	case string:
		return val
	case error:
		return val.Error()
	default:
		if b, err := json.Marshal(val); err == nil {
			return string(b)
		}
		return fmt.Sprintf("%v", v)
	}
}
