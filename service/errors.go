package service

import (
	"context"
	"errors"

	scheduler "github.com/rbaliyan/event-scheduler"
	eventerrors "github.com/rbaliyan/event/v3/errors"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

// toGRPCError converts scheduler errors to gRPC status errors.
func toGRPCError(err error) error {
	if err == nil {
		return nil
	}

	// Check both scheduler.ErrNotFound (wraps eventerrors.ErrNotFound)
	// and eventerrors.ErrNotFound directly, since NewNotFoundError
	// returns an eventerrors.NotFoundError that unwraps to eventerrors.ErrNotFound.
	switch {
	case errors.Is(err, scheduler.ErrNotFound),
		errors.Is(err, eventerrors.ErrNotFound):
		return status.Error(codes.NotFound, err.Error())
	case errors.Is(err, context.Canceled):
		return status.Error(codes.Canceled, err.Error())
	case errors.Is(err, context.DeadlineExceeded):
		return status.Error(codes.DeadlineExceeded, err.Error())
	default:
		return status.Errorf(codes.Internal, "internal error: %v", err)
	}
}
