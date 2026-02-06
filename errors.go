package scheduler

import (
	"fmt"

	eventerrors "github.com/rbaliyan/event/v3/errors"
)

// ErrNotFound is returned when a scheduled message cannot be found.
// Use errors.Is(err, ErrNotFound) to check for this condition.
//
// This error wraps the shared event errors package error, so both
// errors.Is(err, scheduler.ErrNotFound) and errors.Is(err, eventerrors.ErrNotFound)
// will work for error checking.
var ErrNotFound = fmt.Errorf("scheduled message %w", eventerrors.ErrNotFound)

// NewNotFoundError creates a detailed not found error for a scheduled message.
func NewNotFoundError(id string) error {
	return eventerrors.NewNotFoundError("scheduled message", id)
}
