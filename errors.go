package scheduler

import "errors"

// ErrNotFound is returned when a scheduled message cannot be found.
// Use errors.Is(err, ErrNotFound) to check for this condition.
var ErrNotFound = errors.New("scheduled message not found")
