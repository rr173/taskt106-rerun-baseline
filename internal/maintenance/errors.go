package maintenance

import "errors"

var (
	ErrInvalidWindow = errors.New("maintenance window is invalid")
	ErrWindowOverlap = errors.New("maintenance window overlaps an existing window")
	ErrWindowClosed  = errors.New("maintenance window is already closed")
)
