package resource

import "errors"

var (
	ErrNotFound          = errors.New("resource not found")
	ErrRetired           = errors.New("resource is retired")
	ErrDraining          = errors.New("resource is draining")
	ErrPolicyDenied      = errors.New("holder is denied by resource policy")
	ErrLeaseTooLong      = errors.New("requested lease exceeds resource policy")
	ErrInvalidTransition = errors.New("invalid resource state transition")
)
