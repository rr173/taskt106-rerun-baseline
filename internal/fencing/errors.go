package fencing

import "errors"

var (
	ErrTokenNotFound = errors.New("fencing token not found")
	ErrTokenRevoked  = errors.New("fencing token revoked")
	ErrTokenExpired  = errors.New("fencing token expired")
	ErrTokenMismatch = errors.New("fencing token does not match resource or holder")
	ErrTokenStale    = errors.New("fencing token is stale")
)
