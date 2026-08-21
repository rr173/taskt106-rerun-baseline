package namespace

import "errors"

var (
	ErrEmptyPath    = errors.New("resource path is empty")
	ErrInvalidPath  = errors.New("resource path is invalid")
	ErrPathCycle    = errors.New("resource path creates a cycle")
	ErrOutsideScope = errors.New("resource is outside requested scope")
)
