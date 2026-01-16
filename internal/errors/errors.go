package errors

import "errors"

var (
	ErrRateLimited = errors.New("rate limited")
	ErrDuplicate   = errors.New("duplicate event")
)
