package coordination

import "errors"

var (
	ErrInvalidRequest = errors.New("coordination: invalid request")
	ErrNotAcquired    = errors.New("coordination: not acquired")
	ErrLeaseLost      = errors.New("coordination: lease lost")
	ErrNotOwner       = errors.New("coordination: not owner")
	ErrClosed         = errors.New("coordination: closed")
)
