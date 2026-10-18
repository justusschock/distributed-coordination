package coordination

import "errors"

// Sentinel errors returned by the coordination helpers and by conforming
// backends. Test for them with [errors.Is]; implementations may wrap them.
var (
	// ErrInvalidRequest reports an empty required field, a non-positive
	// count or a non-positive TTL.
	ErrInvalidRequest = errors.New("coordination: invalid request")
	// ErrNotAcquired reports immediate lease contention (Wait: false), a
	// duplicate active membership, or a barrier arrival whose TTL expired.
	ErrNotAcquired = errors.New("coordination: not acquired")
	// ErrLeaseLost reports that a lease or membership expired, was released
	// or was replaced, so it can no longer be renewed or released.
	ErrLeaseLost = errors.New("coordination: lease lost")
	// ErrNotOwner reports a Mutex or Leadership operation without matching
	// active ownership.
	ErrNotOwner = errors.New("coordination: not owner")
)
