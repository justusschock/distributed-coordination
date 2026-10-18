package coordination

import (
	"context"
	"time"
)

// FencingToken orders the leases granted for one key. Each successful
// acquisition of a key receives a token strictly greater than every token
// previously granted for that key. Tokens of different keys are unrelated
// and must not be compared.
type FencingToken uint64

// AcquireRequest describes a lease acquisition.
type AcquireRequest struct {
	// Key names the coordinated resource. It must not be empty.
	Key string
	// Owner identifies the requester. It must not be empty and is recorded
	// in [LeaseInfo]; backends do not interpret it.
	Owner string
	// TTL bounds the lease lifetime from the moment of acquisition. It must
	// be positive.
	TTL time.Duration
	// Wait selects the behaviour under contention: false returns
	// [ErrNotAcquired] immediately, true waits for release or expiry until
	// the context is done.
	Wait bool
}

// LeaseInfo is a snapshot of a lease.
type LeaseInfo struct {
	Key        string
	Owner      string
	Token      FencingToken
	AcquiredAt time.Time
	ExpiresAt  time.Time
}

// Lease is exclusive, time-bounded ownership of one key. Implementations must
// be safe for concurrent use.
type Lease interface {
	// Info returns the latest snapshot of the lease, including renewals.
	Info() LeaseInfo
	// Done is closed exactly once when the lease is released or lost
	// (expiry, replacement or, for some backends, loss of the underlying
	// lock).
	Done() <-chan struct{}
	// Renew extends the deadline to the backend clock's current time plus
	// ttl. It returns [ErrInvalidRequest] for a non-positive ttl and
	// [ErrLeaseLost] after release, expiry or replacement.
	Renew(ctx context.Context, ttl time.Duration) (LeaseInfo, error)
	// Release gives up the lease and closes Done. It returns [ErrLeaseLost]
	// if the lease was already released, expired or replaced.
	Release(ctx context.Context) error
}

// Backend grants fenced leases. Implementations must be safe for concurrent
// use and must pass the conformance suite in package conformance.
type Backend interface {
	// Acquire grants a lease on request.Key. It returns [ErrInvalidRequest]
	// for an empty key or owner or a non-positive TTL, [ErrNotAcquired] when
	// the key is held and request.Wait is false, and the context's error
	// when ctx is done while waiting.
	Acquire(ctx context.Context, request AcquireRequest) (Lease, error)
}

// Clock is the time source a backend uses for lease timestamps and expiry.
// Backends accept a Clock so adapters and tests can control time; pass nil
// to the constructors in this module to use the real clock.
type Clock interface {
	// Now returns the current time.
	Now() time.Time
	// After returns a channel that receives the time once d has elapsed.
	After(d time.Duration) <-chan time.Time
}
