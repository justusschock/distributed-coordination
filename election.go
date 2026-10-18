package coordination

import (
	"context"
	"sync"
	"time"
)

// ElectionOptions configures [Campaign].
type ElectionOptions struct {
	// Name identifies the election; the lease key is "election/" + Name.
	Name string
	// Candidate identifies this participant and becomes the lease owner.
	Candidate string
	// TTL is the leadership lease lifetime, renewed every TTL/3.
	TTL time.Duration
}

// Leadership is won leadership of an election. It may be copied; all copies
// share the same lease.
type Leadership struct {
	// Token is the fencing token of the leadership lease.
	Token FencingToken
	// Lost is closed when the lease is lost or its keepalive stops, including
	// after Resign or cancellation of the Campaign context.
	Lost   <-chan struct{}
	resign func(context.Context) error
}

// Resign stops the keepalive and releases the leadership lease. The release
// happens at most once; every call returns the first call's result. The zero
// Leadership returns [ErrNotOwner].
func (l Leadership) Resign(ctx context.Context) error {
	if l.resign == nil {
		return ErrNotOwner
	}
	return l.resign(ctx)
}

// Campaign waits until it acquires the election lease for options.Name, then
// keeps it alive with [KeepAlive] until ctx is done, the lease is lost or
// [Leadership.Resign] is called. It returns [ErrInvalidRequest] for a nil
// backend, an empty name or candidate, or a non-positive TTL, and otherwise
// the backend's acquisition error.
func Campaign(ctx context.Context, backend Backend, options ElectionOptions) (Leadership, error) {
	if backend == nil || options.Name == "" || options.Candidate == "" || options.TTL <= 0 {
		return Leadership{}, ErrInvalidRequest
	}

	lease, err := backend.Acquire(ctx, AcquireRequest{
		Key:   "election/" + options.Name,
		Owner: options.Candidate,
		TTL:   options.TTL,
		Wait:  true,
	})
	if err != nil {
		return Leadership{}, err
	}

	keepaliveCtx, stopKeepalive := context.WithCancel(ctx)
	keepalive, err := KeepAlive(keepaliveCtx, lease, options.TTL)
	if err != nil {
		stopKeepalive()
		_ = lease.Release(context.Background())
		return Leadership{}, err
	}

	lost := make(chan struct{})
	go func() {
		select {
		case <-lease.Done():
		case <-keepalive.Done():
		}
		close(lost)
	}()

	var (
		resignOnce sync.Once
		resignErr  error
	)
	resign := func(ctx context.Context) error {
		resignOnce.Do(func() {
			stopKeepalive()
			resignErr = lease.Release(ctx)
		})
		return resignErr
	}

	return Leadership{
		Token:  lease.Info().Token,
		Lost:   lost,
		resign: resign,
	}, nil
}
