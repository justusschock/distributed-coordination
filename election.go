package coordination

import (
	"context"
	"sync"
	"time"
)

type ElectionOptions struct {
	Name      string
	Candidate string
	TTL       time.Duration
}

type Leadership struct {
	Token  FencingToken
	Lost   <-chan struct{}
	resign func(context.Context) error
}

func (l Leadership) Resign(ctx context.Context) error {
	if l.resign == nil {
		return ErrNotOwner
	}
	return l.resign(ctx)
}

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
