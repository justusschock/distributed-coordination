package coordination

import (
	"context"
	"time"
)

type FencingToken uint64

type AcquireRequest struct {
	Key   string
	Owner string
	TTL   time.Duration
	Wait  bool
}

type LeaseInfo struct {
	Key        string
	Owner      string
	Token      FencingToken
	AcquiredAt time.Time
	ExpiresAt  time.Time
}

type Lease interface {
	Info() LeaseInfo
	Done() <-chan struct{}
	Renew(context.Context, time.Duration) (LeaseInfo, error)
	Release(context.Context) error
}

type Backend interface {
	Acquire(context.Context, AcquireRequest) (Lease, error)
}

type Clock interface {
	Now() time.Time
	After(time.Duration) <-chan time.Time
}
