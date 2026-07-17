package coordination

import (
	"context"
	"errors"
	"sync"
	"time"
)

type Mutex struct {
	backend    Backend
	key, owner string
	ttl        time.Duration
	mu         sync.Mutex
	lease      Lease
}

func NewMutex(backend Backend, key, owner string, ttl time.Duration) (*Mutex, error) {
	if backend == nil || key == "" || owner == "" || ttl <= 0 {
		return nil, ErrInvalidRequest
	}
	return &Mutex{backend: backend, key: key, owner: owner, ttl: ttl}, nil
}

func (m *Mutex) Lock(ctx context.Context) (FencingToken, error) {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lease != nil {
		return 0, ErrNotOwner
	}

	lease, err := m.backend.Acquire(ctx, AcquireRequest{
		Key:   m.key,
		Owner: m.owner,
		TTL:   m.ttl,
		Wait:  true,
	})
	if err != nil {
		return 0, err
	}
	m.lease = lease
	return lease.Info().Token, nil
}

func (m *Mutex) Unlock(ctx context.Context) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.lease == nil {
		return ErrNotOwner
	}

	err := m.lease.Release(ctx)
	if err == nil || errors.Is(err, ErrLeaseLost) {
		m.lease = nil
	}
	return err
}
