package coordination

import (
	"context"
	"errors"
	"sync"
	"time"
)

// Mutex is a fenced mutual-exclusion lock over a [Backend] for one key and
// one owner. A Mutex value holds at most one lease at a time and does not
// renew it: choose a TTL that bounds the critical section or use [KeepAlive]
// on a raw [Lease].
//
// A Mutex is safe for concurrent use, but concurrent Lock calls on the same
// value do not queue: the second returns [ErrNotOwner].
type Mutex struct {
	backend    Backend
	key, owner string
	ttl        time.Duration
	mu         sync.Mutex
	lease      Lease
	locking    bool
}

// NewMutex returns a Mutex for key and owner whose leases last ttl. It
// returns [ErrInvalidRequest] for a nil backend, an empty key or owner, or a
// non-positive ttl.
func NewMutex(backend Backend, key, owner string, ttl time.Duration) (*Mutex, error) {
	if backend == nil || key == "" || owner == "" || ttl <= 0 {
		return nil, ErrInvalidRequest
	}
	return &Mutex{backend: backend, key: key, owner: owner, ttl: ttl}, nil
}

// Lock waits until the lease for the mutex key is acquired and returns its
// fencing token, which callers pass to the protected resource. It returns
// [ErrNotOwner] promptly if this Mutex already holds or is acquiring a lease,
// and the context's error if ctx is done while waiting.
func (m *Mutex) Lock(ctx context.Context) (FencingToken, error) {
	m.mu.Lock()
	if m.lease != nil || m.locking {
		m.mu.Unlock()
		return 0, ErrNotOwner
	}
	m.locking = true
	m.mu.Unlock()

	lease, err := m.backend.Acquire(ctx, AcquireRequest{
		Key:   m.key,
		Owner: m.owner,
		TTL:   m.ttl,
		Wait:  true,
	})
	m.mu.Lock()
	defer m.mu.Unlock()
	m.locking = false
	if err != nil {
		return 0, err
	}
	m.lease = lease
	return lease.Info().Token, nil
}

// Unlock releases the held lease. It returns [ErrNotOwner] when no lease is
// held. If the lease was already lost it returns [ErrLeaseLost] and the
// Mutex may be locked again; other release errors keep the lease so Unlock
// can be retried.
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
