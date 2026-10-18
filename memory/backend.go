// Package memory provides a synchronized, in-process reference
// implementation of the coordination contracts.
//
// [Backend] implements [coordination.Backend], [coordination.Registry] and
// [coordination.Barrier]. It is deterministic under an injected
// [coordination.Clock] (or inside a testing/synctest bubble with the real
// clock), which makes it suitable for tests and for coordinating goroutines
// of one process. It does not coordinate separate processes and keeps no
// state across restarts.
package memory

import (
	"context"
	"sync"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
)

// Backend is the in-memory reference backend. Its zero value is not usable;
// create one with [NewBackend]. A Backend is safe for concurrent use.
//
// Fencing tokens start at 1 per key and survive release and expiry for the
// lifetime of the Backend.
type Backend struct {
	mu        sync.Mutex
	clock     coordination.Clock
	records   map[string]*record
	nextToken map[string]coordination.FencingToken
	members   map[string]map[string]*memberRecord
	barriers  map[string]*barrierState
}

type record struct {
	info    coordination.LeaseInfo
	done    chan struct{}
	changed chan struct{}
	lost    bool
}

type lease struct {
	backend *Backend
	record  *record
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) After(delay time.Duration) <-chan time.Time {
	return time.After(delay)
}

// NewBackend returns an empty Backend that reads time from clock. A nil clock
// selects the real clock.
func NewBackend(clock coordination.Clock) *Backend {
	if clock == nil {
		clock = realClock{}
	}
	return &Backend{
		clock:     clock,
		records:   make(map[string]*record),
		nextToken: make(map[string]coordination.FencingToken),
		members:   make(map[string]map[string]*memberRecord),
		barriers:  make(map[string]*barrierState),
	}
}

// Acquire implements [coordination.Backend]. A waiting acquisition wakes on
// release or at the holder's deadline and honours ctx.
func (b *Backend) Acquire(ctx context.Context, request coordination.AcquireRequest) (coordination.Lease, error) {
	if request.Key == "" || request.Owner == "" || request.TTL <= 0 {
		return nil, coordination.ErrInvalidRequest
	}

	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		b.mu.Lock()
		now := b.clock.Now()
		current := b.records[request.Key]
		if current != nil && !current.info.ExpiresAt.After(now) {
			b.loseLocked(request.Key, current)
			current = nil
		}

		if current == nil {
			b.nextToken[request.Key]++
			current = &record{
				info: coordination.LeaseInfo{
					Key:        request.Key,
					Owner:      request.Owner,
					Token:      b.nextToken[request.Key],
					AcquiredAt: now,
					ExpiresAt:  now.Add(request.TTL),
				},
				done:    make(chan struct{}),
				changed: make(chan struct{}),
			}
			b.records[request.Key] = current
			b.mu.Unlock()
			b.scheduleExpiry(current, request.TTL)
			return &lease{backend: b, record: current}, nil
		}

		if !request.Wait {
			b.mu.Unlock()
			return nil, coordination.ErrNotAcquired
		}

		changed := current.changed
		expiry := b.clock.After(current.info.ExpiresAt.Sub(now))
		b.mu.Unlock()

		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-changed:
		case <-expiry:
			b.expire(current)
		}
	}
}

func (b *Backend) scheduleExpiry(current *record, delay time.Duration) {
	expiry := b.clock.After(delay)
	go func() {
		select {
		case <-current.done:
		case <-expiry:
			b.expire(current)
		}
	}()
}

func (b *Backend) expire(current *record) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if b.records[current.info.Key] != current || current.lost {
		return
	}
	if current.info.ExpiresAt.After(b.clock.Now()) {
		return
	}
	b.loseLocked(current.info.Key, current)
}

func (b *Backend) loseLocked(key string, current *record) {
	if b.records[key] != current || current.lost {
		return
	}
	current.lost = true
	delete(b.records, key)
	close(current.done)
	close(current.changed)
}

func (l *lease) Info() coordination.LeaseInfo {
	l.backend.mu.Lock()
	defer l.backend.mu.Unlock()
	return l.record.info
}

func (l *lease) Done() <-chan struct{} {
	return l.record.done
}

func (l *lease) Renew(ctx context.Context, ttl time.Duration) (coordination.LeaseInfo, error) {
	if ttl <= 0 {
		return coordination.LeaseInfo{}, coordination.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return coordination.LeaseInfo{}, err
	}

	b := l.backend
	b.mu.Lock()
	current := b.records[l.record.info.Key]
	if current != l.record || current.info.Key != l.record.info.Key || current.info.Owner != l.record.info.Owner || current.info.Token != l.record.info.Token || current.lost {
		b.mu.Unlock()
		return coordination.LeaseInfo{}, coordination.ErrLeaseLost
	}
	now := b.clock.Now()
	if !current.info.ExpiresAt.After(now) {
		b.loseLocked(current.info.Key, current)
		b.mu.Unlock()
		return coordination.LeaseInfo{}, coordination.ErrLeaseLost
	}
	current.info.ExpiresAt = now.Add(ttl)
	info := current.info
	b.mu.Unlock()

	b.scheduleExpiry(current, ttl)
	return info, nil
}

func (l *lease) Release(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	b := l.backend
	b.mu.Lock()
	defer b.mu.Unlock()
	current := b.records[l.record.info.Key]
	if current != l.record || current.info.Key != l.record.info.Key || current.info.Owner != l.record.info.Owner || current.info.Token != l.record.info.Token || current.lost {
		return coordination.ErrLeaseLost
	}
	if !current.info.ExpiresAt.After(b.clock.Now()) {
		b.loseLocked(current.info.Key, current)
		return coordination.ErrLeaseLost
	}
	b.loseLocked(current.info.Key, current)
	return nil
}
