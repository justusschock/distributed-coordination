package memory

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
)

func TestBackendFencingTokensNeverRepeat(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)

	first, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
		Key:   "jobs",
		Owner: "one",
		TTL:   time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if err := first.Release(context.Background()); err != nil {
		t.Fatal(err)
	}

	second, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
		Key:   "jobs",
		Owner: "two",
		TTL:   time.Minute,
	})
	if err != nil {
		t.Fatal(err)
	}
	if second.Info().Token <= first.Info().Token {
		t.Fatalf("tokens = %d then %d", first.Info().Token, second.Info().Token)
	}
}

func TestBackendExpiryLosesLeaseAndAllowsWaiter(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)

	first, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
		Key:   "jobs",
		Owner: "one",
		TTL:   time.Second,
	})
	if err != nil {
		t.Fatal(err)
	}

	result := make(chan coordination.Lease, 1)
	errs := make(chan error, 1)
	go func() {
		lease, acquireErr := backend.Acquire(context.Background(), coordination.AcquireRequest{
			Key:   "jobs",
			Owner: "two",
			TTL:   time.Second,
			Wait:  true,
		})
		if acquireErr != nil {
			errs <- acquireErr
			return
		}
		result <- lease
	}()

	clock.Advance(time.Second)

	select {
	case <-first.Done():
	case <-time.After(time.Second):
		t.Fatal("lease did not expire")
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case second := <-result:
		if second.Info().Token <= first.Info().Token {
			t.Fatal("token did not advance")
		}
	case <-time.After(time.Second):
		t.Fatal("waiter did not acquire")
	}
}

func TestBackendRejectsInvalidAndContendedAcquisitions(t *testing.T) {
	backend := NewBackend(nil)

	for _, request := range []coordination.AcquireRequest{
		{Owner: "owner", TTL: time.Second},
		{Key: "key", TTL: time.Second},
		{Key: "key", Owner: "owner"},
	} {
		if _, err := backend.Acquire(context.Background(), request); !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Fatalf("Acquire(%+v) error = %v, want ErrInvalidRequest", request, err)
		}
	}

	lease, err := backend.Acquire(context.Background(), coordination.AcquireRequest{Key: "key", Owner: "one", TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = lease.Release(context.Background()) })

	_, err = backend.Acquire(context.Background(), coordination.AcquireRequest{Key: "key", Owner: "two", TTL: time.Second})
	if !errors.Is(err, coordination.ErrNotAcquired) {
		t.Fatalf("contended Acquire() error = %v, want ErrNotAcquired", err)
	}
}

func TestBackendLostLeaseCannotRenewOrRelease(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)
	lease, err := backend.Acquire(context.Background(), coordination.AcquireRequest{Key: "key", Owner: "one", TTL: time.Second})
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(time.Second)
	select {
	case <-lease.Done():
	case <-time.After(time.Second):
		t.Fatal("lease did not expire")
	}

	if _, err := lease.Renew(context.Background(), time.Second); !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("Renew() error = %v, want ErrLeaseLost", err)
	}
	if err := lease.Release(context.Background()); !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("Release() error = %v, want ErrLeaseLost", err)
	}
}

type fakeTimer struct {
	at time.Time
	ch chan time.Time
}

type fakeClock struct {
	mu         sync.Mutex
	now        time.Time
	timers     []*fakeTimer
	timerAdded chan struct{}
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now, timerAdded: make(chan struct{}, 64)}
}

func (c *fakeClock) Now() time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.now
}

func (c *fakeClock) After(delay time.Duration) <-chan time.Time {
	c.mu.Lock()
	defer c.mu.Unlock()
	timer := &fakeTimer{at: c.now.Add(delay), ch: make(chan time.Time, 1)}
	c.timers = append(c.timers, timer)
	c.timerAdded <- struct{}{}
	return timer.ch
}

func (c *fakeClock) Advance(delay time.Duration) {
	c.mu.Lock()
	c.now = c.now.Add(delay)
	now := c.now
	remaining := c.timers[:0]
	var due []*fakeTimer
	for _, timer := range c.timers {
		if !timer.at.After(now) {
			due = append(due, timer)
		} else {
			remaining = append(remaining, timer)
		}
	}
	c.timers = remaining
	c.mu.Unlock()

	for _, timer := range due {
		timer.ch <- now
	}
}

func (c *fakeClock) waitForTimer(t *testing.T) {
	t.Helper()
	select {
	case <-c.timerAdded:
	case <-time.After(time.Second):
		t.Fatal("timer was not registered")
	}
}
