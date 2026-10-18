package conformance_test

import (
	"sync"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	"github.com/justusschock/go-distributed-coordination/conformance"
	"github.com/justusschock/go-distributed-coordination/file"
	"github.com/justusschock/go-distributed-coordination/memory"
)

// FR-COORD-17, FR-COORD-11
func TestMemoryBackend(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (coordination.Backend, func(time.Duration)) {
		t.Helper()
		clock := newFakeClock(time.Unix(100, 0))
		return memory.NewBackend(clock), clock.Advance
	})
}

// FR-COORD-17
func TestFileBackend(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (coordination.Backend, func(time.Duration)) {
		t.Helper()
		clock := newFakeClock(time.Unix(100, 0))
		backend, err := file.NewBackend(t.TempDir(), file.WithClock(clock))
		if err != nil {
			t.Fatal(err)
		}
		return backend, clock.Advance
	})
}

// fakeClock is the manual coordination.Clock behind the Factory's advance
// hook. These tests cannot use testing/synctest instead: Run creates subtests
// with t.Run, which is not allowed inside a synctest bubble.
type fakeTimer struct {
	at time.Time
	ch chan time.Time
}

type fakeClock struct {
	mu     sync.Mutex
	now    time.Time
	timers []*fakeTimer
}

func newFakeClock(now time.Time) *fakeClock {
	return &fakeClock{now: now}
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
