package coordination_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
)

// FR-COORD-06
func TestKeepAliveRenewsImmediatelyThenAtOneThirdTTL(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lease := newTestLease()
		lease.failAfter = 3
		lease.renewErr = coordination.ErrLeaseLost
		start := time.Now()

		future, err := coordination.KeepAlive(context.Background(), lease, 30*time.Millisecond)
		if err != nil {
			t.Fatal(err)
		}

		for i, want := range []time.Duration{0, 10 * time.Millisecond, 20 * time.Millisecond} {
			synctest.Wait()
			select {
			case call := <-lease.renewed:
				if call.ttl != 30*time.Millisecond {
					t.Fatalf("renewal %d TTL = %v, want 30ms", i, call.ttl)
				}
				if elapsed := call.at.Sub(start); elapsed != want {
					t.Fatalf("renewal %d after %v, want %v", i, elapsed, want)
				}
			default:
				t.Fatalf("renewal %d did not happen", i)
			}
			time.Sleep(10*time.Millisecond - time.Nanosecond)
			synctest.Wait()
			if len(lease.renewed) != 0 {
				t.Fatalf("renewal %d was followed by a renewal before ttl/3", i)
			}
			time.Sleep(time.Nanosecond)
		}

		info, err := future.Await(context.Background())
		if !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("Await() error = %v, want ErrLeaseLost", err)
		}
		if info.Token != 1 {
			t.Fatalf("Await() token = %d, want 1", info.Token)
		}
	})
}

// FR-COORD-06
func TestKeepAliveValidatesArgumentsAndReportsCancellationCause(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		if _, err := coordination.KeepAlive(context.Background(), nil, time.Second); !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Fatalf("KeepAlive(nil) error = %v, want ErrInvalidRequest", err)
		}
		if _, err := coordination.KeepAlive(context.Background(), newTestLease(), 0); !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Fatalf("KeepAlive(ttl=0) error = %v, want ErrInvalidRequest", err)
		}

		cause := errors.New("stop renewal")
		ctx, cancel := context.WithCancelCause(context.Background())
		lease := newTestLease()
		future, err := coordination.KeepAlive(ctx, lease, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if len(lease.renewed) != 1 {
			t.Fatalf("initial renewals = %d, want 1", len(lease.renewed))
		}
		cancel(cause)

		if _, err := future.Await(context.Background()); !errors.Is(err, cause) {
			t.Fatalf("Await() error = %v, want %v", err, cause)
		}
	})
}

// FR-COORD-06
func TestKeepAliveStopsWhenLeaseIsLost(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		lease := newTestLease()
		future, err := coordination.KeepAlive(context.Background(), lease, time.Hour)
		if err != nil {
			t.Fatal(err)
		}
		synctest.Wait()
		if err := lease.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		if _, err := future.Await(context.Background()); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("Await() error = %v, want ErrLeaseLost", err)
		}
		if len(lease.renewed) != 1 {
			t.Fatalf("renewals = %d, want only the initial renewal", len(lease.renewed))
		}
	})
}

type renewCall struct {
	ttl time.Duration
	at  time.Time
}

type testLease struct {
	mu        sync.Mutex
	info      coordination.LeaseInfo
	done      chan struct{}
	doneOnce  sync.Once
	renewed   chan renewCall
	renewals  int
	failAfter int
	renewErr  error
	releases  int
}

func newTestLease() *testLease {
	return &testLease{
		info: coordination.LeaseInfo{
			Key:        "key",
			Owner:      "owner",
			Token:      1,
			AcquiredAt: time.Now(),
			ExpiresAt:  time.Now().Add(time.Second),
		},
		done:    make(chan struct{}),
		renewed: make(chan renewCall, 8),
	}
}

func (l *testLease) Info() coordination.LeaseInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.info
}

func (l *testLease) Done() <-chan struct{} {
	return l.done
}

func (l *testLease) Renew(_ context.Context, ttl time.Duration) (coordination.LeaseInfo, error) {
	l.mu.Lock()
	defer l.mu.Unlock()
	l.renewals++
	l.renewed <- renewCall{ttl: ttl, at: time.Now()}
	if l.failAfter > 0 && l.renewals >= l.failAfter {
		return l.info, l.renewErr
	}
	l.info.ExpiresAt = time.Now().Add(ttl)
	return l.info, nil
}

func (l *testLease) Release(context.Context) error {
	l.mu.Lock()
	l.releases++
	l.mu.Unlock()
	l.doneOnce.Do(func() { close(l.done) })
	return nil
}
