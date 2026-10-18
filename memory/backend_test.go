package memory

import (
	"context"
	"errors"
	"testing"
	"testing/synctest"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
)

// FR-COORD-04
func TestBackendFencingTokensNeverRepeat(t *testing.T) {
	backend := NewBackend(nil)

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
	t.Cleanup(func() { _ = second.Release(context.Background()) })
	if second.Info().Token <= first.Info().Token {
		t.Fatalf("tokens = %d then %d", first.Info().Token, second.Info().Token)
	}
}

// FR-COORD-03, FR-COORD-05
func TestBackendExpiryLosesLeaseAndAllowsWaiter(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(nil)
		start := time.Now()

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

		time.Sleep(time.Second - time.Nanosecond)
		synctest.Wait()
		if isClosed(first.Done()) || len(result) != 0 {
			t.Fatal("lease expired or waiter acquired before the deadline")
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if !isClosed(first.Done()) {
			t.Fatal("lease did not expire at its deadline")
		}
		select {
		case err := <-errs:
			t.Fatal(err)
		case second := <-result:
			defer func() { _ = second.Release(context.Background()) }()
			if second.Info().Token <= first.Info().Token {
				t.Fatal("token did not advance")
			}
			if got := second.Info().AcquiredAt.Sub(start); got != time.Second {
				t.Fatalf("waiter acquired after %v, want 1s", got)
			}
		default:
			t.Fatal("waiter did not acquire")
		}
	})
}

// FR-COORD-01, FR-COORD-02
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
	if _, err := lease.Renew(context.Background(), 0); !errors.Is(err, coordination.ErrInvalidRequest) {
		t.Fatalf("Renew(0) error = %v, want ErrInvalidRequest", err)
	}
}

// FR-COORD-05
func TestBackendLostLeaseCannotRenewOrRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(nil)
		lease, err := backend.Acquire(context.Background(), coordination.AcquireRequest{Key: "key", Owner: "one", TTL: time.Second})
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Second)
		synctest.Wait()
		if !isClosed(lease.Done()) {
			t.Fatal("lease did not expire")
		}

		if _, err := lease.Renew(context.Background(), time.Second); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("Renew() error = %v, want ErrLeaseLost", err)
		}
		if err := lease.Release(context.Background()); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("Release() error = %v, want ErrLeaseLost", err)
		}
	})
}

// FR-COORD-03
func TestBackendWaitingAcquisitionHonorsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(nil)
		first, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
			Key: "key", Owner: "one", TTL: time.Minute,
		})
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = first.Release(context.Background()) }()

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, acquireErr := backend.Acquire(ctx, coordination.AcquireRequest{
				Key: "key", Owner: "two", TTL: time.Minute, Wait: true,
			})
			result <- acquireErr
		}()
		synctest.Wait()
		if len(result) != 0 {
			t.Fatal("waiting Acquire returned while the key was held")
		}
		cancel()
		synctest.Wait()

		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("waiting Acquire() error = %v, want context.Canceled", err)
			}
		default:
			t.Fatal("waiting Acquire did not return after cancellation")
		}
	})
}

func isClosed(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

// laggingClock reads the real (in tests: synctest virtual) time but delivers
// every timer lag late. It models delayed timer delivery, so tests can check
// that deadlines are enforced by comparing times rather than by timers.
type laggingClock struct {
	lag time.Duration
}

func (laggingClock) Now() time.Time {
	return time.Now()
}

func (c laggingClock) After(delay time.Duration) <-chan time.Time {
	return time.After(delay + c.lag)
}
