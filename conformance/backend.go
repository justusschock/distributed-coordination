package conformance

import (
	"context"
	"errors"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
)

type Factory func(t *testing.T) (coordination.Backend, func(time.Duration))

func Run(t *testing.T, factory Factory) {
	t.Helper()

	t.Run("non-waiting contention", func(t *testing.T) {
		backend, _ := factory(t)
		first := acquire(t, backend, "conformance/contention", "one", time.Minute, false)
		t.Cleanup(func() { _ = first.Release(context.Background()) })
		_, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
			Key:   "conformance/contention",
			Owner: "two",
			TTL:   time.Minute,
		})
		if !errors.Is(err, coordination.ErrNotAcquired) {
			t.Fatalf("Acquire() error = %v, want ErrNotAcquired", err)
		}
	})

	t.Run("waiting cancellation", func(t *testing.T) {
		backend, _ := factory(t)
		first := acquire(t, backend, "conformance/cancellation", "one", time.Minute, false)
		t.Cleanup(func() { _ = first.Release(context.Background()) })
		ctx, cancel := context.WithCancel(context.Background())
		started := make(chan struct{})
		result := make(chan error, 1)
		go func() {
			close(started)
			_, err := backend.Acquire(ctx, coordination.AcquireRequest{
				Key:   "conformance/cancellation",
				Owner: "two",
				TTL:   time.Minute,
				Wait:  true,
			})
			result <- err
		}()
		<-started
		cancel()
		select {
		case err := <-result:
			if !errors.Is(err, context.Canceled) {
				t.Fatalf("waiting Acquire() error = %v, want context.Canceled", err)
			}
		case <-time.After(time.Second):
			t.Fatal("waiting Acquire did not honor cancellation")
		}
	})

	t.Run("renew", func(t *testing.T) {
		backend, advance := factory(t)
		lease := acquire(t, backend, "conformance/renew", "one", time.Second, false)
		advance(500 * time.Millisecond)
		if _, err := lease.Renew(context.Background(), time.Second); err != nil {
			t.Fatal(err)
		}
		advance(500 * time.Millisecond)
		select {
		case <-lease.Done():
			t.Fatal("lease expired at original deadline after renewal")
		default:
		}
		advance(500 * time.Millisecond)
		waitForClosure(t, lease.Done(), "renewed lease")
	})

	t.Run("release", func(t *testing.T) {
		backend, _ := factory(t)
		lease := acquire(t, backend, "conformance/release", "one", time.Minute, false)
		if err := lease.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		waitForClosure(t, lease.Done(), "released lease")
	})

	t.Run("expiry", func(t *testing.T) {
		backend, advance := factory(t)
		lease := acquire(t, backend, "conformance/expiry", "one", time.Second, false)
		advance(time.Second)
		waitForClosure(t, lease.Done(), "expired lease")
	})

	t.Run("stale renewal", func(t *testing.T) {
		backend, _ := factory(t)
		stale := acquire(t, backend, "conformance/stale", "one", time.Minute, false)
		if err := stale.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		current := acquire(t, backend, "conformance/stale", "two", time.Minute, false)
		t.Cleanup(func() { _ = current.Release(context.Background()) })
		if _, err := stale.Renew(context.Background(), time.Minute); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("stale Renew() error = %v, want ErrLeaseLost", err)
		}
	})

	t.Run("strictly increasing fencing tokens", func(t *testing.T) {
		backend, advance := factory(t)
		first := acquire(t, backend, "conformance/fencing", "one", time.Second, false)
		if err := first.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		second := acquire(t, backend, "conformance/fencing", "two", time.Second, false)
		advance(time.Second)
		waitForClosure(t, second.Done(), "second lease")
		third := acquire(t, backend, "conformance/fencing", "three", time.Second, false)
		t.Cleanup(func() { _ = third.Release(context.Background()) })
		if first.Info().Token >= second.Info().Token || second.Info().Token >= third.Info().Token {
			t.Fatalf("tokens = %d, %d, %d, want strictly increasing", first.Info().Token, second.Info().Token, third.Info().Token)
		}
	})
}

func acquire(t *testing.T, backend coordination.Backend, key, owner string, ttl time.Duration, wait bool) coordination.Lease {
	t.Helper()
	lease, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
		Key:   key,
		Owner: owner,
		TTL:   ttl,
		Wait:  wait,
	})
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func waitForClosure(t *testing.T, done <-chan struct{}, name string) {
	t.Helper()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatalf("%s did not close Done", name)
	}
}
