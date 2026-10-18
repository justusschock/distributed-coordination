package coordination_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"testing/synctest"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
	"github.com/justusschock/distributed-coordination/memory"
)

// FR-COORD-07
func TestMutexReturnsFencingToken(t *testing.T) {
	backend := memory.NewBackend(nil)
	mutex, err := coordination.NewMutex(backend, "critical", "owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	token, err := mutex.Lock(context.Background())
	if err != nil || token == 0 {
		t.Fatalf("Lock() = (%d, %v)", token, err)
	}
	if err := mutex.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// FR-COORD-07
func TestNewMutexValidatesArguments(t *testing.T) {
	backend := memory.NewBackend(nil)
	for name, build := range map[string]func() (*coordination.Mutex, error){
		"nil backend": func() (*coordination.Mutex, error) { return coordination.NewMutex(nil, "key", "owner", time.Second) },
		"empty key":   func() (*coordination.Mutex, error) { return coordination.NewMutex(backend, "", "owner", time.Second) },
		"empty owner": func() (*coordination.Mutex, error) { return coordination.NewMutex(backend, "key", "", time.Second) },
		"zero TTL":    func() (*coordination.Mutex, error) { return coordination.NewMutex(backend, "key", "owner", 0) },
	} {
		if _, err := build(); !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Errorf("%s: NewMutex() error = %v, want ErrInvalidRequest", name, err)
		}
	}
}

// FR-COORD-07, FR-COORD-04
func TestMutexSerializesOwnersAndAdvancesToken(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := memory.NewBackend(nil)
		first, err := coordination.NewMutex(backend, "critical", "one", time.Second)
		if err != nil {
			t.Fatal(err)
		}
		second, err := coordination.NewMutex(backend, "critical", "two", time.Second)
		if err != nil {
			t.Fatal(err)
		}

		firstToken, err := first.Lock(context.Background())
		if err != nil {
			t.Fatal(err)
		}

		result := make(chan coordination.FencingToken, 1)
		errs := make(chan error, 1)
		go func() {
			token, lockErr := second.Lock(context.Background())
			if lockErr != nil {
				errs <- lockErr
				return
			}
			result <- token
		}()

		synctest.Wait()
		if len(result) != 0 || len(errs) != 0 {
			t.Fatal("second Lock returned before first Unlock")
		}
		if err := first.Unlock(context.Background()); err != nil {
			t.Fatal(err)
		}

		synctest.Wait()
		select {
		case err := <-errs:
			t.Fatal(err)
		case secondToken := <-result:
			if secondToken <= firstToken {
				t.Fatalf("tokens = %d then %d, want increasing", firstToken, secondToken)
			}
		default:
			t.Fatal("second Lock did not proceed after Unlock")
		}
		if err := second.Unlock(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

// FR-COORD-07, FR-COORD-03
func TestMutexWaitingLockHonorsCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := memory.NewBackend(nil)
		first, _ := coordination.NewMutex(backend, "critical", "one", time.Minute)
		second, _ := coordination.NewMutex(backend, "critical", "two", time.Minute)
		if _, err := first.Lock(context.Background()); err != nil {
			t.Fatal(err)
		}
		defer func() { _ = first.Unlock(context.Background()) }()

		ctx, cancel := context.WithCancel(context.Background())
		result := make(chan error, 1)
		go func() {
			_, err := second.Lock(ctx)
			result <- err
		}()
		synctest.Wait()
		if len(result) != 0 {
			t.Fatal("waiting Lock returned while the mutex was held")
		}
		cancel()
		if err := <-result; !errors.Is(err, context.Canceled) {
			t.Fatalf("Lock() error = %v, want context.Canceled", err)
		}
		if err := second.Unlock(context.Background()); !errors.Is(err, coordination.ErrNotOwner) {
			t.Fatalf("Unlock after canceled Lock error = %v, want ErrNotOwner", err)
		}
	})
}

// FR-COORD-07
func TestMutexRejectsDuplicateOwnershipOperations(t *testing.T) {
	backend := memory.NewBackend(nil)
	mutex, err := coordination.NewMutex(backend, "critical", "owner", time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if err := mutex.Unlock(context.Background()); !errors.Is(err, coordination.ErrNotOwner) {
		t.Fatalf("Unlock before Lock error = %v, want ErrNotOwner", err)
	}
	if _, err := mutex.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	if _, err := mutex.Lock(context.Background()); !errors.Is(err, coordination.ErrNotOwner) {
		t.Fatalf("duplicate Lock error = %v, want ErrNotOwner", err)
	}
	if err := mutex.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// FR-COORD-07
func TestMutexRejectsConcurrentLockWhileAcquisitionIsInFlight(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		release := make(chan struct{})
		var releaseOnce sync.Once
		releaseAcquire := func() { releaseOnce.Do(func() { close(release) }) }
		defer releaseAcquire()
		backend := &blockingAcquireBackend{
			lease:   newTestLease(),
			release: release,
		}
		mutex, err := coordination.NewMutex(backend, "critical", "owner", time.Minute)
		if err != nil {
			t.Fatal(err)
		}

		type lockResult struct {
			token coordination.FencingToken
			err   error
		}
		first := make(chan lockResult, 1)
		go func() {
			token, lockErr := mutex.Lock(context.Background())
			first <- lockResult{token: token, err: lockErr}
		}()
		synctest.Wait()
		if !backend.started() {
			t.Fatal("first Lock did not enter backend acquisition")
		}

		// A concurrent Lock must fail fast instead of queueing behind the
		// in-flight acquisition; the deadline only bounds a regression.
		secondCtx, cancelSecond := context.WithTimeout(context.Background(), 50*time.Millisecond)
		defer cancelSecond()
		start := time.Now()
		token, err := mutex.Lock(secondCtx)
		if token != 0 || !errors.Is(err, coordination.ErrNotOwner) {
			t.Fatalf("concurrent Lock() = (%d, %v), want (0, ErrNotOwner)", token, err)
		}
		if waited := time.Since(start); waited != 0 {
			t.Fatalf("concurrent Lock waited %v, want immediate return", waited)
		}

		releaseAcquire()
		result := <-first
		if result.err != nil || result.token == 0 {
			t.Fatalf("first Lock() = (%d, %v)", result.token, result.err)
		}
		if err := mutex.Unlock(context.Background()); err != nil {
			t.Fatal(err)
		}
	})
}

type blockingAcquireBackend struct {
	lease   coordination.Lease
	release <-chan struct{}

	mu    sync.Mutex
	calls int
}

func (b *blockingAcquireBackend) started() bool {
	b.mu.Lock()
	defer b.mu.Unlock()
	return b.calls > 0
}

func (b *blockingAcquireBackend) Acquire(ctx context.Context, _ coordination.AcquireRequest) (coordination.Lease, error) {
	b.mu.Lock()
	b.calls++
	b.mu.Unlock()
	select {
	case <-b.release:
		return b.lease, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}
