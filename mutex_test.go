package coordination_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	"github.com/justusschock/go-distributed-coordination/memory"
)

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

func TestMutexSerializesOwnersAndAdvancesToken(t *testing.T) {
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

	select {
	case token := <-result:
		t.Fatalf("second Lock returned token %d before first Unlock", token)
	default:
	}
	if err := first.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}

	select {
	case err := <-errs:
		t.Fatal(err)
	case secondToken := <-result:
		if secondToken <= firstToken {
			t.Fatalf("tokens = %d then %d, want increasing", firstToken, secondToken)
		}
	case <-time.After(time.Second):
		t.Fatal("second Lock did not proceed after Unlock")
	}
	if err := second.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
}

func TestMutexWaitingLockHonorsCancellation(t *testing.T) {
	backend := memory.NewBackend(nil)
	first, _ := coordination.NewMutex(backend, "critical", "one", time.Second)
	second, _ := coordination.NewMutex(backend, "critical", "two", time.Second)
	if _, err := first.Lock(context.Background()); err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Unlock(context.Background()) })

	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	if _, err := second.Lock(ctx); !errors.Is(err, context.Canceled) {
		t.Fatalf("Lock() error = %v, want context.Canceled", err)
	}
}

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

func TestMutexRejectsConcurrentLockWhileAcquisitionIsInFlight(t *testing.T) {
	started := make(chan struct{})
	release := make(chan struct{})
	var releaseOnce sync.Once
	releaseAcquire := func() { releaseOnce.Do(func() { close(release) }) }
	defer releaseAcquire()
	backend := &blockingAcquireBackend{
		lease:   newTestLease(),
		started: started,
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
	select {
	case <-started:
	case <-time.After(time.Second):
		t.Fatal("first Lock did not enter backend acquisition")
	}

	secondCtx, cancelSecond := context.WithTimeout(context.Background(), 50*time.Millisecond)
	defer cancelSecond()
	second := make(chan lockResult, 1)
	go func() {
		token, lockErr := mutex.Lock(secondCtx)
		second <- lockResult{token: token, err: lockErr}
	}()
	select {
	case result := <-second:
		if result.token != 0 || !errors.Is(result.err, coordination.ErrNotOwner) {
			t.Fatalf("concurrent Lock() = (%d, %v), want (0, ErrNotOwner)", result.token, result.err)
		}
	case <-secondCtx.Done():
		releaseAcquire()
		<-first
		result := <-second
		t.Fatalf("concurrent Lock waited behind backend acquisition and returned (%d, %v)", result.token, result.err)
	}

	releaseAcquire()
	result := <-first
	if result.err != nil || result.token == 0 {
		t.Fatalf("first Lock() = (%d, %v)", result.token, result.err)
	}
	if err := mutex.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}
}

type blockingAcquireBackend struct {
	lease       coordination.Lease
	started     chan struct{}
	startedOnce sync.Once
	release     <-chan struct{}
}

func (b *blockingAcquireBackend) Acquire(ctx context.Context, _ coordination.AcquireRequest) (coordination.Lease, error) {
	b.startedOnce.Do(func() { close(b.started) })
	select {
	case <-b.release:
		return b.lease, nil
	case <-ctx.Done():
		return nil, context.Cause(ctx)
	}
}
