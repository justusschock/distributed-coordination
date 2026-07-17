package coordination_test

import (
	"context"
	"errors"
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
