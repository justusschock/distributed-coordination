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

// FR-COORD-04, FR-COORD-07
func TestFencedMutexIntegrationRejectsStaleOwnerWrites(t *testing.T) {
	backend := memory.NewBackend(nil)
	first, err := coordination.NewMutex(backend, "jobs", "worker-a", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	second, err := coordination.NewMutex(backend, "jobs", "worker-b", time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	store := newFencedStore()

	staleToken, err := first.Lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Write(staleToken, "from worker-a"); err != nil {
		t.Fatal(err)
	}
	if err := first.Unlock(context.Background()); err != nil {
		t.Fatal(err)
	}

	currentToken, err := second.Lock(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Unlock(context.Background()) })
	if err := store.Write(currentToken, "from worker-b"); err != nil {
		t.Fatal(err)
	}
	if err := store.Write(staleToken, "late worker-a write"); !errors.Is(err, errStaleFencingToken) {
		t.Fatalf("stale Write() error = %v, want errStaleFencingToken", err)
	}
	if got := store.Value(); got != "from worker-b" {
		t.Fatalf("stored value = %q, want current owner's value", got)
	}
}

var errStaleFencingToken = errors.New("stale fencing token")

type fencedStore struct {
	mu    sync.Mutex
	token coordination.FencingToken
	value string
}

func newFencedStore() *fencedStore {
	return &fencedStore{}
}

func (s *fencedStore) Write(token coordination.FencingToken, value string) error {
	s.mu.Lock()
	defer s.mu.Unlock()
	if token < s.token {
		return errStaleFencingToken
	}
	s.token = token
	s.value = value
	return nil
}

func (s *fencedStore) Value() string {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.value
}
