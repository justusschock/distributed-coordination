package coordination_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
)

func TestKeepAliveRenewsImmediatelyThenAtOneThirdTTL(t *testing.T) {
	lease := newTestLease()
	lease.failAfter = 2
	lease.renewErr = coordination.ErrLeaseLost

	future, err := coordination.KeepAlive(context.Background(), lease, 30*time.Millisecond)
	if err != nil {
		t.Fatal(err)
	}

	select {
	case call := <-lease.renewed:
		if call.ttl != 30*time.Millisecond {
			t.Fatalf("first renewal TTL = %v, want 30ms", call.ttl)
		}
	case <-time.After(time.Second):
		t.Fatal("keepalive did not renew immediately")
	}

	select {
	case <-lease.renewed:
	case <-time.After(time.Second):
		t.Fatal("keepalive did not renew after ttl/3")
	}

	info, err := future.Await(context.Background())
	if !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("Await() error = %v, want ErrLeaseLost", err)
	}
	if info.Token != 1 {
		t.Fatalf("Await() token = %d, want 1", info.Token)
	}
}

func TestKeepAliveValidatesArgumentsAndReportsCancellationCause(t *testing.T) {
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
	select {
	case <-lease.renewed:
	case <-time.After(time.Second):
		t.Fatal("keepalive did not perform initial renewal")
	}
	cancel(cause)

	if _, err := future.Await(context.Background()); !errors.Is(err, cause) {
		t.Fatalf("Await() error = %v, want %v", err, cause)
	}
}

type renewCall struct {
	ttl time.Duration
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
	l.renewed <- renewCall{ttl: ttl}
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
