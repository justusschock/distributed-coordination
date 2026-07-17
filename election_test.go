package coordination_test

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
)

func TestCampaignAcquiresWaitingLeaseAndExposesLifetime(t *testing.T) {
	lease := newTestLease()
	backend := &recordingBackend{lease: lease}

	leadership, err := coordination.Campaign(context.Background(), backend, coordination.ElectionOptions{
		Name:      "scheduler",
		Candidate: "node-a",
		TTL:       time.Hour,
	})
	if err != nil {
		t.Fatal(err)
	}

	backend.mu.Lock()
	request := backend.request
	backend.mu.Unlock()
	if request.Key != "election/scheduler" || request.Owner != "node-a" || request.TTL != time.Hour || !request.Wait {
		t.Fatalf("Acquire request = %+v", request)
	}
	if leadership.Token != lease.Info().Token {
		t.Fatalf("leadership token = %d, want %d", leadership.Token, lease.Info().Token)
	}

	if err := leadership.Resign(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := leadership.Resign(context.Background()); err != nil {
		t.Fatalf("second Resign() error = %v", err)
	}
	lease.mu.Lock()
	releases := lease.releases
	lease.mu.Unlock()
	if releases != 1 {
		t.Fatalf("lease releases = %d, want 1", releases)
	}
	select {
	case <-leadership.Lost:
	case <-time.After(time.Second):
		t.Fatal("leadership loss was not reported")
	}
}

func TestCampaignValidatesOptionsAndPropagatesAcquisitionError(t *testing.T) {
	backend := &recordingBackend{lease: newTestLease()}
	for _, options := range []coordination.ElectionOptions{
		{Candidate: "node", TTL: time.Second},
		{Name: "name", TTL: time.Second},
		{Name: "name", Candidate: "node"},
	} {
		if _, err := coordination.Campaign(context.Background(), backend, options); !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Fatalf("Campaign(%+v) error = %v, want ErrInvalidRequest", options, err)
		}
	}

	want := errors.New("backend unavailable")
	backend.err = want
	if _, err := coordination.Campaign(context.Background(), backend, coordination.ElectionOptions{Name: "name", Candidate: "node", TTL: time.Second}); !errors.Is(err, want) {
		t.Fatalf("Campaign() error = %v, want %v", err, want)
	}
}

type recordingBackend struct {
	mu      sync.Mutex
	request coordination.AcquireRequest
	lease   coordination.Lease
	err     error
}

func (b *recordingBackend) Acquire(_ context.Context, request coordination.AcquireRequest) (coordination.Lease, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	b.request = request
	return b.lease, b.err
}
