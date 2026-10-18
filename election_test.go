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

// FR-COORD-08
func TestCampaignAcquiresWaitingLeaseAndExposesLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
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
		synctest.Wait()
		select {
		case <-leadership.Lost:
		default:
			t.Fatal("leadership loss was not reported")
		}
	})
}

// FR-COORD-08
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
	if _, err := coordination.Campaign(context.Background(), nil, coordination.ElectionOptions{Name: "name", Candidate: "node", TTL: time.Second}); !errors.Is(err, coordination.ErrInvalidRequest) {
		t.Fatalf("Campaign(nil backend) error = %v, want ErrInvalidRequest", err)
	}
	if err := (coordination.Leadership{}).Resign(context.Background()); !errors.Is(err, coordination.ErrNotOwner) {
		t.Fatalf("zero Leadership Resign() error = %v, want ErrNotOwner", err)
	}

	want := errors.New("backend unavailable")
	backend.err = want
	if _, err := coordination.Campaign(context.Background(), backend, coordination.ElectionOptions{Name: "name", Candidate: "node", TTL: time.Second}); !errors.Is(err, want) {
		t.Fatalf("Campaign() error = %v, want %v", err, want)
	}
}

// FR-COORD-08, FR-COORD-06
func TestCampaignKeepsLeadershipUntilContextCancellation(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := memory.NewBackend(nil)
		options := coordination.ElectionOptions{Name: "scheduler", Candidate: "node-a", TTL: 3 * time.Second}

		ctx, cancel := context.WithCancel(context.Background())
		leader, err := coordination.Campaign(ctx, backend, options)
		if err != nil {
			t.Fatal(err)
		}

		type outcome struct {
			leadership coordination.Leadership
			err        error
		}
		rivalCtx, cancelRival := context.WithCancel(context.Background())
		defer cancelRival()
		rival := make(chan outcome, 1)
		go func() {
			options := options
			options.Candidate = "node-b"
			leadership, err := coordination.Campaign(rivalCtx, backend, options)
			rival <- outcome{leadership, err}
		}()

		// Keepalive renews every TTL/3, so leadership outlives many TTLs.
		time.Sleep(time.Hour)
		synctest.Wait()
		if len(rival) != 0 {
			t.Fatal("rival won the election while the leader was alive")
		}
		select {
		case <-leader.Lost:
			t.Fatal("leader lost leadership while its keepalive was running")
		default:
		}

		// Canceling the campaign context stops the keepalive: Lost closes at
		// once and the lease lapses after one TTL.
		cancel()
		synctest.Wait()
		select {
		case <-leader.Lost:
		default:
			t.Fatal("Lost did not close after the campaign context was canceled")
		}
		time.Sleep(options.TTL)
		synctest.Wait()
		var won outcome
		select {
		case won = <-rival:
		default:
			t.Fatal("rival did not win after the leader's lease expired")
		}
		if won.err != nil {
			t.Fatal(won.err)
		}
		defer func() { _ = won.leadership.Resign(context.Background()) }()
		if won.leadership.Token <= leader.Token {
			t.Fatalf("tokens = %d then %d, want increasing", leader.Token, won.leadership.Token)
		}
		if err := leader.Resign(context.Background()); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("stale Resign() error = %v, want ErrLeaseLost", err)
		}
	})
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
