package memory

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"testing/synctest"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
)

// FR-COORD-09
func TestMembershipCopiesMetadataAndListsMembersByID(t *testing.T) {
	backend := NewBackend(nil)
	metadata := map[string]string{"zone": "one"}

	second, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "b", Metadata: metadata}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Leave(context.Background()) })
	metadata["zone"] = "changed"

	first, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a", Metadata: map[string]string{"zone": "two"}}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = first.Leave(context.Background()) })

	info := second.Info()
	if got := info.Member.Metadata["zone"]; got != "one" {
		t.Fatalf("joined metadata = %q, want one", got)
	}
	info.Member.Metadata["zone"] = "mutated"
	if got := second.Info().Member.Metadata["zone"]; got != "one" {
		t.Fatalf("Info metadata after mutation = %q, want one", got)
	}

	members, err := backend.List(context.Background(), "workers")
	if err != nil {
		t.Fatal(err)
	}
	if got := []string{members[0].Member.ID, members[1].Member.ID}; !reflect.DeepEqual(got, []string{"a", "b"}) {
		t.Fatalf("member IDs = %v, want [a b]", got)
	}
	members[1].Member.Metadata["zone"] = "list mutation"
	membersAgain, err := backend.List(context.Background(), "workers")
	if err != nil {
		t.Fatal(err)
	}
	if got := membersAgain[1].Member.Metadata["zone"]; got != "one" {
		t.Fatalf("List metadata after mutation = %q, want one", got)
	}
}

// FR-COORD-09
func TestMembershipRejectsInvalidAndDuplicateJoins(t *testing.T) {
	backend := NewBackend(nil)
	for _, join := range []struct {
		member coordination.Member
		ttl    time.Duration
	}{
		{coordination.Member{ID: "a"}, time.Minute},
		{coordination.Member{Group: "workers"}, time.Minute},
		{coordination.Member{Group: "workers", ID: "a"}, 0},
	} {
		if _, err := backend.Join(context.Background(), join.member, join.ttl); !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Fatalf("Join(%+v, %v) error = %v, want ErrInvalidRequest", join.member, join.ttl, err)
		}
	}
	if _, err := backend.List(context.Background(), ""); !errors.Is(err, coordination.ErrInvalidRequest) {
		t.Fatalf("List(\"\") error = %v, want ErrInvalidRequest", err)
	}

	membership, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Minute); !errors.Is(err, coordination.ErrNotAcquired) {
		t.Fatalf("duplicate Join() error = %v, want ErrNotAcquired", err)
	}
	if err := membership.Leave(context.Background()); err != nil {
		t.Fatal(err)
	}
	rejoined, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Minute)
	if err != nil {
		t.Fatalf("Join after Leave error = %v", err)
	}
	if err := rejoined.Leave(context.Background()); err != nil {
		t.Fatal(err)
	}
}

// FR-COORD-09
func TestMembershipExpiryClosesDoneAndRejectsStaleOperations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(nil)
		membership, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Second)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(time.Second)
		synctest.Wait()
		if !isClosed(membership.Done()) {
			t.Fatal("membership did not expire")
		}
		if _, err := membership.Renew(context.Background(), time.Second); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("Renew() error = %v, want ErrLeaseLost", err)
		}
		if err := membership.Leave(context.Background()); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("Leave() error = %v, want ErrLeaseLost", err)
		}

		members, err := backend.List(context.Background(), "workers")
		if err != nil {
			t.Fatal(err)
		}
		if len(members) != 0 {
			t.Fatalf("members after expiry = %v, want empty", members)
		}
	})
}

// FR-COORD-09
func TestMembershipRenewExtendsLifetime(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(nil)
		start := time.Now()
		membership, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Second)
		if err != nil {
			t.Fatal(err)
		}

		time.Sleep(500 * time.Millisecond)
		info, err := membership.Renew(context.Background(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		if want := start.Add(1500 * time.Millisecond); !info.ExpiresAt.Equal(want) {
			t.Fatalf("ExpiresAt = %v, want %v", info.ExpiresAt, want)
		}
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		if isClosed(membership.Done()) {
			t.Fatal("membership expired at its original deadline")
		}
		time.Sleep(500 * time.Millisecond)
		synctest.Wait()
		if !isClosed(membership.Done()) {
			t.Fatal("membership did not expire at renewed deadline")
		}
	})
}

// FR-COORD-09
func TestMembershipCanRejoinExpiredRecordBeforeTimerDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(laggingClock{lag: time.Hour})
		first, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Second)
		if err != nil {
			t.Fatal(err)
		}

		// The deadline has passed but the expiry timer has not fired yet.
		time.Sleep(time.Second)
		synctest.Wait()
		if isClosed(first.Done()) {
			t.Fatal("expiry timer fired despite the lagging clock")
		}
		second, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Second)
		if err != nil {
			t.Fatal(err)
		}
		defer func() { _ = second.Leave(context.Background()) }()

		if !isClosed(first.Done()) {
			t.Fatal("replaced membership did not lose ownership")
		}
		members, err := backend.List(context.Background(), "workers")
		if err != nil {
			t.Fatal(err)
		}
		if len(members) != 1 || members[0].Member.ID != "a" {
			t.Fatalf("members after rejoin = %+v, want member a", members)
		}
		if _, err := second.Renew(context.Background(), time.Second); err != nil {
			t.Fatalf("rejoined membership Renew() error = %v", err)
		}
	})
}
