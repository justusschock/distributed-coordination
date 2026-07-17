package memory

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
)

func TestMembershipCopiesMetadataAndListsMembersByID(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)
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

func TestMembershipExpiryClosesDoneAndRejectsStaleOperations(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)
	membership, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(time.Second)
	select {
	case <-membership.Done():
	case <-time.After(time.Second):
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
}

func TestMembershipRenewExtendsLifetime(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)
	membership, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	clock.Advance(500 * time.Millisecond)
	info, err := membership.Renew(context.Background(), time.Second)
	if err != nil {
		t.Fatal(err)
	}
	if want := time.Unix(101, 500_000_000); !info.ExpiresAt.Equal(want) {
		t.Fatalf("ExpiresAt = %v, want %v", info.ExpiresAt, want)
	}
	clock.Advance(500 * time.Millisecond)
	select {
	case <-membership.Done():
		t.Fatal("membership expired at its original deadline")
	default:
	}
	clock.Advance(500 * time.Millisecond)
	select {
	case <-membership.Done():
	case <-time.After(time.Second):
		t.Fatal("membership did not expire at renewed deadline")
	}
}

func TestMembershipCanRejoinExpiredRecordBeforeTimerDelivery(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)
	first, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}

	clock.elapseWithoutFiring(time.Second)
	second, err := backend.Join(context.Background(), coordination.Member{Group: "workers", ID: "a"}, time.Second)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = second.Leave(context.Background()) })

	select {
	case <-first.Done():
	case <-time.After(time.Second):
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
}
