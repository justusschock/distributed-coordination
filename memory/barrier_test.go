package memory

import (
	"context"
	"errors"
	"reflect"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
)

func TestBarrierReturnsSortedParticipantsAndIncreasingGenerations(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)

	first := completeBarrier(t, clock, backend, "deploy", []string{"b", "a"})
	if first.Generation == 0 {
		t.Fatal("first generation is zero")
	}
	if !reflect.DeepEqual(first.Participants, []string{"a", "b"}) {
		t.Fatalf("participants = %v, want [a b]", first.Participants)
	}

	second := completeBarrier(t, clock, backend, "deploy", []string{"d", "c"})
	if second.Generation <= first.Generation {
		t.Fatalf("generations = %d then %d, want increasing", first.Generation, second.Generation)
	}
	if !reflect.DeepEqual(second.Participants, []string{"c", "d"}) {
		t.Fatalf("participants = %v, want [c d]", second.Participants)
	}
}

func TestBarrierDuplicateArrivalIsIdempotent(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)
	request := coordination.BarrierRequest{Name: "deploy", Participant: "a", Parties: 2, TTL: time.Minute}

	firstA := waitForBarrier(backend, request)
	clock.waitForTimer(t)
	secondA := waitForBarrier(backend, request)
	clock.waitForTimer(t)
	b := waitForBarrier(backend, coordination.BarrierRequest{Name: "deploy", Participant: "b", Parties: 2, TTL: time.Minute})

	results := []coordination.BarrierResult{
		receiveBarrier(t, firstA),
		receiveBarrier(t, secondA),
		receiveBarrier(t, b),
	}
	for _, result := range results {
		if result.Generation != results[0].Generation || !reflect.DeepEqual(result.Participants, []string{"a", "b"}) {
			t.Fatalf("result = %+v, want generation %d and [a b]", result, results[0].Generation)
		}
	}
}

func TestBarrierRejectsMismatchedPartyCount(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)
	ctx, cancel := context.WithCancel(context.Background())
	first := waitForBarrierContext(backend, ctx, coordination.BarrierRequest{Name: "deploy", Participant: "a", Parties: 2, TTL: time.Minute})
	clock.waitForTimer(t)

	_, err := backend.Wait(context.Background(), coordination.BarrierRequest{Name: "deploy", Participant: "b", Parties: 3, TTL: time.Minute})
	if !errors.Is(err, coordination.ErrInvalidRequest) {
		t.Fatalf("Wait() error = %v, want ErrInvalidRequest", err)
	}
	cancel()
	if err := receiveBarrierError(t, first); !errors.Is(err, context.Canceled) {
		t.Fatalf("first Wait() error = %v, want context.Canceled", err)
	}
}

func TestBarrierCancellationRemovesUncompletedArrival(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)
	ctx, cancel := context.WithCancel(context.Background())
	canceled := waitForBarrierContext(backend, ctx, coordination.BarrierRequest{Name: "deploy", Participant: "a", Parties: 2, TTL: time.Minute})
	clock.waitForTimer(t)
	cancel()
	if err := receiveBarrierError(t, canceled); !errors.Is(err, context.Canceled) {
		t.Fatalf("canceled Wait() error = %v, want context.Canceled", err)
	}

	b := waitForBarrier(backend, coordination.BarrierRequest{Name: "deploy", Participant: "b", Parties: 2, TTL: time.Minute})
	clock.waitForTimer(t)
	select {
	case result := <-b:
		t.Fatalf("barrier completed from canceled arrival: %+v", result)
	default:
	}
	c := waitForBarrier(backend, coordination.BarrierRequest{Name: "deploy", Participant: "c", Parties: 2, TTL: time.Minute})

	for _, result := range []coordination.BarrierResult{receiveBarrier(t, b), receiveBarrier(t, c)} {
		if !reflect.DeepEqual(result.Participants, []string{"b", "c"}) {
			t.Fatalf("participants = %v, want [b c]", result.Participants)
		}
	}
}

func TestBarrierTTLRemovesUncompletedArrival(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)
	result := waitForBarrier(backend, coordination.BarrierRequest{Name: "deploy", Participant: "a", Parties: 2, TTL: time.Second})
	clock.waitForTimer(t)
	clock.Advance(time.Second)
	if err := receiveBarrierError(t, result); !errors.Is(err, coordination.ErrNotAcquired) {
		t.Fatalf("expired Wait() error = %v, want ErrNotAcquired", err)
	}
}

func TestBarrierPrunesExpiredArrivalBeforeDelayedTimerDelivery(t *testing.T) {
	clock := newFakeClock(time.Unix(100, 0))
	backend := NewBackend(clock)
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	expired := waitForBarrierContext(backend, ctx, coordination.BarrierRequest{
		Name: "deploy", Participant: "expired", Parties: 2, TTL: time.Second,
	})
	clock.waitForTimer(t)
	clock.elapseWithoutFiring(time.Second)

	current := waitForBarrierContext(backend, ctx, coordination.BarrierRequest{
		Name: "deploy", Participant: "current", Parties: 2, TTL: time.Minute,
	})
	if err := receiveBarrierError(t, expired); !errors.Is(err, coordination.ErrNotAcquired) {
		t.Fatalf("expired Wait() error = %v, want ErrNotAcquired", err)
	}
	clock.waitForTimer(t)
	select {
	case result := <-current:
		t.Fatalf("barrier completed with expired arrival: %+v", result)
	default:
	}

	last := waitForBarrierContext(backend, ctx, coordination.BarrierRequest{
		Name: "deploy", Participant: "last", Parties: 2, TTL: time.Minute,
	})
	for _, result := range []coordination.BarrierResult{receiveBarrier(t, current), receiveBarrier(t, last)} {
		if !reflect.DeepEqual(result.Participants, []string{"current", "last"}) {
			t.Fatalf("participants = %v, want [current last]", result.Participants)
		}
	}
}

type barrierOutcome struct {
	result coordination.BarrierResult
	err    error
}

func completeBarrier(t *testing.T, clock *fakeClock, backend *Backend, name string, participants []string) coordination.BarrierResult {
	t.Helper()
	first := waitForBarrier(backend, coordination.BarrierRequest{Name: name, Participant: participants[0], Parties: len(participants), TTL: time.Minute})
	clock.waitForTimer(t)
	second := waitForBarrier(backend, coordination.BarrierRequest{Name: name, Participant: participants[1], Parties: len(participants), TTL: time.Minute})
	firstResult := receiveBarrier(t, first)
	secondResult := receiveBarrier(t, second)
	if firstResult.Generation != secondResult.Generation || !reflect.DeepEqual(firstResult.Participants, secondResult.Participants) {
		t.Fatalf("barrier results differ: %+v and %+v", firstResult, secondResult)
	}
	return firstResult
}

func waitForBarrier(backend *Backend, request coordination.BarrierRequest) <-chan barrierOutcome {
	return waitForBarrierContext(backend, context.Background(), request)
}

func waitForBarrierContext(backend *Backend, ctx context.Context, request coordination.BarrierRequest) <-chan barrierOutcome {
	result := make(chan barrierOutcome, 1)
	go func() {
		barrierResult, err := backend.Wait(ctx, request)
		result <- barrierOutcome{result: barrierResult, err: err}
	}()
	return result
}

func receiveBarrier(t *testing.T, outcome <-chan barrierOutcome) coordination.BarrierResult {
	t.Helper()
	select {
	case received := <-outcome:
		if received.err != nil {
			t.Fatal(received.err)
		}
		return received.result
	case <-time.After(time.Second):
		t.Fatal("barrier did not complete")
		return coordination.BarrierResult{}
	}
}

func receiveBarrierError(t *testing.T, outcome <-chan barrierOutcome) error {
	t.Helper()
	select {
	case received := <-outcome:
		return received.err
	case <-time.After(time.Second):
		t.Fatal("barrier did not return")
		return nil
	}
}
