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

// FR-COORD-10
func TestBarrierReturnsSortedParticipantsAndIncreasingGenerations(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(nil)

		first := completeBarrier(t, backend, "deploy", []string{"b", "a"})
		if first.Generation == 0 {
			t.Fatal("first generation is zero")
		}
		if !reflect.DeepEqual(first.Participants, []string{"a", "b"}) {
			t.Fatalf("participants = %v, want [a b]", first.Participants)
		}

		second := completeBarrier(t, backend, "deploy", []string{"d", "c"})
		if second.Generation <= first.Generation {
			t.Fatalf("generations = %d then %d, want increasing", first.Generation, second.Generation)
		}
		if !reflect.DeepEqual(second.Participants, []string{"c", "d"}) {
			t.Fatalf("participants = %v, want [c d]", second.Participants)
		}
	})
}

// FR-COORD-10
func TestBarrierRejectsInvalidRequests(t *testing.T) {
	backend := NewBackend(nil)
	for _, request := range []coordination.BarrierRequest{
		{Participant: "a", Parties: 2, TTL: time.Minute},
		{Name: "deploy", Parties: 2, TTL: time.Minute},
		{Name: "deploy", Participant: "a", TTL: time.Minute},
		{Name: "deploy", Participant: "a", Parties: 2},
	} {
		if _, err := backend.Wait(context.Background(), request); !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Fatalf("Wait(%+v) error = %v, want ErrInvalidRequest", request, err)
		}
	}
}

// FR-COORD-10
func TestBarrierDuplicateArrivalIsIdempotent(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(nil)
		request := coordination.BarrierRequest{Name: "deploy", Participant: "a", Parties: 2, TTL: time.Minute}

		firstA := waitForBarrier(backend, request)
		synctest.Wait()
		secondA := waitForBarrier(backend, request)
		synctest.Wait()
		if len(firstA) != 0 || len(secondA) != 0 {
			t.Fatal("barrier completed with one distinct participant")
		}
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
	})
}

// FR-COORD-10
func TestBarrierRejectsMismatchedPartyCount(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(nil)
		ctx, cancel := context.WithCancel(context.Background())
		first := waitForBarrierContext(ctx, backend, coordination.BarrierRequest{Name: "deploy", Participant: "a", Parties: 2, TTL: time.Minute})
		synctest.Wait()

		_, err := backend.Wait(context.Background(), coordination.BarrierRequest{Name: "deploy", Participant: "b", Parties: 3, TTL: time.Minute})
		if !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Fatalf("Wait() error = %v, want ErrInvalidRequest", err)
		}
		cancel()
		if err := receiveBarrierError(t, first); !errors.Is(err, context.Canceled) {
			t.Fatalf("first Wait() error = %v, want context.Canceled", err)
		}
	})
}

// FR-COORD-10
func TestBarrierCancellationRemovesUncompletedArrival(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(nil)
		ctx, cancel := context.WithCancel(context.Background())
		canceled := waitForBarrierContext(ctx, backend, coordination.BarrierRequest{Name: "deploy", Participant: "a", Parties: 2, TTL: time.Minute})
		synctest.Wait()
		cancel()
		if err := receiveBarrierError(t, canceled); !errors.Is(err, context.Canceled) {
			t.Fatalf("canceled Wait() error = %v, want context.Canceled", err)
		}

		b := waitForBarrier(backend, coordination.BarrierRequest{Name: "deploy", Participant: "b", Parties: 2, TTL: time.Minute})
		synctest.Wait()
		if len(b) != 0 {
			t.Fatalf("barrier completed from canceled arrival: %+v", <-b)
		}
		c := waitForBarrier(backend, coordination.BarrierRequest{Name: "deploy", Participant: "c", Parties: 2, TTL: time.Minute})

		for _, result := range []coordination.BarrierResult{receiveBarrier(t, b), receiveBarrier(t, c)} {
			if !reflect.DeepEqual(result.Participants, []string{"b", "c"}) {
				t.Fatalf("participants = %v, want [b c]", result.Participants)
			}
		}
	})
}

// FR-COORD-10
func TestBarrierTTLRemovesUncompletedArrival(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(nil)
		result := waitForBarrier(backend, coordination.BarrierRequest{Name: "deploy", Participant: "a", Parties: 2, TTL: time.Second})
		time.Sleep(time.Second - time.Nanosecond)
		synctest.Wait()
		if len(result) != 0 {
			t.Fatal("arrival returned before its TTL elapsed")
		}
		time.Sleep(time.Nanosecond)
		if err := receiveBarrierError(t, result); !errors.Is(err, coordination.ErrNotAcquired) {
			t.Fatalf("expired Wait() error = %v, want ErrNotAcquired", err)
		}
	})
}

// FR-COORD-10
func TestBarrierPrunesExpiredArrivalBeforeDelayedTimerDelivery(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := NewBackend(laggingClock{lag: time.Hour})
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		expired := waitForBarrierContext(ctx, backend, coordination.BarrierRequest{
			Name: "deploy", Participant: "expired", Parties: 2, TTL: time.Second,
		})
		// The arrival's deadline passes but its timer has not fired yet.
		time.Sleep(time.Second)
		synctest.Wait()
		if len(expired) != 0 {
			t.Fatal("expiry timer fired despite the lagging clock")
		}

		current := waitForBarrierContext(ctx, backend, coordination.BarrierRequest{
			Name: "deploy", Participant: "current", Parties: 2, TTL: time.Minute,
		})
		synctest.Wait()
		if len(current) != 0 {
			t.Fatalf("barrier completed with expired arrival: %+v", <-current)
		}
		if err := receiveBarrierError(t, expired); !errors.Is(err, coordination.ErrNotAcquired) {
			t.Fatalf("pruned Wait() error = %v, want ErrNotAcquired", err)
		}

		last := waitForBarrierContext(ctx, backend, coordination.BarrierRequest{
			Name: "deploy", Participant: "last", Parties: 2, TTL: time.Minute,
		})
		for _, result := range []coordination.BarrierResult{receiveBarrier(t, current), receiveBarrier(t, last)} {
			if !reflect.DeepEqual(result.Participants, []string{"current", "last"}) {
				t.Fatalf("participants = %v, want [current last]", result.Participants)
			}
		}
	})
}

// FR-COORD-10
func TestBarrierExpiredArrivalWinsWhenCompletionIsAlsoReady(t *testing.T) {
	backend := NewBackend(nil)
	generation := newBarrierGeneration(1)
	arrival := &barrierArrival{
		participant: "expired",
		expired:     make(chan struct{}),
	}
	close(arrival.expired)
	generation.completed = true
	generation.result = coordination.BarrierResult{
		Name: "deploy", Generation: 1, Participants: []string{"current", "last"},
	}
	close(generation.done)

	for range 1_000 {
		result, err := backend.awaitBarrier(context.Background(), generation, arrival, make(<-chan time.Time))
		if !errors.Is(err, coordination.ErrNotAcquired) {
			t.Fatalf("awaitBarrier() = (%+v, %v), want ErrNotAcquired", result, err)
		}
	}
}

type barrierOutcome struct {
	result coordination.BarrierResult
	err    error
}

// completeBarrier runs a two-party generation with a deterministic arrival
// order. It must be called inside a synctest bubble.
func completeBarrier(t *testing.T, backend *Backend, name string, participants []string) coordination.BarrierResult {
	t.Helper()
	first := waitForBarrier(backend, coordination.BarrierRequest{Name: name, Participant: participants[0], Parties: len(participants), TTL: time.Minute})
	synctest.Wait()
	second := waitForBarrier(backend, coordination.BarrierRequest{Name: name, Participant: participants[1], Parties: len(participants), TTL: time.Minute})
	firstResult := receiveBarrier(t, first)
	secondResult := receiveBarrier(t, second)
	if firstResult.Generation != secondResult.Generation || !reflect.DeepEqual(firstResult.Participants, secondResult.Participants) {
		t.Fatalf("barrier results differ: %+v and %+v", firstResult, secondResult)
	}
	return firstResult
}

func waitForBarrier(backend *Backend, request coordination.BarrierRequest) <-chan barrierOutcome {
	return waitForBarrierContext(context.Background(), backend, request)
}

func waitForBarrierContext(ctx context.Context, backend *Backend, request coordination.BarrierRequest) <-chan barrierOutcome {
	result := make(chan barrierOutcome, 1)
	go func() {
		barrierResult, err := backend.Wait(ctx, request)
		result <- barrierOutcome{result: barrierResult, err: err}
	}()
	return result
}

// receiveBarrier returns the outcome once every goroutine in the bubble is
// blocked, failing if the waiter has not returned by then.
func receiveBarrier(t *testing.T, outcome <-chan barrierOutcome) coordination.BarrierResult {
	t.Helper()
	synctest.Wait()
	select {
	case received := <-outcome:
		if received.err != nil {
			t.Fatal(received.err)
		}
		return received.result
	default:
		t.Fatal("barrier did not complete")
		return coordination.BarrierResult{}
	}
}

func receiveBarrierError(t *testing.T, outcome <-chan barrierOutcome) error {
	t.Helper()
	synctest.Wait()
	select {
	case received := <-outcome:
		return received.err
	default:
		t.Fatal("barrier did not return")
		return nil
	}
}
