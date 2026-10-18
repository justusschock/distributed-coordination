package coordination

import (
	"context"
	"time"
)

// BarrierRequest describes one participant's arrival at a named barrier.
type BarrierRequest struct {
	// Name identifies the barrier. It must not be empty.
	Name string
	// Participant identifies the arriving party. It must not be empty; a
	// participant counts once per generation even with concurrent waiters.
	Participant string
	// Parties is the number of distinct participants that complete a
	// generation. It must be positive, and every waiter of a barrier must
	// use the same value.
	Parties int
	// TTL bounds how long the arrival counts towards the current generation.
	// It must be positive.
	TTL time.Duration
}

// BarrierResult describes a completed barrier generation.
type BarrierResult struct {
	Name string
	// Generation increases monotonically per barrier name, starting at 1.
	Generation uint64
	// Participants lists the participants of the generation, sorted.
	Participants []string
}

// Barrier is a reusable rendezvous of a fixed number of participants.
// Implementations must be safe for concurrent use.
type Barrier interface {
	// Wait blocks until Parties distinct, unexpired participants have
	// arrived, then returns the completed generation to every active waiter.
	// It returns [ErrInvalidRequest] for invalid fields or a party count
	// that differs from the barrier's, [ErrNotAcquired] when the arrival's
	// TTL expires first, and the context's cancellation cause when ctx is
	// done first. Expired and canceled arrivals are removed from the
	// generation.
	Wait(ctx context.Context, request BarrierRequest) (BarrierResult, error)
}
