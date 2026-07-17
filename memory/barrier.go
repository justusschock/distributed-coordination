package memory

import (
	"context"
	"sort"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
)

type barrierState struct {
	parties int
	current *barrierGeneration
}

type barrierGeneration struct {
	number       uint64
	arrivals     map[*barrierArrival]struct{}
	participants map[string]int
	done         chan struct{}
	completed    bool
	result       coordination.BarrierResult
}

type barrierArrival struct {
	participant string
	expiresAt   time.Time
	expired     chan struct{}
	active      bool
}

func (b *Backend) Wait(ctx context.Context, request coordination.BarrierRequest) (coordination.BarrierResult, error) {
	if request.Name == "" || request.Participant == "" || request.Parties <= 0 || request.TTL <= 0 {
		return coordination.BarrierResult{}, coordination.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return coordination.BarrierResult{}, err
	}

	b.mu.Lock()
	state := b.barriers[request.Name]
	if state == nil {
		state = &barrierState{
			parties: request.Parties,
			current: newBarrierGeneration(1),
		}
		b.barriers[request.Name] = state
	} else if state.parties != request.Parties {
		b.mu.Unlock()
		return coordination.BarrierResult{}, coordination.ErrInvalidRequest
	}

	generation := state.current
	now := b.clock.Now()
	b.pruneBarrierArrivalsLocked(generation, now)
	arrival := &barrierArrival{
		participant: request.Participant,
		expiresAt:   now.Add(request.TTL),
		expired:     make(chan struct{}),
		active:      true,
	}
	generation.arrivals[arrival] = struct{}{}
	generation.participants[request.Participant]++
	if len(generation.participants) == state.parties {
		participants := make([]string, 0, len(generation.participants))
		for participant := range generation.participants {
			participants = append(participants, participant)
		}
		sort.Strings(participants)
		generation.result = coordination.BarrierResult{
			Name:         request.Name,
			Generation:   generation.number,
			Participants: participants,
		}
		generation.completed = true
		close(generation.done)
		state.current = newBarrierGeneration(generation.number + 1)
		result := copyBarrierResult(generation.result)
		b.mu.Unlock()
		return result, nil
	}

	expiry := b.clock.After(arrival.expiresAt.Sub(now))
	b.mu.Unlock()
	return b.awaitBarrier(ctx, generation, arrival, expiry)
}

func (b *Backend) awaitBarrier(ctx context.Context, generation *barrierGeneration, arrival *barrierArrival, expiry <-chan time.Time) (coordination.BarrierResult, error) {
	select {
	case <-generation.done:
		return b.completedBarrierResult(generation, arrival)
	case <-arrival.expired:
		return coordination.BarrierResult{}, coordination.ErrNotAcquired
	case <-ctx.Done():
		result, completed, expired := b.withdrawBarrierArrival(generation, arrival)
		if completed {
			return result, nil
		}
		if expired {
			return coordination.BarrierResult{}, coordination.ErrNotAcquired
		}
		return coordination.BarrierResult{}, context.Cause(ctx)
	case <-expiry:
		if result, completed := b.expireBarrierArrival(generation, arrival); completed {
			return result, nil
		}
		return coordination.BarrierResult{}, coordination.ErrNotAcquired
	}
}

func newBarrierGeneration(number uint64) *barrierGeneration {
	return &barrierGeneration{
		number:       number,
		arrivals:     make(map[*barrierArrival]struct{}),
		participants: make(map[string]int),
		done:         make(chan struct{}),
	}
}

func (b *Backend) withdrawBarrierArrival(generation *barrierGeneration, arrival *barrierArrival) (coordination.BarrierResult, bool, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !arrival.active {
		return coordination.BarrierResult{}, false, true
	}
	if generation.completed {
		return copyBarrierResult(generation.result), true, false
	}
	b.removeBarrierArrivalLocked(generation, arrival, false)
	return coordination.BarrierResult{}, false, false
}

func (b *Backend) expireBarrierArrival(generation *barrierGeneration, arrival *barrierArrival) (coordination.BarrierResult, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !arrival.active {
		return coordination.BarrierResult{}, false
	}
	if generation.completed {
		return copyBarrierResult(generation.result), true
	}
	b.removeBarrierArrivalLocked(generation, arrival, true)
	return coordination.BarrierResult{}, false
}

func (b *Backend) completedBarrierResult(generation *barrierGeneration, arrival *barrierArrival) (coordination.BarrierResult, error) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if !arrival.active {
		return coordination.BarrierResult{}, coordination.ErrNotAcquired
	}
	return copyBarrierResult(generation.result), nil
}

func (b *Backend) pruneBarrierArrivalsLocked(generation *barrierGeneration, now time.Time) {
	for arrival := range generation.arrivals {
		if !arrival.expiresAt.After(now) {
			b.removeBarrierArrivalLocked(generation, arrival, true)
		}
	}
}

func (b *Backend) removeBarrierArrivalLocked(generation *barrierGeneration, arrival *barrierArrival, expired bool) {
	if !arrival.active {
		return
	}
	arrival.active = false
	delete(generation.arrivals, arrival)
	if generation.participants[arrival.participant] <= 1 {
		delete(generation.participants, arrival.participant)
	} else {
		generation.participants[arrival.participant]--
	}
	if expired {
		close(arrival.expired)
	}
}

func copyBarrierResult(result coordination.BarrierResult) coordination.BarrierResult {
	result.Participants = append([]string(nil), result.Participants...)
	return result
}
