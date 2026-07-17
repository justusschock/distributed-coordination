package memory

import (
	"context"
	"sort"

	coordination "github.com/justusschock/go-distributed-coordination"
)

type barrierState struct {
	parties int
	current *barrierGeneration
}

type barrierGeneration struct {
	number    uint64
	arrivals  map[string]int
	done      chan struct{}
	completed bool
	result    coordination.BarrierResult
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
	generation.arrivals[request.Participant]++
	if len(generation.arrivals) == state.parties {
		participants := make([]string, 0, len(generation.arrivals))
		for participant := range generation.arrivals {
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

	expiry := b.clock.After(request.TTL)
	b.mu.Unlock()

	select {
	case <-generation.done:
		return copyBarrierResult(generation.result), nil
	case <-ctx.Done():
		if result, completed := b.withdrawBarrierArrival(generation, request.Participant); completed {
			return result, nil
		}
		return coordination.BarrierResult{}, context.Cause(ctx)
	case <-expiry:
		if result, completed := b.withdrawBarrierArrival(generation, request.Participant); completed {
			return result, nil
		}
		return coordination.BarrierResult{}, coordination.ErrNotAcquired
	}
}

func newBarrierGeneration(number uint64) *barrierGeneration {
	return &barrierGeneration{
		number:   number,
		arrivals: make(map[string]int),
		done:     make(chan struct{}),
	}
}

func (b *Backend) withdrawBarrierArrival(generation *barrierGeneration, participant string) (coordination.BarrierResult, bool) {
	b.mu.Lock()
	defer b.mu.Unlock()
	if generation.completed {
		return copyBarrierResult(generation.result), true
	}
	if generation.arrivals[participant] <= 1 {
		delete(generation.arrivals, participant)
	} else {
		generation.arrivals[participant]--
	}
	return coordination.BarrierResult{}, false
}

func copyBarrierResult(result coordination.BarrierResult) coordination.BarrierResult {
	result.Participants = append([]string(nil), result.Participants...)
	return result
}
