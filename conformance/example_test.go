package conformance_test

import (
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	"github.com/justusschock/go-distributed-coordination/conformance"
	"github.com/justusschock/go-distributed-coordination/memory"
)

// Backend authors call Run from a test in their package, returning a fresh
// backend and a hook that advances its clock.
func ExampleRun() {
	_ = func(t *testing.T) {
		conformance.Run(t, func(t *testing.T) (coordination.Backend, func(time.Duration)) {
			clock := newFakeClock(time.Unix(0, 0))
			return memory.NewBackend(clock), clock.Advance
		})
	}
}
