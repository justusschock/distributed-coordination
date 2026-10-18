package memory_test

import (
	"context"
	"fmt"
	"sync"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	"github.com/justusschock/go-distributed-coordination/memory"
)

func ExampleNewBackend() {
	backend := memory.NewBackend(nil) // nil selects the real clock

	lease, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
		Key: "jobs", Owner: "worker-a", TTL: time.Minute,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println(lease.Info().Key, lease.Info().Owner, lease.Info().Token)
	_ = lease.Release(context.Background())
	// Output: jobs worker-a 1
}

func ExampleBackend_Join() {
	ctx := context.Background()
	backend := memory.NewBackend(nil)

	for _, id := range []string{"node-b", "node-a"} {
		membership, err := backend.Join(ctx, coordination.Member{
			Group:    "workers",
			ID:       id,
			Metadata: map[string]string{"zone": "eu-central"},
		}, 30*time.Second)
		if err != nil {
			fmt.Println(err)
			return
		}
		defer func() { _ = membership.Leave(context.Background()) }()
	}

	members, err := backend.List(ctx, "workers")
	if err != nil {
		fmt.Println(err)
		return
	}
	for _, member := range members {
		fmt.Println(member.Member.ID, member.Member.Metadata["zone"])
	}
	// Output:
	// node-a eu-central
	// node-b eu-central
}

func ExampleBackend_Wait() {
	backend := memory.NewBackend(nil)

	var wg sync.WaitGroup
	results := make([]coordination.BarrierResult, 2)
	for i, participant := range []string{"node-b", "node-a"} {
		wg.Go(func() {
			result, err := backend.Wait(context.Background(), coordination.BarrierRequest{
				Name:        "deployment-ready",
				Participant: participant,
				Parties:     2,
				TTL:         time.Minute,
			})
			if err != nil {
				fmt.Println(err)
			}
			results[i] = result
		})
	}
	wg.Wait()
	fmt.Printf("generation %d: %v\n", results[0].Generation, results[0].Participants)
	// Output: generation 1: [node-a node-b]
}
