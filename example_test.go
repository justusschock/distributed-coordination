package coordination_test

import (
	"context"
	"errors"
	"fmt"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
	"github.com/justusschock/distributed-coordination/memory"
)

// Acquire a fenced lease and pass its token to the protected resource.
func Example() {
	ctx := context.Background()
	backend := memory.NewBackend(nil)

	lease, err := backend.Acquire(ctx, coordination.AcquireRequest{
		Key:   "reports/daily",
		Owner: "worker-a",
		TTL:   30 * time.Second,
		Wait:  true,
	})
	if err != nil {
		fmt.Println("acquire:", err)
		return
	}
	defer func() { _ = lease.Release(context.Background()) }()

	// A second owner cannot take the key while the lease is held.
	_, err = backend.Acquire(ctx, coordination.AcquireRequest{
		Key:   "reports/daily",
		Owner: "worker-b",
		TTL:   30 * time.Second,
	})
	fmt.Println("contended:", errors.Is(err, coordination.ErrNotAcquired))

	// The protected store must reject writes carrying an older token.
	fmt.Println("token:", lease.Info().Token)
	// Output:
	// contended: true
	// token: 1
}

func ExampleNewMutex() {
	ctx := context.Background()
	backend := memory.NewBackend(nil)

	mutex, err := coordination.NewMutex(backend, "billing/close", "worker-a", 30*time.Second)
	if err != nil {
		fmt.Println(err)
		return
	}
	token, err := mutex.Lock(ctx)
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("locked with token", token)
	if err := mutex.Unlock(ctx); err != nil {
		fmt.Println(err)
	}
	// Output: locked with token 1
}

func ExampleCampaign() {
	ctx := context.Background()
	backend := memory.NewBackend(nil)

	leadership, err := coordination.Campaign(ctx, backend, coordination.ElectionOptions{
		Name:      "scheduler",
		Candidate: "node-a",
		TTL:       15 * time.Second,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	fmt.Println("leader with token", leadership.Token)

	if err := leadership.Resign(ctx); err != nil {
		fmt.Println(err)
	}
	<-leadership.Lost
	fmt.Println("resigned")
	// Output:
	// leader with token 1
	// resigned
}

func ExampleKeepAlive() {
	backend := memory.NewBackend(nil)
	lease, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
		Key: "index/rebuild", Owner: "worker-a", TTL: 30 * time.Second,
	})
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = lease.Release(context.Background()) }()

	ctx, stop := context.WithCancel(context.Background())
	defer stop()
	future, err := coordination.KeepAlive(ctx, lease, 30*time.Second)
	if err != nil {
		fmt.Println(err)
		return
	}

	// ... long-running work protected by lease.Info().Token ...

	stop()
	_, err = future.Await(context.Background())
	fmt.Println("keepalive stopped:", err)
	// Output: keepalive stopped: context canceled
}
