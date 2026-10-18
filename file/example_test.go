package file_test

import (
	"context"
	"fmt"
	"os"
	"path/filepath"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
	"github.com/justusschock/distributed-coordination/file"
)

func ExampleNewBackend() {
	dir, err := os.MkdirTemp("", "locks")
	if err != nil {
		fmt.Println(err)
		return
	}
	defer func() { _ = os.RemoveAll(dir) }()

	backend, err := file.NewBackend(filepath.Join(dir, "leases"), file.WithPollInterval(50*time.Millisecond))
	if err != nil {
		fmt.Println(err)
		return
	}

	// Every process that opens a backend on the same directory competes for
	// the same keys; fencing tokens persist in the directory.
	for range 2 {
		lease, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
			Key:   "repo",
			Owner: fmt.Sprintf("pid-%d", os.Getpid()),
			TTL:   time.Minute,
			Wait:  true,
		})
		if err != nil {
			fmt.Println(err)
			return
		}
		fmt.Println("token", lease.Info().Token)
		if err := lease.Release(context.Background()); err != nil {
			fmt.Println(err)
		}
	}
	// Output:
	// token 1
	// token 2
}
