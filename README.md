# go-distributed-coordination

Backend-neutral distributed-coordination contracts and a deterministic in-memory reference backend for Go.

The module provides fenced leases, cancellation-safe acquisition, mutexes, leader election, expiring membership, reusable barriers, and a conformance suite for backend adapters. It is independent of any product resource model.

## Requirements

- Go 1.26 or newer
- Read access to the private `github.com/justusschock` modules

## Installation

Configure the private module namespace once:

```bash
go env -w 'GOPRIVATE=github.com/justusschock/*'
go env -w 'GONOSUMDB=github.com/justusschock/*'
```

Ensure Git can authenticate to GitHub, then add the release:

```bash
go get github.com/justusschock/go-distributed-coordination@v0.1.0
```

The only non-standard-library dependency is [`github.com/justusschock/go-concurrency`](https://github.com/justusschock/go-concurrency), used by asynchronous keepalive handling.

## Fenced leases

`Backend.Acquire` grants exclusive ownership of a key for a bounded lifetime. A successful acquisition carries a per-key fencing token that is strictly greater than every token previously granted for that key, even after release or expiry.

```go
package worker

import (
	"context"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	"github.com/justusschock/go-distributed-coordination/memory"
)

type FencedStore interface {
	Write(context.Context, coordination.FencingToken, []byte) error
}

func update(ctx context.Context, store FencedStore) error {
	backend := memory.NewBackend(nil)
	lease, err := backend.Acquire(ctx, coordination.AcquireRequest{
		Key:   "reports/daily",
		Owner: "worker-a",
		TTL:   30 * time.Second,
		Wait:  true,
	})
	if err != nil {
		return err
	}
	defer lease.Release(context.Background())

	return store.Write(ctx, lease.Info().Token, []byte("ready"))
}
```

The token must be checked by the protected database, object store, or service. A lock alone cannot reject a late write from a paused former owner. Tokens are ordered only within one coordination key; do not compare tokens from different keys.

Acquisition behavior:

- Empty keys or owners and non-positive TTLs return `ErrInvalidRequest`.
- A contended request with `Wait: false` returns `ErrNotAcquired` immediately.
- A contended request with `Wait: true` waits for release or expiry and honors context cancellation.
- `Lease.Done` closes exactly once on release or ownership loss.
- `Renew` extends the deadline from the backend clock's current time.
- `Renew` and `Release` return `ErrLeaseLost` after release, expiry, or replacement.

Use `KeepAlive` when work can outlive one TTL. It renews immediately and then every `ttl/3`, returning a `go-concurrency` future that resolves on cancellation or lease loss:

```go
future, err := coordination.KeepAlive(ctx, lease, 30*time.Second)
if err != nil {
	return err
}

// Perform protected work and continue checking lease.Done as appropriate.
_, keepaliveErr := future.Await(context.Background())
```

## Fenced mutex

`Mutex` is a convenience wrapper over a waiting lease acquisition. `Lock` always returns the fencing token so callers can pass it to protected storage.

```go
mutex, err := coordination.NewMutex(backend, "billing/close", "worker-a", 30*time.Second)
if err != nil {
	return err
}

token, err := mutex.Lock(ctx)
if err != nil {
	return err
}
defer mutex.Unlock(context.Background())

return store.Write(ctx, token, []byte("closed"))
```

One `Mutex` value represents one owner and may hold at most one lease. Duplicate `Lock` calls, including a call made while that value is still acquiring, return `ErrNotOwner` promptly; `Unlock` without an active lease does the same. The helper does not renew its lease automatically; choose a TTL that bounds the operation or use a raw lease with `KeepAlive` when explicit lifetime observation is required.

## Leader election

`Campaign` waits for the named election, starts lease keepalive, and exposes both the fencing token and leadership-loss signal.

```go
leadership, err := coordination.Campaign(ctx, backend, coordination.ElectionOptions{
	Name:      "scheduler",
	Candidate: "node-a",
	TTL:       15 * time.Second,
})
if err != nil {
	return err
}
defer leadership.Resign(context.Background())

select {
case <-leadership.Lost:
	return coordination.ErrLeaseLost
case <-ctx.Done():
	return context.Cause(ctx)
}
```

The election key is `election/<name>`. `Resign` releases at most once and every call returns that first release result. `Lost` closes when the lease is lost or its keepalive future completes.

## Membership

The memory backend also implements `Registry`. Membership records have independent TTLs and defensively copied metadata.

```go
membership, err := backend.Join(ctx, coordination.Member{
	Group:    "workers",
	ID:       "node-a",
	Metadata: map[string]string{"zone": "eu-central"},
}, 30*time.Second)
if err != nil {
	return err
}
defer membership.Leave(context.Background())

members, err := backend.List(ctx, "workers")
```

`List` returns members sorted by ID. `MemberInfo`, list results, and input metadata do not share mutable maps with backend state. A duplicate active group/ID returns `ErrNotAcquired`; after leave or expiry, the same ID may join again as a fresh membership. Expiry closes `Membership.Done`, and stale renewal or leave returns `ErrLeaseLost`.

## Reusable barriers

The memory backend implements `Barrier` with monotonically increasing generations and sorted participant lists.

```go
result, err := backend.Wait(ctx, coordination.BarrierRequest{
	Name:        "deployment-ready",
	Participant: "node-a",
	Parties:     3,
	TTL:         time.Minute,
})
if err != nil {
	return err
}
fmt.Printf("generation %d: %v\n", result.Generation, result.Participants)
```

Within a backend, the first wait fixes the party count for a barrier name; a different count returns `ErrInvalidRequest`. A participant is counted once per generation even if it has concurrent waiters. Exactly `Parties` unique, unexpired participants complete the generation for every active waiter, after which the next generation starts empty. Cancellation removes an arrival from an incomplete generation. TTL expiry does the same and returns `ErrNotAcquired`; absolute deadlines are pruned before completion even when timer delivery is delayed.

## Packages and API

### Root package `coordination`

- Lease and fencing: `FencingToken`, `AcquireRequest`, `LeaseInfo`, `Lease`, `Backend`, `Clock`.
- Renewal: `KeepAlive`.
- Mutual exclusion: `Mutex`, `NewMutex`, `Lock`, `Unlock`.
- Election: `ElectionOptions`, `Leadership`, `Campaign`, `Resign`.
- Membership: `Member`, `MemberInfo`, `Membership`, `Registry`.
- Barriers: `BarrierRequest`, `BarrierResult`, `Barrier`.

### Package `memory`

`memory.NewBackend(clock)` returns the synchronized reference backend. Pass `nil` for the real clock or inject a `coordination.Clock` for deterministic tests. The backend implements `coordination.Backend`, `coordination.Registry`, and `coordination.Barrier`.

### Package `conformance`

Backend authors can apply the reusable lease contract suite to an adapter:

```go
func TestBackend(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (coordination.Backend, func(time.Duration)) {
		backend, clock := newBackendForTest(t)
		return backend, clock.Advance
	})
}
```

`conformance.Run` covers contention, waiting cancellation, renewal, release, expiry, stale renewal, and strictly increasing fencing tokens. Each factory call must return a fresh backend and a deterministic clock-advance hook.

## Errors

The package exposes stable sentinel errors for `errors.Is`:

| Error | Meaning |
| --- | --- |
| `ErrInvalidRequest` | A required field is empty, a count is invalid, or a TTL is non-positive. |
| `ErrNotAcquired` | Immediate lease contention, duplicate active membership, or barrier-arrival TTL expiry. |
| `ErrLeaseLost` | A lease or membership has expired, was released, or is no longer the current record. |
| `ErrNotOwner` | A mutex or leadership operation has no matching active ownership. |
| `ErrClosed` | Reserved for closed coordination implementations. |

Canceled operations return their context error or cancellation cause directly.

## Correctness boundary

The in-memory backend is deterministic, synchronized, and useful for tests and single-process coordination. It is not a distributed store and cannot coordinate separate processes or survive process restarts. Production adapters must provide atomic acquisition, durable monotonically increasing per-key tokens, expiry, and record-identity checks with the same observable semantics.

This initial release intentionally does not include adapters for etcd, Consul, ZooKeeper, SQL, Redis, Kubernetes, or network services. It also does not provide distributed transactions, scheduling, resource selection, work queues, or protected-storage fencing enforcement.

## Development

Run the complete checks with private-module access configured:

```bash
go test -race ./...
go vet ./...
pre-commit run --all-files
```

Run ownership-sensitive tests repeatedly when changing timing or state transitions:

```bash
go test -race -count=20 -run 'Fencing|Mutex|Campaign|Barrier' ./...
```

Local implementation plans under `docs/` are deliberately excluded from Git.

## License

Licensed under the Apache License 2.0. See [LICENSE](LICENSE).
