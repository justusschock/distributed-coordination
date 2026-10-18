# distributed-coordination

Backend-neutral distributed-coordination contracts for Go: fenced leases,
keepalive, mutexes, leader election, expiring membership and reusable
barriers, with a deterministic in-memory reference backend, a
cross-process local-filesystem backend, and a conformance suite for
backend authors. It is for Go services and tools that run more than one
worker and need to agree who owns what, and for library authors who want
to write coordination logic once and plug in any backend.

## Features

- **Fenced leases.** `Backend.Acquire` grants exclusive, expiring ownership
  of a key with a `FencingToken` that strictly increases per key, even
  after release or expiry.
- **Keepalive.** `KeepAlive` renews a lease immediately and then every
  `ttl/3`, and reports cancellation, loss or renewal errors through a
  `concurrency` future.
- **Mutex and leader election.** `Mutex` and `Campaign` work with any
  `Backend` and hand out fencing tokens.
- **Membership and barriers.** `Registry` tracks expiring group members
  with metadata; `Barrier` is a reusable rendezvous with generations.
- **In-memory backend** (`memory`): synchronized, deterministic, implements
  every contract; ideal for tests and single-process coordination.
- **File backend** (`file`): separate processes on one host exclude each
  other through operating-system locks, with crash release and persistent
  tokens.
- **Conformance suite** (`conformance`): one call verifies that a backend
  adapter behaves like the reference.

## Installation

```sh
go get github.com/justusschock/distributed-coordination@latest
```

Requires Go 1.25 or newer. The only non-standard-library dependencies are
[`github.com/justusschock/concurrency`](https://github.com/justusschock/concurrency)
(the future returned by `KeepAlive`) and
[`golang.org/x/sys`](https://pkg.go.dev/golang.org/x/sys) (operating-system
locks in package `file`).

## Quick start

Acquire a fenced lease and pass its token to the protected resource (from
[`example_test.go`](example_test.go)):

```go
package main

import (
	"context"
	"errors"
	"fmt"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
	"github.com/justusschock/distributed-coordination/memory"
)

func main() {
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
}
```

The token must be checked by the protected database, object store or
service: a lock alone cannot reject a late write from a paused former
owner. Tokens are ordered only within one key.

More examples, all runnable and tested, are on pkg.go.dev:
`ExampleNewMutex`, `ExampleCampaign`, `ExampleKeepAlive`,
`memory.ExampleBackend_Join`, `memory.ExampleBackend_Wait`,
`file.ExampleNewBackend` and `conformance.ExampleRun`.

### Leases

- Empty keys or owners and non-positive TTLs return `ErrInvalidRequest`.
- A contended request with `Wait: false` returns `ErrNotAcquired`
  immediately; with `Wait: true` it waits for release or expiry and honours
  context cancellation.
- `Lease.Done` closes exactly once on release or loss.
- `Renew` extends the deadline from the backend clock's current time.
- `Renew` and `Release` return `ErrLeaseLost` after release, expiry or
  replacement.

### Mutex

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
return store.Write(ctx, token, data)
```

One `Mutex` value represents one owner and holds at most one lease.
Duplicate `Lock` calls, including one made while that value is still
acquiring, return `ErrNotOwner` promptly; so does `Unlock` without a lease.
`Mutex` does not renew its lease: choose a TTL that bounds the critical
section, or use a raw lease with `KeepAlive`.

### Leader election

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

`Campaign` waits for the lease `election/<name>` and keeps it alive until
`Resign`, lease loss or cancellation of `ctx`; `Lost` closes in each case.
Cancellation stops the keepalive without releasing, so the lease lapses
after one TTL. `Resign` releases at most once and every call returns the
first result.

### Membership and barriers

The memory backend implements `Registry` and `Barrier`.
`Join` registers a member with its own TTL and copies metadata; a duplicate
active group/ID returns `ErrNotAcquired`; after leave or expiry the ID may
join again. `List` returns active members sorted by ID.
`Wait` blocks until `Parties` distinct, unexpired participants arrive and
returns the generation number and sorted participants to every active
waiter. The first `Wait` fixes a barrier's party count; a different count
returns `ErrInvalidRequest`. Canceled arrivals are removed; expired ones
are removed and return `ErrNotAcquired`.

### File backend

```go
backend, err := file.NewBackend("/var/lib/myapp/locks")
```

`file.NewBackend(dir, opts...)` creates `dir` if needed. Options:

| Option | Default | Effect |
| --- | --- | --- |
| `WithClock(clock)` | real clock | Clock for lease timestamps and expiry. |
| `WithPollInterval(d)` | `100ms` | Maximum delay between attempts of a waiting acquisition. Attempts start at `d/16` and back off exponentially with jitter. |
| `WithFileMode(mode)` | `0644` | Permission bits for created lock and state files; must include owner read and write. A created directory receives matching execute bits. |

Each key maps to a lock file, locked with `flock` (Linux, macOS, BSDs) or
`LockFileEx` (Windows), and a JSON state file holding the owner, token and
deadline, written atomically under the lock. Guarantees and limits:

- **Mutual exclusion** across processes, `Backend` values and lease handles
  on one host.
- **Crash release**: the operating system drops the lock when the holder
  exits or is killed.
- **Persistent fencing**: tokens increase across processes and restarts; a
  corrupt or foreign state file fails acquisition instead of resetting.
- **Holder-enforced expiry**: a live but suspended holder cannot be
  pre-empted; rely on fencing tokens at the protected resource.
- **Local filesystems only**: lock semantics on NFS or SMB vary.
- Locks are advisory, and one lock and one state file remain per key ever
  used. Other platforms get `errors.ErrUnsupported` from `NewBackend`.

## Packages

| Package | Description |
| --- | --- |
| [`coordination`](https://pkg.go.dev/github.com/justusschock/distributed-coordination) (root) | Contracts (`Backend`, `Lease`, `Registry`, `Membership`, `Barrier`, `Clock`), helpers (`KeepAlive`, `Mutex`, `Campaign`) and errors. |
| [`memory`](https://pkg.go.dev/github.com/justusschock/distributed-coordination/memory) | In-process reference backend implementing every contract. |
| [`file`](https://pkg.go.dev/github.com/justusschock/distributed-coordination/file) | Cross-process backend over lock files in a local directory. |
| [`conformance`](https://pkg.go.dev/github.com/justusschock/distributed-coordination/conformance) | `Run`, the test suite every `Backend` implementation must pass. |

Packages under `internal/`, should any be added, are not part of the API.

### Writing a backend

Implement `coordination.Backend` and run the suite from your tests, giving
each subtest a fresh backend and a hook that advances its clock:

```go
func TestBackend(t *testing.T) {
	conformance.Run(t, func(t *testing.T) (coordination.Backend, func(time.Duration)) {
		clock := newManualClock()
		return newBackend(clock), clock.Advance
	})
}
```

Production adapters must provide atomic acquisition, durable per-key
monotonically increasing tokens, expiry and record-identity checks.

## Design and guarantees

- **Concurrency.** Every exported type is safe for concurrent use. Blocking
  operations take a `context.Context` and return promptly when it is done.
- **Errors.** Sentinels for `errors.Is`: `ErrInvalidRequest` (invalid input),
  `ErrNotAcquired` (immediate contention, duplicate membership, expired
  barrier arrival), `ErrLeaseLost` (expired, released or replaced) and
  `ErrNotOwner` (mutex or leadership operation without ownership).
  Canceled operations return the context error or cancellation cause.
- **Time.** Expiry compares stored deadlines with the backend `Clock`, so a
  late timer never extends ownership. Inject a `Clock` or use
  `testing/synctest` for deterministic tests.
- **No logging.** The library reports only through return values, futures
  and channels.
- **Scope.** The memory backend does not coordinate processes; the file
  backend coordinates processes on one host. Adapters for etcd, SQL, Redis
  and similar systems belong in other modules.
- **Stability.** The module is `v0.x`: minor releases may break the API.
  Every break is listed in [CHANGELOG.md](CHANGELOG.md).

## Documentation

- [docs/](docs/README.md): requirements (PRD) and architecture decision
  records (ADRs).
- API reference on
  [pkg.go.dev](https://pkg.go.dev/github.com/justusschock/distributed-coordination).

## Contributing

Contributions go through pull requests; see [AGENTS.md](AGENTS.md) for the
docs-first workflow, engineering rules and the checks to run before
opening one.

## License

Apache License 2.0; see [LICENSE](LICENSE) and [NOTICE](NOTICE).
