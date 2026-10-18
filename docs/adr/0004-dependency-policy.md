# ADR 0004: Dependency policy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 |

## Context

Recorded retrospectively for the first public release. Coordination code
is imported by many other libraries (job runners, caches, session stores,
storage adapters). Every dependency it declares becomes a dependency of
all of them. The module has exactly two non-standard-library
dependencies: `github.com/justusschock/concurrency` and
`golang.org/x/sys`.

## Decision drivers

- D1 Keep the transitive dependency graph of consumers small.
- D2 Reuse well-tested primitives rather than re-implement them.
- D3 Platform locking needs system calls the standard library does not
  expose portably.

## Considered options

1. **Standard library first; `concurrency` for futures; `x/sys` for
   locks; no backend client libraries**
   - Pro: `concurrency` gives `KeepAlive` a cancellable, awaitable
     result type shared with sibling libraries (D2).
   - Pro: `x/sys/unix.Flock` and `x/sys/windows.LockFileEx` are the
     maintained wrappers for advisory locks (D3).
   - Con: `KeepAlive`'s return type ties the public API to
     `concurrency`'s `Future`.
2. **Standard library only**
   - Pro: zero dependencies (D1).
   - Con: `KeepAlive` would need its own future or channel-pair type; locks
     would need `syscall`, which is frozen and lacks `LockFileEx` helpers.
3. **Include adapters for etcd, Redis, SQL and similar in this module**
   - Pro: batteries included.
   - Con: every consumer would download those clients (D1).

## Decision

We will depend on the standard library first. `concurrency` is allowed
for asynchronous results (`KeepAlive` returns
`*concurrency.Future[LeaseInfo]`). `golang.org/x/sys` is allowed for
operating-system locks in package `file`. Adapters for external systems
live in the modules that own those clients. Any new dependency needs an
ADR.

## Consequences

### Positive
- Consumers pull in two small modules.
- `KeepAlive` composes with other `concurrency` futures.

### Negative
- A breaking change in `concurrency`'s `Future` is a breaking change of
  `KeepAlive`.

## Links

- [go.mod](../../go.mod)
