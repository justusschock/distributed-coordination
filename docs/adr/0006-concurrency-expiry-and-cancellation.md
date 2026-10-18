# ADR 0006: Concurrency, expiry and cancellation model

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0005, ADR-0011 |

## Context

Recorded retrospectively for the first public release. Leases and
memberships expire in the background, waiters block until a key frees up,
and holders must learn about loss promptly. The library must define who
observes expiry, how callers wait and cancel, and how time is sourced, in
a way that remains testable.

## Decision drivers

- D1 Blocking operations must be cancellable.
- D2 Loss of ownership must be observable without polling.
- D3 Expiry must be correct even if a timer is delivered late.
- D4 Time must be controllable in tests and by adapters.

## Considered options

1. **`context.Context` on every operation, `Done` channels for loss,
   deadlines compared against a `Clock`, timers only as wakeups**
   - Pro: standard cancellation (D1); `select` on `Done` (D2).
   - Pro: every state transition re-checks `ExpiresAt` against `Now`, so a
     late timer cannot extend ownership (D3).
   - Pro: backends take a `Clock`; tests use `testing/synctest` or a manual
     clock (D4).
   - Con: each lease starts a goroutine waiting for its expiry timer.
2. **Callbacks on loss**
   - Pro: no goroutine per waiter in the caller.
   - Con: callback reentrancy and ordering are hard to specify.
3. **Polling `Info()` for expiry**
   - Pro: no background work.
   - Con: loss is detected late or not at all (D2).

## Decision

We will take `ctx context.Context` as the first argument of every
blocking or I/O operation and return promptly when it is done. Each
`Lease` and `Membership` exposes `Done()`, closed exactly once on release
or loss. Expiry is decided by comparing the stored deadline with
`Clock.Now()` under the backend's lock, on every acquisition, renewal,
release and listing; timers from `Clock.After` only trigger that check.
Renewal extends the deadline from the current time. All exported types
are safe for concurrent use. `KeepAlive` runs on a `concurrency`
goroutine and renews at `ttl/3`; `Campaign` stops it on `Resign`, lease
loss or context cancellation but does not release the lease on
cancellation. In the file backend the holding process enforces expiry,
and other processes cannot pre-empt a suspended holder (fencing covers
that case, ADR-0003).

## Consequences

### Positive
- Behaviour is deterministic under virtual time; the same code runs in
  production with the real clock.
- Late timer delivery cannot break mutual exclusion in the memory backend.

### Negative
- A goroutine per active lease, membership or waiting barrier arrival.
- Canceling a campaign leaves the lease to expire after its TTL.

## Links

- [memory/backend.go](../../memory/backend.go),
  [keepalive.go](../../keepalive.go), [election.go](../../election.go)
