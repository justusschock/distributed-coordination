# ADR 0002: Backend-neutral contracts and package layout

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0007 |

## Context

Recorded retrospectively for the first public release. Coordination
systems differ widely (consensus stores, databases, object stores, lock
files), but applications need the same few primitives: an exclusive
expiring lease, group membership and a rendezvous. Applications and
higher-level libraries (job runners, caches, session stores) should be
written once against small contracts and wired to a backend at startup.

## Decision drivers

- D1 Backends must be easy to implement and easy to fake in tests.
- D2 Helpers (keepalive, mutex, election) should work with every backend.
- D3 The root package must not pull in backend dependencies.
- D4 Request and result types should evolve without breaking callers.

## Considered options

1. **Small interfaces in the root package, helpers as functions over them,
   backends in sub-packages**
   - Pro: `Backend` has one method, `Lease` four; fakes are a few lines (D1).
   - Pro: `KeepAlive`, `Mutex` and `Campaign` depend only on `Backend` and
     `Lease` (D2).
   - Pro: importing the root package imports no backend (D3).
   - Con: capabilities beyond leases (`Registry`, `Barrier`) are separate
     interfaces a backend may or may not implement.
2. **One large interface covering leases, membership and barriers**
   - Pro: one type to pass around.
   - Con: every backend must implement everything; most lease stores have
     no natural barrier (D1).
3. **Generic, type-parameterised contracts (for example over key or token
   types)**
   - Pro: flexible typing.
   - Con: keys are strings and tokens are ordered integers in every target
     system; type parameters add noise without benefit.

## Decision

We will define the contracts as small interfaces in package
`coordination`: `Backend` and `Lease` for fenced leases, `Registry` and
`Membership` for expiring membership, `Barrier` for reusable rendezvous and
`Clock` for time. Requests and results are plain structs
(`AcquireRequest`, `LeaseInfo`, `Member`, `MemberInfo`, `BarrierRequest`,
`BarrierResult`, `ElectionOptions`) so fields can be added compatibly.
Helpers are functions and types in the root package that accept a
`Backend`. Implementations live in sub-packages (`memory`, `file`); their
configuration uses constructor arguments or functional options
(`file.Option`). The conformance suite lives in `conformance`.

## Consequences

### Positive
- Adapters in other modules implement `Backend` without depending on any
  backend here.
- Applications test against `memory` and run against another backend.

### Negative
- `Registry` and `Barrier` have only the memory implementation and no
  conformance suite yet.
- `Leadership` is a struct with exported fields; changing them is a
  breaking change.

### Follow-ups
- Consider conformance suites for `Registry` and `Barrier` (PRD-0001 Q1).

## Links

- [lease.go](../../lease.go), [membership.go](../../membership.go),
  [barrier.go](../../barrier.go)
