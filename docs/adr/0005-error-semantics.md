# ADR 0005: Error semantics

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0009 |

## Context

Recorded retrospectively for the first public release. Callers must react
differently to a malformed request, to contention, to having lost
ownership and to cancellation. Backends in other modules must report
these outcomes the same way so helpers and applications work unchanged.

## Decision drivers

- D1 Outcomes callers act on must be distinguishable with `errors.Is`.
- D2 Backends may add context (paths, keys) without breaking checks.
- D3 Cancellation must look like cancellation, including custom causes.

## Considered options

1. **A small set of sentinel errors; context errors returned as is**
   - Pro: `errors.Is` works through wrapping (D1, D2).
   - Pro: callers see `context.Canceled`, `context.DeadlineExceeded` or the
     cancellation cause directly (D3).
   - Con: sentinels carry no structured detail.
2. **Typed errors with fields (key, owner, token)**
   - Pro: richer diagnostics.
   - Con: more API surface for backends to reproduce; the fields are
     rarely needed to decide what to do.
3. **Boolean results (`acquired bool`)**
   - Pro: simple for contention.
   - Con: cannot express lost ownership or invalid input.

## Decision

We will expose four sentinels: `ErrInvalidRequest` (invalid fields or
counts, non-positive TTL), `ErrNotAcquired` (immediate contention,
duplicate active membership, expired barrier arrival), `ErrLeaseLost`
(expired, released or replaced lease or membership) and `ErrNotOwner`
(mutex or leadership operation without ownership). Canceled operations
return `ctx.Err()` or `context.Cause(ctx)`; helpers that run goroutines
(`KeepAlive`, `Barrier.Wait`) report the cause. Backends may wrap
sentinels with `%w`; I/O and corruption errors of the file backend are
returned wrapped with context and are deliberately not `ErrNotAcquired`,
so a waiting acquisition does not spin on them.

## Consequences

### Positive
- Callers handle every documented outcome with `errors.Is`.

### Negative
- `ErrNotAcquired` covers three situations; the operation tells them
  apart.

### Follow-ups
- The unused, reserved `ErrClosed` sentinel was removed before the first
  public release (ADR-0009).

## Links

- [errors.go](../../errors.go)
