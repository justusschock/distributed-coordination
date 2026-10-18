# ADR 0007: Reference backends and a public conformance suite

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0008 |

## Context

Recorded retrospectively for the first public release. The value of
backend-neutral contracts depends on every backend behaving the same way:
contention, cancellation, renewal, expiry and token ordering. Backends
are written in other modules (for example an object-store lease adapter),
so the expected behaviour must be executable, not just documented.

## Decision drivers

- D1 Every backend, in any module, must be checkable against one
  specification.
- D2 Applications need a dependency-free backend for tests.
- D3 Single-host tools need real cross-process exclusion without a server.
- D4 The suite must run fast and deterministically against backends whose
  time source the suite does not own.

## Considered options

1. **Public `conformance.Run(t, Factory)` with a clock-advance hook, plus
   `memory` and `file` backends that pass it**
   - Pro: implementers call one function from their tests (D1).
   - Pro: the factory returns a fresh backend and an `advance` function, so
     expiry tests need no real waiting (D4).
   - Pro: `memory` serves tests (D2); `file` serves single-host tools (D3).
   - Con: implementers must make their backend's time controllable.
2. **Documented semantics only**
   - Pro: no test code to maintain.
   - Con: divergence is discovered in production (D1).
3. **Suite driven by real time**
   - Pro: no hook needed.
   - Con: slow and flaky (D4).

## Decision

We will publish package `conformance` with `Factory` and `Run`. `Run`
covers non-waiting contention, waiting cancellation, renewal from current
time, release, expiry, stale renewal after replacement and strictly
increasing tokens; each subtest gets a fresh backend from the factory.
The memory backend is the reference implementation of `Backend`,
`Registry` and `Barrier`; the file backend implements `Backend` for
processes on one host. Both pass `Run` in this repository's tests.

## Consequences

### Positive
- External adapters prove compatibility with a few lines of test code.

### Negative
- `Run` creates subtests with `t.Run`, so it cannot run inside a
  `testing/synctest` bubble; implementers supply a manual clock.
- `Registry` and `Barrier` are not covered by the suite yet.

### Follow-ups
- PRD-0001 Q1 and Q2.

## Links

- [conformance/backend.go](../../conformance/backend.go)
