# ADR 0011: Testing strategy

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0006, ADR-0007, ADR-0010 |

## Context

Recorded retrospectively for the first public release. Coordination bugs
are timing bugs: a waiter that misses a wakeup, an expiry that fires one
instant early, a token that repeats after a restart. Wall-clock tests of
such behaviour are slow and flaky, yet the file backend's cross-process
guarantees can only be shown with real processes.

## Decision drivers

- D1 Timing assertions must be exact and deterministic.
- D2 Cross-process guarantees must be tested with real processes.
- D3 Every backend must pass the same contract tests.
- D4 Data races must be caught.

## Considered options

1. **`testing/synctest` for in-process timing, re-executed test binaries
   for cross-process tests, the conformance suite for every backend, race
   detector always on**
   - Pro: virtual time makes exact deadlines testable in microseconds (D1).
   - Pro: helper processes exercise real `flock`/`LockFileEx` and crash
     release (D2).
   - Con: synctest bubbles cannot contain `t.Run`, so the conformance suite
     uses an injected manual clock.
2. **Manual fake clocks everywhere**
   - Pro: works on older Go versions.
   - Con: tests must synchronise with timer registration by hand, and code
     that uses `time` directly (KeepAlive, file polling) cannot be faked.
3. **Real time with generous timeouts**
   - Pro: no test infrastructure.
   - Con: slow, flaky and unable to assert exact instants (D1).

## Decision

We will test time-dependent in-process behaviour inside
`synctest.Test` bubbles with the real clock, using `time.Sleep` to advance
virtual time and `synctest.Wait` to observe blocked goroutines. Where a
test needs delayed timer delivery, it injects a clock that reads virtual
time but delivers timers late. Cross-process tests in package `file`
re-execute the test binary as helper processes and use real time with
bounded timeouts. `conformance.Run` is applied to the memory and file
backends with a manual clock. Every package has runnable examples. Tests
cite the requirement IDs of PRD-0001 they verify. CI runs
`go test -race` on Linux, macOS and Windows; changes are verified with
`-race -count=3`.

## Consequences

### Positive
- Expiry, renewal and wakeup instants are asserted exactly.
- Cross-process exclusion, crash release and persistent tokens are tested
  with real operating-system locks.

### Negative
- Cross-process tests take seconds and depend on process scheduling.
- File I/O inside synctest bubbles is limited to non-blocking operations
  (non-blocking locks, small reads and writes); blocking I/O would not be
  recognised as durably blocked.

## Links

- [testing/synctest](https://pkg.go.dev/testing/synctest)
- [file/process_test.go](../../file/process_test.go)
