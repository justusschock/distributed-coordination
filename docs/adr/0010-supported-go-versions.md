# ADR 0010: Supported Go versions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0011 |

## Context

Recorded retrospectively for the first public release. The Go project
supports the two most recent major releases. The module's tests use
`testing/synctest`, whose stable API (`synctest.Test`) first shipped in Go
1.25, and the code uses Go 1.22+ features (range over integers,
`math/rand/v2`) and Go 1.25's `sync.WaitGroup.Go` in examples. The
`go` directive was already `1.25.0`.

## Decision drivers

- D1 Tests must build on every supported Go version.
- D2 Support as many toolchains as is practical for consumers.
- D3 CI must prove the claimed range.

## Considered options

1. **Floor at Go 1.25, test 1.25, oldstable and stable**
   - Pro: synctest is available everywhere (D1); covers the two upstream
     releases plus the floor (D2, D3).
   - Con: consumers on Go 1.24 or older cannot use the module.
2. **Floor at an older release, synctest behind build tags**
   - Pro: wider reach (D2).
   - Con: two test code paths; time-dependent tests would fall back to
     wall-clock timing (D1).

## Decision

We will keep `go 1.25.0` in `go.mod` as the minimum and support it plus
the two latest Go releases. CI runs the test job on `1.25.x`, `oldstable`
and `stable`. Raising the floor needs a new ADR and a changelog entry.

## Consequences

### Positive
- One test code path using virtual time.

### Negative
- The floor stays at 1.25 even after it leaves upstream support, until a
  new ADR raises it.

## Links

- [Go release policy](https://go.dev/doc/devel/release#policy)
- [testing/synctest](https://pkg.go.dev/testing/synctest)
