# ADR 0009: Public API boundary

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0002, ADR-0005 |

## Context

Recorded retrospectively for the first public release. Before publishing,
every exported package and identifier was reviewed and classified as
public contract or implementation detail. The module is already used by
other libraries and applications (job runners, caches, session stores,
object-storage adapters, command-line tools), so nothing they import may
disappear. Their usage covers the lease contracts and errors, `Clock`
(as an adapter option), `KeepAlive`, `memory.NewBackend`,
`file.NewBackend` and `conformance.Run`.

## Decision drivers

- D1 The public surface is exactly what users and backend implementers
  need.
- D2 No identifier used by a known consumer is removed or moved.
- D3 Implementation details stay unexported or under `internal/`.

## Considered options

1. **Keep the existing packages public, remove only dead API**
   - Pro: every package is a contract or a backend users construct (D1, D2).
   - Con: none significant; the implementation details were already
     unexported.
2. **Move `memory` or `file` under `internal/`**
   - Pro: smaller surface.
   - Con: consumers construct both directly (D2).
3. **Remove `Clock` in favour of `testing/synctest`**
   - Pro: one less interface.
   - Con: an external lease adapter exposes `coordination.Clock` in its
     options, and `conformance.Factory`'s advance hook assumes an injectable
     clock (D2).

## Decision

We will keep these packages and identifiers public:

- `coordination` (root): `FencingToken`, `AcquireRequest`, `LeaseInfo`,
  `Lease`, `Backend`, `Clock`, `KeepAlive`, `Mutex`, `NewMutex`,
  `ElectionOptions`, `Leadership`, `Campaign`, `Member`, `MemberInfo`,
  `Membership`, `Registry`, `BarrierRequest`, `BarrierResult`, `Barrier`,
  `ErrInvalidRequest`, `ErrNotAcquired`, `ErrLeaseLost`, `ErrNotOwner`.
- `memory`: `Backend`, `NewBackend`.
- `file`: `Backend`, `NewBackend`, `Option`, `WithClock`,
  `WithPollInterval`, `WithFileMode`.
- `conformance`: `Factory`, `Run`.

We will remove `ErrClosed`, which was documented as reserved, was returned
by no code in the module and is referenced by no known consumer. Lease,
record, barrier and lock internals (record types, the real clock, key
encoding, the JSON state record, platform lock functions) remain
unexported. There is no `internal/` package: no implementation detail is
shared between packages, and test helpers (manual clocks, fake leases) are
local to the tests that use them.

## Consequences

### Positive
- The documented surface matches what consumers use.

### Negative
- Removing `ErrClosed` is a breaking change for any unknown code that
  referenced it; it is listed in the changelog.

### Follow-ups
- New implementation details shared between packages go under
  `internal/`.

## Links

- [CHANGELOG.md](../../CHANGELOG.md)
