# ADR 0001: Record architecture decisions

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001 |

## Context

Recorded retrospectively for the first public release. The library's
design decisions were made while it was built and lived only in code,
tests, commit messages and the README. Outside contributors and backend
authors need to know what the contracts promise and why, and changes need
a reviewable place where decisions are proposed before code follows.

## Decision drivers

- D1 Contributors must be able to find the reason behind a contract before
  changing it.
- D2 Decisions must be reviewable in pull requests next to the code.
- D3 History must stay intact: a reversed decision should remain readable.
- D4 Low ceremony: plain Markdown in the repository, no extra tooling.

## Considered options

1. **MADR-style ADRs and PRDs in `docs/`, docs first**
   - Pro: versioned with the code, reviewed in the same pull requests (D2).
   - Pro: a fixed structure (context, drivers, options, decision,
     consequences) makes trade-offs explicit (D1).
   - Con: documents must be maintained; stale documents mislead.
2. **README and commit messages only**
   - Pro: no extra files (D4).
   - Con: reasons scatter across history; alternatives are not recorded (D1).
3. **External wiki or issue tracker**
   - Pro: easy to edit.
   - Con: drifts from the code and is not reviewed with it (D2).

## Decision

We will record requirements as PRDs in `docs/prd/` and decisions as
MADR-style ADRs in `docs/adr/`, both from the templates in those
directories. New documents start `Proposed` and become `Accepted` when
merged; behaviour changes follow accepted documents. Accepted ADRs are
immutable except for their status line; a changed decision gets a new ADR
with `Supersedes: ADR-NNNN`, and the old one is marked
`Superseded by ADR-MMMM`. [docs/README.md](../README.md) indexes every
document with its status.

## Consequences

### Positive
- Contracts and their reasons are discoverable next to the code.
- Superseded decisions remain as history.

### Negative
- Every behaviour change carries a documentation step.

### Follow-ups
- Keep PRD-0001 current when behaviour changes.

## Links

- [MADR](https://adr.github.io/madr/)
- [AGENTS.md](../../AGENTS.md)
