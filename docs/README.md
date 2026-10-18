# Documentation

Requirements (PRDs) and design decisions (ADRs) for
go-distributed-coordination. The process is described in
[ADR-0001](adr/0001-record-architecture-decisions.md) and
[AGENTS.md](../AGENTS.md). New documents start from the templates:
[PRD template](prd/TEMPLATE.md), [ADR template](adr/TEMPLATE.md).

## Product requirements

| PRD | Title | Status | Implementation |
|---|---|---|---|
| [0001](prd/0001-go-distributed-coordination.md) | go-distributed-coordination | Accepted | Complete |

## Architecture decisions

| ADR | Title | Status |
|---|---|---|
| [0001](adr/0001-record-architecture-decisions.md) | Record architecture decisions | Accepted |
| [0002](adr/0002-backend-neutral-contracts-and-package-layout.md) | Backend-neutral contracts and package layout | Accepted |
| [0003](adr/0003-fencing-tokens.md) | Fencing tokens as the correctness primitive | Accepted |
| [0004](adr/0004-dependency-policy.md) | Dependency policy | Accepted |
| [0005](adr/0005-error-semantics.md) | Error semantics | Accepted |
| [0006](adr/0006-concurrency-expiry-and-cancellation.md) | Concurrency, expiry and cancellation model | Accepted |
| [0007](adr/0007-backends-and-conformance-suite.md) | Reference backends and a public conformance suite | Accepted |
| [0008](adr/0008-file-backend-locks-and-state-format.md) | File backend locking and state format | Accepted |
| [0009](adr/0009-public-api-boundary.md) | Public API boundary | Accepted |
| [0010](adr/0010-supported-go-versions.md) | Supported Go versions | Accepted |
| [0011](adr/0011-testing-strategy.md) | Testing strategy | Accepted |
