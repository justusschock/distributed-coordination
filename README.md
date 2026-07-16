# go-distributed-coordination

Distributed leases, locks, elections, fencing, and coordination primitives for Go.

> **Status:** Initial repository scaffold; the public API has not been implemented yet.

Module path: `github.com/justusschock/go-distributed-coordination`

## Scope

Backend-neutral coordination contracts and implementations for leases, mutexes, leader election, fencing tokens, barriers, and membership.

This library is private and intended for reuse by future Go projects.

## Target platform dependencies

Required imports:

- [`go-concurrency`](https://github.com/justusschock/go-concurrency)

Optional adapter imports:

None currently planned.

Optional integrations must remain one-way: concrete adapters import their neutral contracts, never the reverse.

## Design constraints

- Keep public contracts independent of product-specific resource types.
- Keep the dependency graph acyclic; imports point toward lower-level mechanisms.
- Put generic lifecycle transfer in `go-lifecycle/handoff` and finite resource-right transfer in `go-resources/handoff`.
- Use `go-cache` only for reconstructible, disposable state, never correctness-critical state.

## Development

Install and run the repository hooks:

```bash
pre-commit install
pre-commit run --all-files
```

Once the module implementation exists, run its Go checks with:

```bash
go test ./...
```

Local implementation plans live under `docs/` and are intentionally excluded from Git.

## License

Apache License 2.0. See [LICENSE](LICENSE).
