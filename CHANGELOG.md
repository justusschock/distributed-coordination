# Changelog

All notable changes to this project are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).
While the major version is `0`, a minor release may break the API.

## [Unreleased]

## [0.1.0] - 2026-10-18

First public release.

### Added

- Backend-neutral contracts: `Backend`, `Lease`, `Registry`, `Membership`,
  `Barrier` and `Clock`.
- Fenced leases: `Backend.Acquire` grants exclusive, expiring ownership of a
  key with a `FencingToken` that strictly increases per key, plus `Renew`
  and `Release`.
- `KeepAlive`: renews a lease every `ttl/3` and reports cancellation, loss
  or renewal errors through a `concurrency` future.
- `NewMutex` and `Campaign`: a fenced mutex and leader election over any
  `Backend`, with `Leadership.Resign` and a `Lost` channel.
- Expiring group membership (`Registry`) with metadata, and reusable
  barriers (`Barrier`) with generations.
- Package `memory`: a deterministic in-process reference backend that
  implements every contract.
- Package `file`: a cross-process backend over operating-system locks in a
  local directory, with crash release and persistent fencing tokens.
- Package `conformance`: `Run`, the test suite every `Backend`
  implementation must pass.
- Sentinel errors `ErrInvalidRequest`, `ErrNotAcquired`, `ErrLeaseLost` and
  `ErrNotOwner`.
- Requirements and architecture decision records under `docs/`.

Requires Go 1.25 or newer.

[Unreleased]: https://github.com/justusschock/distributed-coordination/compare/v0.1.0...HEAD
[0.1.0]: https://github.com/justusschock/distributed-coordination/releases/tag/v0.1.0
