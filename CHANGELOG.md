# Changelog

All notable changes to this project are documented in this file. The
format is based on [Keep a Changelog](https://keepachangelog.com/en/1.1.0/),
and this project adheres to [Semantic Versioning](https://semver.org/).
While the major version is 0, minor releases may contain breaking changes;
they are listed here.

## [Unreleased]

Preparation for the first public release.

### Removed

- **Breaking:** `coordination.ErrClosed`. It was reserved and never
  returned by any code in this module ([ADR-0009](docs/adr/0009-public-api-boundary.md)).

### Changed

- The minimum Go version stays 1.25, now documented as the floor required
  by `testing/synctest`; CI tests Go 1.25, oldstable and stable ([ADR-0010](docs/adr/0010-supported-go-versions.md)).
- Time-dependent tests run in `testing/synctest` bubbles instead of on
  wall-clock time or a hand-rolled manual clock.
- CI pins actions to commit SHAs, adds golangci-lint and no longer needs
  private-module credentials; Dependabot keeps modules and actions current.

### Added

- Doc comments for every exported identifier and runnable examples for
  every public entry point.
- `docs/` with the retrospective PRD and ADRs, `AGENTS.md`, `NOTICE` and
  this changelog.

## [0.3.0] - 2026-10-15

### Added

- Package `file`: a cross-process backend over lock files in a local
  directory, with persistent fencing tokens and crash release.

### Changed

- The minimum Go version is 1.25.

## [0.2.0] - 2026-07-17

### Changed

- The minimum Go version is 1.22; the module depends on
  `go-concurrency` v0.2.0.

## [0.1.0] - 2026-07-17

### Added

- Fenced lease contracts, `KeepAlive`, `Mutex`, `Campaign`, membership and
  barrier contracts, the `memory` backend and the `conformance` suite.
