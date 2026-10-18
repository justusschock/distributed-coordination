# PRD 0001: go-distributed-coordination

| Field | Value |
|---|---|
| Status | Accepted |
| Implementation | Complete |
| Owners | Justus Perillieux |
| Related ADRs | ADR-0002, ADR-0003, ADR-0004, ADR-0005, ADR-0006, ADR-0007, ADR-0008, ADR-0009, ADR-0010, ADR-0011 |
| Last updated | 2026-10-18 |

Recorded retrospectively for the first public release; describes the
library as implemented.

## 1. Problem and context

Go services and tools that run more than one worker need to coordinate:
only one worker may run a job or own a resource at a time, one node should
lead, workers need to know who else is alive, and phases need a
rendezvous. Each coordination system (etcd, ZooKeeper, a SQL table, an
object store with conditional writes, lock files) offers these primitives
with different APIs and subtly different guarantees, and application code
written against one of them is hard to test and hard to move.

A plain lock is also not enough for correctness: a worker that pauses
(garbage collection, a stopped process, a network partition) can keep
writing after its lease expired and another worker took over. The
protected resource needs a *fencing token* to reject such late writes.

`go-distributed-coordination` defines small, backend-neutral contracts for
fenced leases, membership and barriers, generic helpers built on them
(keepalive, mutex, leader election), two backends (an in-memory reference
and a cross-process local-filesystem backend) and a conformance suite so
third-party backends prove the same observable semantics.

## 2. Users and use cases

- As a **service author**, I want to acquire an exclusive, expiring lease on
  a key and get a fencing token so that my protected store can reject writes
  from a former owner.
- As a **service author**, I want a lease to be renewed automatically while
  long work runs and to be told when it is lost so that I stop working
  without ownership.
- As a **service author**, I want a mutex and a leader election built on any
  backend so that I do not reimplement waiting, renewal and resignation.
- As an **operator of a worker pool**, I want expiring group membership with
  metadata and a reusable barrier so that workers can discover each other and
  synchronise phases.
- As a **CLI or tool author**, I want separate processes on one machine to
  exclude each other through a directory of lock files, with crash release,
  so that I do not need a coordination server.
- As a **backend author**, I want a reusable test suite so that my adapter
  (database, object store, consensus system) provably behaves like the
  reference backend.
- As a **test author**, I want a deterministic in-memory backend so that
  coordination logic can be tested without infrastructure.

## 3. Goals and non-goals

### Goals

- G1 Backend-neutral interfaces (`Backend`, `Lease`, `Registry`,
  `Membership`, `Barrier`, `Clock`) with precisely specified errors,
  expiry and cancellation behaviour.
- G2 Fencing tokens strictly increasing per key across release and expiry.
- G3 Helpers that work with any `Backend`: `KeepAlive`, `Mutex`, `Campaign`.
- G4 A synchronized in-memory reference backend implementing all contracts.
- G5 A cross-process backend for processes on one host with crash release
  and persistent tokens.
- G6 A public conformance suite for `Backend` implementations.

### Non-goals

- NG1 Adapters for etcd, Consul, ZooKeeper, SQL, Redis, Kubernetes or network
  services; they belong in the modules that own those dependencies (one such
  adapter lives in `go-object-storage`).
- NG2 Distributed transactions, scheduling, work queues or resource
  selection; these are built on top of leases by other libraries.
- NG3 Enforcing fencing at the protected resource; the library hands out
  tokens, the resource must check them.
- NG4 Coordination across machines with the file backend; network
  filesystems do not give reliable lock semantics.

## 4. Functional requirements

| ID | Requirement | Priority |
|---|---|---|
| FR-COORD-01 | `Backend.Acquire` grants an exclusive lease on a key for a TTL and returns `ErrInvalidRequest` for an empty key or owner or a non-positive TTL. `Lease.Renew` rejects a non-positive TTL with `ErrInvalidRequest`. | MUST |
| FR-COORD-02 | A contended acquisition with `Wait: false` returns `ErrNotAcquired` immediately. | MUST |
| FR-COORD-03 | A contended acquisition with `Wait: true` waits for release or expiry of the holder and returns the context's error when its context is done first. | MUST |
| FR-COORD-04 | Each successful acquisition of a key carries a `FencingToken` strictly greater than every token previously granted for that key, including after release and expiry. | MUST |
| FR-COORD-05 | `Lease.Done` closes exactly once on release or loss; `Renew` extends the deadline from the backend clock's current time; `Renew` and `Release` return `ErrLeaseLost` after release, expiry or replacement. | MUST |
| FR-COORD-06 | `KeepAlive` renews a lease immediately and then every `ttl/3` (at least every nanosecond) and returns a `go-concurrency` future that resolves with the cancellation cause, `ErrLeaseLost` on lease loss, or the first renewal error; it rejects a nil lease or non-positive TTL with `ErrInvalidRequest`. | MUST |
| FR-COORD-07 | `Mutex` (`NewMutex`, `Lock`, `Unlock`) wraps a waiting acquisition for one key and owner, returns the fencing token from `Lock`, rejects duplicate or concurrent `Lock` and `Unlock` without a lease with `ErrNotOwner`, and validates its arguments with `ErrInvalidRequest`. | MUST |
| FR-COORD-08 | `Campaign` waits for the lease `election/<name>`, keeps it alive, exposes the token and a `Lost` channel that closes on lease loss or keepalive end, and `Leadership.Resign` releases at most once, returning the first result to every call. | MUST |
| FR-COORD-09 | A `Registry` registers expiring members with defensively copied metadata, rejects a duplicate active group/ID with `ErrNotAcquired`, allows rejoining after leave or expiry, lists active members sorted by ID, closes `Membership.Done` on expiry or leave, and returns `ErrLeaseLost` from stale `Renew` and `Leave`. | MUST |
| FR-COORD-10 | A `Barrier` completes a generation when `Parties` distinct, unexpired participants have arrived, returns sorted participants and monotonically increasing generations, fixes the party count on first use (`ErrInvalidRequest` on mismatch), counts a participant once per generation, removes canceled and expired arrivals (`ErrNotAcquired` on expiry) and prunes expired arrivals by deadline before completion. | MUST |
| FR-COORD-11 | Package `memory` provides `NewBackend(clock)` implementing `Backend`, `Registry` and `Barrier`; a nil clock selects the real clock. | MUST |
| FR-COORD-12 | Package `file` provides a `Backend` whose leases exclude each other across processes, `Backend` values and lease handles on one host through operating-system advisory locks, and are released by the operating system when the holding process dies. | MUST |
| FR-COORD-13 | The file backend persists the latest lease (key, owner, token, acquisition time, deadline) per key under the lock so tokens increase across processes and restarts; a corrupt or foreign state file fails acquisition instead of resetting the token. | MUST |
| FR-COORD-14 | The file backend enforces expiry in the holding process, wakes local waiters on release or expiry, and lets other processes poll with exponential backoff from `interval/16` capped at the poll interval, with jitter. | MUST |
| FR-COORD-15 | The file backend maps every key to a distinct flat, filesystem-safe file name (readable prefix plus SHA-256), and a lease whose lock file was removed or replaced is lost. | MUST |
| FR-COORD-16 | `file.NewBackend(dir, opts...)` creates the directory, accepts `WithClock`, `WithPollInterval` and `WithFileMode`, and rejects an empty directory, a non-positive poll interval and file modes without owner read/write or with non-permission bits with `ErrInvalidRequest`. | MUST |
| FR-COORD-17 | Package `conformance` provides `Run(t, Factory)`, which verifies contention, waiting cancellation, renewal, release, expiry, stale renewal and strictly increasing tokens for any `Backend`. | MUST |

## 5. Non-functional requirements

| ID | Requirement | Measure |
|---|---|---|
| NFR-COORD-01 | Every exported type is safe for concurrent use or documents otherwise. | `go test -race -count=3 ./...` passes. |
| NFR-COORD-02 | Tests are deterministic: time-dependent tests use `testing/synctest` or an injected clock; only cross-process tests use real time. | No wall-clock sleeps in in-process tests; repeated runs pass. |
| NFR-COORD-03 | Minimal dependencies: the standard library, `go-concurrency` and `golang.org/x/sys` only. | `go.mod` require block. |
| NFR-COORD-04 | The file backend supports Linux, macOS, the BSDs and Windows; other platforms get `errors.ErrUnsupported` from `NewBackend`. | CI runs file and conformance tests on Linux, macOS and Windows. |
| NFR-COORD-05 | Supports Go 1.25 and the two latest Go releases. | CI matrix `1.25.x`, `oldstable`, `stable`. |
| NFR-COORD-06 | The library never logs; it reports through return values, futures and channels. | Code review; no `log` or `fmt.Print` in non-test code. |
| NFR-COORD-07 | File backend state writes are crash-safe: synced temporary file, atomic rename, directory sync where supported. | Code review of `writeRecord`; `TestCorruptStateIsNotReset`. |

## 6. API sketch

```go
package coordination

type FencingToken uint64
type AcquireRequest struct { Key, Owner string; TTL time.Duration; Wait bool }
type LeaseInfo struct { Key, Owner string; Token FencingToken; AcquiredAt, ExpiresAt time.Time }
type Lease interface {
	Info() LeaseInfo
	Done() <-chan struct{}
	Renew(ctx context.Context, ttl time.Duration) (LeaseInfo, error)
	Release(ctx context.Context) error
}
type Backend interface { Acquire(ctx context.Context, request AcquireRequest) (Lease, error) }
type Clock interface { Now() time.Time; After(d time.Duration) <-chan time.Time }

func KeepAlive(ctx context.Context, lease Lease, ttl time.Duration) (*concurrency.Future[LeaseInfo], error)
func NewMutex(backend Backend, key, owner string, ttl time.Duration) (*Mutex, error)
func (m *Mutex) Lock(ctx context.Context) (FencingToken, error)
func (m *Mutex) Unlock(ctx context.Context) error
func Campaign(ctx context.Context, backend Backend, options ElectionOptions) (Leadership, error)
func (l Leadership) Resign(ctx context.Context) error

type Registry interface {
	Join(ctx context.Context, member Member, ttl time.Duration) (Membership, error)
	List(ctx context.Context, group string) ([]MemberInfo, error)
}
type Barrier interface { Wait(ctx context.Context, request BarrierRequest) (BarrierResult, error) }

var ErrInvalidRequest, ErrNotAcquired, ErrLeaseLost, ErrNotOwner error

package memory
func NewBackend(clock coordination.Clock) *Backend // Backend, Registry, Barrier

package file
func NewBackend(dir string, opts ...Option) (*Backend, error)
func WithClock(coordination.Clock) Option
func WithPollInterval(time.Duration) Option
func WithFileMode(fs.FileMode) Option

package conformance
type Factory func(t *testing.T) (coordination.Backend, func(time.Duration))
func Run(t *testing.T, factory Factory)
```

## 7. Acceptance criteria

| Criterion | Requirement(s) | Test(s) |
|---|---|---|
| Invalid requests are rejected | FR-COORD-01 | `TestBackendRejectsInvalidAndContendedAcquisitions`, `TestBackendRejectsInvalidRequests` |
| Immediate contention fails fast | FR-COORD-02 | `TestBackendRejectsInvalidAndContendedAcquisitions`, `TestBackendsSharingDirectoryExcludeEachOther`, `TestMemoryBackend`, `TestFileBackend` |
| Waiting acquisition wakes on expiry and honours cancellation | FR-COORD-03 | `TestBackendExpiryLosesLeaseAndAllowsWaiter`, `TestBackendWaitingAcquisitionHonorsCancellation`, `TestMutexWaitingLockHonorsCancellation`, `TestWaitingAcquisitionWakesOnLocalRelease`, `TestWaitingAcquisitionHonorsDeadline` |
| Tokens strictly increase | FR-COORD-04 | `TestBackendFencingTokensNeverRepeat`, `TestMutexSerializesOwnersAndAdvancesToken`, `TestFencedMutexIntegrationRejectsStaleOwnerWrites`, `TestFencingTokensPersistAcrossBackends` |
| Lease lifecycle (Done, Renew, Release, ErrLeaseLost) | FR-COORD-05 | `TestBackendExpiryLosesLeaseAndAllowsWaiter`, `TestBackendLostLeaseCannotRenewOrRelease`, `TestLeaseExpiresWithoutRenewal`, `TestRenewExtendsAndPersistsDeadline` |
| KeepAlive schedule and termination | FR-COORD-06 | `TestKeepAliveRenewsImmediatelyThenAtOneThirdTTL`, `TestKeepAliveValidatesArgumentsAndReportsCancellationCause`, `TestKeepAliveStopsWhenLeaseIsLost`, `TestKeepAliveIntervalIsAlwaysPositive` |
| Mutex semantics | FR-COORD-07 | `TestMutexReturnsFencingToken`, `TestNewMutexValidatesArguments`, `TestMutexSerializesOwnersAndAdvancesToken`, `TestMutexRejectsDuplicateOwnershipOperations`, `TestMutexRejectsConcurrentLockWhileAcquisitionIsInFlight` |
| Leader election | FR-COORD-08 | `TestCampaignAcquiresWaitingLeaseAndExposesLifetime`, `TestCampaignValidatesOptionsAndPropagatesAcquisitionError`, `TestCampaignKeepsLeadershipUntilContextCancellation` |
| Membership | FR-COORD-09 | `TestMembershipCopiesMetadataAndListsMembersByID`, `TestMembershipRejectsInvalidAndDuplicateJoins`, `TestMembershipExpiryClosesDoneAndRejectsStaleOperations`, `TestMembershipRenewExtendsLifetime`, `TestMembershipCanRejoinExpiredRecordBeforeTimerDelivery` |
| Barriers | FR-COORD-10 | `TestBarrierReturnsSortedParticipantsAndIncreasingGenerations`, `TestBarrierRejectsInvalidRequests`, `TestBarrierDuplicateArrivalIsIdempotent`, `TestBarrierRejectsMismatchedPartyCount`, `TestBarrierCancellationRemovesUncompletedArrival`, `TestBarrierTTLRemovesUncompletedArrival`, `TestBarrierPrunesExpiredArrivalBeforeDelayedTimerDelivery`, `TestBarrierExpiredArrivalWinsWhenCompletionIsAlsoReady` |
| Memory backend passes the suite | FR-COORD-11 | `TestMemoryBackend` |
| Cross-process exclusion and crash release | FR-COORD-12 | `TestBackendsSharingDirectoryExcludeEachOther`, `TestCrossProcessExclusion`, `TestCrossProcessCrashReleasesLock`, `TestCrossProcessMutualExclusion` |
| Persistent tokens, corrupt state | FR-COORD-13 | `TestFencingTokensPersistAcrossBackends`, `TestCorruptStateIsNotReset`, `TestCrossProcessMutualExclusion` |
| Holder-enforced expiry, wakeup and polling | FR-COORD-14 | `TestLeaseExpiresWithoutRenewal`, `TestRenewExtendsAndPersistsDeadline`, `TestWaitingAcquisitionWakesOnLocalRelease`, `TestWaitingAcquisitionPollsForOtherHolders`, `TestCrossProcessExpiredHolderLosesLease` |
| Key encoding and lock identity | FR-COORD-15 | `TestKeysMapToDistinctFlatFiles`, `TestRemovedLockFileLosesLease` |
| File backend options | FR-COORD-16 | `TestNewBackendRejectsInvalidOptions`, `TestNewBackendCreatesDirectory` |
| Conformance suite | FR-COORD-17 | `TestMemoryBackend`, `TestFileBackend` |

## 8. Source references

None.

## 9. Open questions

- Q1 Should the conformance suite also cover `Registry` and `Barrier`? Today
  only the memory backend implements them, so their contract is verified by
  the memory package's own tests.
- Q2 Should `conformance.Run` offer a variant that runs inside
  `testing/synctest` bubbles, so implementers can drop their manual clocks?
  Subtests created with `t.Run` cannot run inside a bubble, so this needs a
  different suite shape.
