// Package coordination defines backend-neutral contracts for distributed
// coordination: fenced leases, lease keepalive, mutexes, leader election,
// expiring group membership and reusable barriers.
//
// A [Backend] grants exclusive, time-bounded [Lease]s on string keys. Every
// successful acquisition carries a [FencingToken] that is strictly greater
// than every token previously granted for the same key, even after release
// or expiry. Protected resources must check the token: a lease alone cannot
// stop a paused former owner from writing late.
//
// The helpers in this package ([KeepAlive], [Mutex], [Campaign]) work with
// any Backend. Implementations live in sub-packages:
//
//   - memory: a synchronized in-process reference backend that also
//     implements [Registry] and [Barrier];
//   - file: a cross-process backend over lock files in a local directory;
//   - conformance: the test suite every Backend implementation must pass.
//
// # Errors
//
// Operations report the sentinel errors [ErrInvalidRequest],
// [ErrNotAcquired], [ErrLeaseLost] and [ErrNotOwner]; test for them with
// [errors.Is]. Canceled operations return the context's error or
// cancellation cause.
//
// # Concurrency
//
// All interfaces in this package must be safe for concurrent use by
// implementations. [Mutex] is safe for concurrent use; [Leadership] values
// may be copied and their Resign method called from any goroutine.
package coordination
