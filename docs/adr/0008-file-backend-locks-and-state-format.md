# ADR 0008: File backend locking and state format

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0003, ADR-0007 |

## Context

Recorded retrospectively for the first public release. Command-line tools
and local daemons need separate processes on one machine to exclude each
other, survive crashes without waiting for a TTL, and keep fencing tokens
increasing across restarts. The directory may be shared by any number of
processes and `Backend` values.

## Decision drivers

- D1 A crashed holder must not block others until its TTL.
- D2 Tokens must survive release, process exit and corrupt writes.
- D3 Any key string must map to a safe, distinct file name on every
  supported filesystem, including case-insensitive ones and Windows.
- D4 A lease must never be held on a file others cannot see.

## Considered options

1. **Operating-system advisory lock per key plus a JSON state file written
   by atomic rename**
   - Pro: the kernel drops `flock`/`LockFileEx` locks when the process
     dies (D1).
   - Pro: the state file is read and written only under the lock through a
     synced temporary file, `rename` and a directory sync (D2).
   - Pro: JSON is inspectable by operators.
   - Con: advisory locks only exclude cooperating processes and are
     unreliable on network filesystems.
2. **Exclusive-create lock files (`O_EXCL`) with PID and deadline**
   - Pro: no system-call dependency.
   - Con: a crashed holder leaves a stale file; others must wait for the
     deadline or guess liveness (D1).
3. **An embedded database file (for example bbolt)**
   - Pro: transactions.
   - Con: a new dependency and a single global writer lock for all keys.

## Decision

We will map each key to `<prefix>-<sha256hex>` in one flat directory,
where the prefix is up to 48 lowercase characters of the key with
characters outside `[a-z0-9_-]` replaced by `_`. Each key has a
`<name>.lock` file, the target of an exclusive non-blocking `flock` on
Linux, macOS and the BSDs or `LockFileEx` on the first byte on Windows,
and a `<name>.state` file holding JSON
`{"key","owner","token","acquired_at","expires_at"}`. Every lease handle
opens its own descriptor (close-on-exec). After locking, the backend
checks that the open file is still the file at the lock path, otherwise
it treats the key as not acquired. State is written to `<name>.state.tmp`,
synced, renamed over the state file and the directory is synced
(a no-op on Windows). A state file that fails to parse or names another
key fails acquisition. Lock and state files are never deleted. Other
platforms get `errors.ErrUnsupported` from `NewBackend`. The JSON field
names are a persistence format: changing them needs an ADR.

## Consequences

### Positive
- Crash release without TTL waits; tokens increase across restarts.
- Keys may contain `/`, `..`, reserved device names or any Unicode.

### Negative
- One lock and one state file per key ever used remain in the directory.
- Not suitable for network filesystems or cross-machine coordination.
- Waiters in other processes poll (`WithPollInterval`, default 100ms,
  exponential backoff from interval/16 with ±25% jitter).

## Links

- [file/backend.go](../../file/backend.go),
  [file/lock_flock.go](../../file/lock_flock.go),
  [file/lock_windows.go](../../file/lock_windows.go)
