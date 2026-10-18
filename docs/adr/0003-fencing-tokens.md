# ADR 0003: Fencing tokens as the correctness primitive

| Field | Value |
|---|---|
| Status | Accepted |
| Date | 2026-10-18 |
| Deciders | Justus Perillieux |
| Supersedes | — |
| Related | PRD-0001, ADR-0008 |

## Context

Recorded retrospectively for the first public release. A lease holder can
be paused (garbage collection, a stopped process, a slow network) past its
deadline while another owner acquires the key. When the first holder
resumes it does not know it lost the lease and may write. No lock service
can prevent that write; only the protected resource can reject it.

## Decision drivers

- D1 Late writes from former owners must be rejectable.
- D2 The mechanism must be cheap for the protected resource to check.
- D3 It must work for every backend, including the file backend across
  restarts.

## Considered options

1. **Per-key, strictly increasing integer token on every acquisition**
   - Pro: the resource stores the highest token seen and rejects lower ones
     (D1, D2).
   - Pro: every backend can maintain a counter per key (D3).
   - Con: tokens of different keys are unrelated and must not be compared.
   - Con: backends must keep the counter after release and expiry.
2. **Lease identifiers (random IDs) checked against the backend**
   - Pro: no counter to persist.
   - Con: the resource must call the coordination service on every write (D2).
3. **Rely on TTLs and clocks only**
   - Pro: simplest.
   - Con: does not handle paused holders at all (D1).

## Decision

We will attach a `FencingToken` (`uint64`) to every lease. Each successful
acquisition of a key receives a token strictly greater than every token
previously granted for that key, including after release and expiry.
`Mutex.Lock` and `Leadership.Token` expose the token directly. Backends
never reset the counter: the memory backend keeps it for its lifetime, the
file backend persists it and fails rather than resetting on a corrupt
state file, and token overflow is an error.

## Consequences

### Positive
- Protected resources can reject stale writes with one comparison.
- The conformance suite can verify the property for every backend.

### Negative
- Backends must keep per-key state forever (the file backend never deletes
  lock or state files).
- Correctness depends on the resource actually checking tokens, which the
  library cannot enforce.

## Links

- Martin Kleppmann, "How to do distributed locking" (2016), which describes
  fencing tokens.
