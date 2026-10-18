package file_test

import (
	"context"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"testing/synctest"
	"time"

	coordination "github.com/justusschock/distributed-coordination"
	"github.com/justusschock/distributed-coordination/file"
)

func newBackend(t *testing.T, dir string, opts ...file.Option) *file.Backend {
	t.Helper()
	backend, err := file.NewBackend(dir, opts...)
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func acquire(t *testing.T, backend coordination.Backend, key, owner string, ttl time.Duration) coordination.Lease {
	t.Helper()
	lease, err := backend.Acquire(context.Background(), coordination.AcquireRequest{Key: key, Owner: owner, TTL: ttl})
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

func isClosed(done <-chan struct{}) bool {
	select {
	case <-done:
		return true
	default:
		return false
	}
}

type stateRecord struct {
	Key       string    `json:"key"`
	Owner     string    `json:"owner"`
	Token     uint64    `json:"token"`
	ExpiresAt time.Time `json:"expires_at"`
}

func readState(t *testing.T, dir string) stateRecord {
	t.Helper()
	matches, err := filepath.Glob(filepath.Join(dir, "*.state"))
	if err != nil {
		t.Fatal(err)
	}
	if len(matches) != 1 {
		t.Fatalf("state files = %v, want exactly one", matches)
	}
	data, err := os.ReadFile(matches[0])
	if err != nil {
		t.Fatal(err)
	}
	var record stateRecord
	if err := json.Unmarshal(data, &record); err != nil {
		t.Fatal(err)
	}
	return record
}

// FR-COORD-16
func TestNewBackendRejectsInvalidOptions(t *testing.T) {
	dir := t.TempDir()
	for name, opts := range map[string][]file.Option{
		"zero poll interval":     {file.WithPollInterval(0)},
		"negative poll interval": {file.WithPollInterval(-time.Second)},
		"unwritable file mode":   {file.WithFileMode(0o444)},
		"non-permission bits":    {file.WithFileMode(os.ModeDir | 0o644)},
	} {
		if _, err := file.NewBackend(dir, opts...); !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Errorf("%s: NewBackend() error = %v, want ErrInvalidRequest", name, err)
		}
	}
	if _, err := file.NewBackend(""); !errors.Is(err, coordination.ErrInvalidRequest) {
		t.Errorf("empty dir: NewBackend() error = %v, want ErrInvalidRequest", err)
	}
}

// FR-COORD-16
func TestNewBackendCreatesDirectory(t *testing.T) {
	dir := filepath.Join(t.TempDir(), "nested", "locks")
	backend := newBackend(t, dir)
	lease := acquire(t, backend, "repo", "one", time.Minute)
	defer func() { _ = lease.Release(context.Background()) }()
	if info, err := os.Stat(dir); err != nil || !info.IsDir() {
		t.Fatalf("Stat(%s) = %v, %v; want directory", dir, info, err)
	}
}

// FR-COORD-01
func TestBackendRejectsInvalidRequests(t *testing.T) {
	backend := newBackend(t, t.TempDir())
	for _, request := range []coordination.AcquireRequest{
		{Owner: "owner", TTL: time.Second},
		{Key: "key", TTL: time.Second},
		{Key: "key", Owner: "owner"},
	} {
		if _, err := backend.Acquire(context.Background(), request); !errors.Is(err, coordination.ErrInvalidRequest) {
			t.Fatalf("Acquire(%+v) error = %v, want ErrInvalidRequest", request, err)
		}
	}
	lease := acquire(t, backend, "key", "one", time.Minute)
	defer func() { _ = lease.Release(context.Background()) }()
	if _, err := lease.Renew(context.Background(), 0); !errors.Is(err, coordination.ErrInvalidRequest) {
		t.Fatalf("Renew(0) error = %v, want ErrInvalidRequest", err)
	}
}

// FR-COORD-12, FR-COORD-02
func TestBackendsSharingDirectoryExcludeEachOther(t *testing.T) {
	dir := t.TempDir()
	first, second := newBackend(t, dir), newBackend(t, dir)

	lease := acquire(t, first, "repo", "one", time.Minute)
	_, err := second.Acquire(context.Background(), coordination.AcquireRequest{Key: "repo", Owner: "two", TTL: time.Minute})
	if !errors.Is(err, coordination.ErrNotAcquired) {
		t.Fatalf("contended Acquire() error = %v, want ErrNotAcquired", err)
	}
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	next := acquire(t, second, "repo", "two", time.Minute)
	defer func() { _ = next.Release(context.Background()) }()
	if next.Info().Token <= lease.Info().Token {
		t.Fatalf("tokens = %d then %d, want increasing", lease.Info().Token, next.Info().Token)
	}
}

// FR-COORD-13, FR-COORD-04
func TestFencingTokensPersistAcrossBackends(t *testing.T) {
	dir := t.TempDir()
	var last coordination.FencingToken
	for i := range 3 {
		lease := acquire(t, newBackend(t, dir), "repo", "owner", time.Minute)
		if token := lease.Info().Token; token != last+1 {
			t.Fatalf("acquisition %d token = %d, want %d", i, token, last+1)
		}
		last = lease.Info().Token
		if err := lease.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
	}
	record := readState(t, dir)
	if record.Key != "repo" || record.Owner != "owner" || record.Token != uint64(last) {
		t.Fatalf("persisted state = %+v, want key repo, owner owner, token %d", record, last)
	}
}

// FR-COORD-15
func TestKeysMapToDistinctFlatFiles(t *testing.T) {
	dir := t.TempDir()
	backend := newBackend(t, dir)
	keys := []string{"data/a.csv", "Data/A.csv", "data_a_csv", "data/a/csv", "../escape", "CON", strings.Repeat("long/", 100)}
	for _, key := range keys {
		lease := acquire(t, backend, key, "owner", time.Minute)
		defer func() { _ = lease.Release(context.Background()) }()
	}
	entries, err := os.ReadDir(dir)
	if err != nil {
		t.Fatal(err)
	}
	locks := 0
	for _, entry := range entries {
		if entry.IsDir() {
			t.Fatalf("unexpected subdirectory %s", entry.Name())
		}
		if len(entry.Name()) > 128 {
			t.Fatalf("file name %q is too long", entry.Name())
		}
		if strings.HasSuffix(entry.Name(), ".lock") {
			locks++
		}
	}
	if locks != len(keys) {
		t.Fatalf("lock files = %d, want %d", locks, len(keys))
	}
}

// FR-COORD-13
func TestCorruptStateIsNotReset(t *testing.T) {
	dir := t.TempDir()
	backend := newBackend(t, dir)
	lease := acquire(t, backend, "repo", "one", time.Minute)
	if err := lease.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	matches, _ := filepath.Glob(filepath.Join(dir, "*.state"))
	if err := os.WriteFile(matches[0], []byte("{truncated"), 0o644); err != nil {
		t.Fatal(err)
	}
	_, err := backend.Acquire(context.Background(), coordination.AcquireRequest{Key: "repo", Owner: "two", TTL: time.Minute})
	if err == nil || errors.Is(err, coordination.ErrNotAcquired) {
		t.Fatalf("Acquire() with corrupt state error = %v, want corruption error", err)
	}
	// The failed attempt must not hold the lock.
	_, err = backend.Acquire(context.Background(), coordination.AcquireRequest{Key: "repo", Owner: "two", TTL: time.Minute})
	if err == nil || errors.Is(err, coordination.ErrNotAcquired) {
		t.Fatalf("second Acquire() error = %v, want corruption error", err)
	}
}

// FR-COORD-15
func TestRemovedLockFileLosesLease(t *testing.T) {
	dir := t.TempDir()
	backend := newBackend(t, dir)
	lease := acquire(t, backend, "repo", "one", time.Minute)
	matches, _ := filepath.Glob(filepath.Join(dir, "*.lock"))
	if err := os.Remove(matches[0]); err != nil {
		_ = lease.Release(context.Background())
		t.Skipf("platform does not allow removing a locked file: %v", err)
	}
	if _, err := lease.Renew(context.Background(), time.Minute); !errors.Is(err, coordination.ErrLeaseLost) {
		t.Fatalf("Renew() error = %v, want ErrLeaseLost", err)
	}
	if !isClosed(lease.Done()) {
		t.Fatal("Done did not close after losing the lock file")
	}
	next := acquire(t, backend, "repo", "two", time.Minute)
	defer func() { _ = next.Release(context.Background()) }()
	if next.Info().Token <= lease.Info().Token {
		t.Fatalf("tokens = %d then %d, want increasing", lease.Info().Token, next.Info().Token)
	}
}

// FR-COORD-14, FR-COORD-05
func TestLeaseExpiresWithoutRenewal(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newBackend(t, t.TempDir())
		lease := acquire(t, backend, "repo", "one", time.Second)

		time.Sleep(time.Second - time.Nanosecond)
		synctest.Wait()
		if isClosed(lease.Done()) {
			t.Fatal("lease expired before its deadline")
		}

		time.Sleep(time.Nanosecond)
		synctest.Wait()
		if !isClosed(lease.Done()) {
			t.Fatal("lease did not expire at its deadline")
		}
		if _, err := lease.Renew(context.Background(), time.Second); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("Renew() error = %v, want ErrLeaseLost", err)
		}
		if err := lease.Release(context.Background()); !errors.Is(err, coordination.ErrLeaseLost) {
			t.Fatalf("Release() error = %v, want ErrLeaseLost", err)
		}

		next := acquire(t, backend, "repo", "two", time.Second)
		defer func() { _ = next.Release(context.Background()) }()
		if next.Info().Token <= lease.Info().Token {
			t.Fatalf("tokens = %d then %d, want increasing", lease.Info().Token, next.Info().Token)
		}
	})
}

// FR-COORD-14, FR-COORD-05
func TestRenewExtendsAndPersistsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		backend := newBackend(t, dir)
		start := time.Now()
		lease := acquire(t, backend, "repo", "one", time.Second)
		if got := readState(t, dir).ExpiresAt; !got.Equal(start.Add(time.Second)) {
			t.Fatalf("persisted expires_at = %v, want %v", got, start.Add(time.Second))
		}

		time.Sleep(500 * time.Millisecond)
		info, err := lease.Renew(context.Background(), time.Second)
		if err != nil {
			t.Fatal(err)
		}
		want := start.Add(1500 * time.Millisecond)
		if !info.ExpiresAt.Equal(want) || !lease.Info().ExpiresAt.Equal(want) {
			t.Fatalf("renewed ExpiresAt = %v, want %v", info.ExpiresAt, want)
		}
		if got := readState(t, dir).ExpiresAt; !got.Equal(want) {
			t.Fatalf("persisted expires_at = %v, want %v", got, want)
		}

		time.Sleep(999 * time.Millisecond)
		synctest.Wait()
		if isClosed(lease.Done()) {
			t.Fatal("renewed lease expired early")
		}
		time.Sleep(time.Millisecond)
		synctest.Wait()
		if !isClosed(lease.Done()) {
			t.Fatal("renewed lease did not expire at its new deadline")
		}
	})
}

// FR-COORD-14, FR-COORD-03
func TestWaitingAcquisitionWakesOnLocalRelease(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		backend := newBackend(t, t.TempDir(), file.WithPollInterval(time.Hour))
		holder := acquire(t, backend, "repo", "one", time.Second)

		result := make(chan coordination.Lease, 1)
		go func() {
			lease, err := backend.Acquire(context.Background(), coordination.AcquireRequest{
				Key: "repo", Owner: "two", TTL: time.Minute, Wait: true,
			})
			if err != nil {
				t.Error(err)
				close(result)
				return
			}
			result <- lease
		}()

		start := time.Now()
		synctest.Wait()
		if len(result) != 0 {
			t.Fatal("waiter acquired a held lease")
		}

		// Expiry of an unrenewed lease wakes a waiter on the same backend
		// without waiting for the next poll.
		time.Sleep(time.Second)
		synctest.Wait()
		lease, ok := <-result
		if !ok {
			return
		}
		defer func() { _ = lease.Release(context.Background()) }()
		if elapsed := time.Since(start); elapsed != time.Second {
			t.Fatalf("waiter acquired after %v, want 1s", elapsed)
		}
		if lease.Info().Token <= holder.Info().Token {
			t.Fatalf("tokens = %d then %d, want increasing", holder.Info().Token, lease.Info().Token)
		}
	})
}

// FR-COORD-14
func TestWaitingAcquisitionPollsForOtherHolders(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		// Separate backends model separate processes: no in-process wakeup.
		holderBackend := newBackend(t, dir)
		waiterBackend := newBackend(t, dir, file.WithPollInterval(100*time.Millisecond))
		holder := acquire(t, holderBackend, "repo", "one", time.Minute)

		result := make(chan time.Time, 1)
		go func() {
			lease, err := waiterBackend.Acquire(context.Background(), coordination.AcquireRequest{
				Key: "repo", Owner: "two", TTL: time.Minute, Wait: true,
			})
			if err != nil {
				t.Error(err)
				close(result)
				return
			}
			result <- time.Now()
			_ = lease.Release(context.Background())
		}()

		time.Sleep(10 * time.Second)
		synctest.Wait()
		if len(result) != 0 {
			t.Fatal("waiter acquired a held lease")
		}
		released := time.Now()
		if err := holder.Release(context.Background()); err != nil {
			t.Fatal(err)
		}
		acquiredAt, ok := <-result
		if !ok {
			return
		}
		// Backoff is capped at the poll interval plus 25% jitter.
		if wait := acquiredAt.Sub(released); wait <= 0 || wait > 125*time.Millisecond {
			t.Fatalf("waiter acquired %v after release, want within one capped poll interval", wait)
		}
	})
}

// FR-COORD-03
func TestWaitingAcquisitionHonorsDeadline(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		dir := t.TempDir()
		holder := acquire(t, newBackend(t, dir), "repo", "one", time.Hour)
		defer func() { _ = holder.Release(context.Background()) }()

		ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		start := time.Now()
		_, err := newBackend(t, dir).Acquire(ctx, coordination.AcquireRequest{
			Key: "repo", Owner: "two", TTL: time.Minute, Wait: true,
		})
		if !errors.Is(err, context.DeadlineExceeded) {
			t.Fatalf("Acquire() error = %v, want context.DeadlineExceeded", err)
		}
		if elapsed := time.Since(start); elapsed != 3*time.Second {
			t.Fatalf("Acquire() returned after %v, want 3s", elapsed)
		}
	})
}
