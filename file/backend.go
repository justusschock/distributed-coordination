// Package file implements coordination.Backend with cross-process leases on a
// local filesystem directory.
//
// Each key maps to a lock file guarded by an operating-system advisory lock
// (flock on Unix, LockFileEx on Windows) and a state file that persists the
// current owner, fencing token, and deadline. The holding process enforces
// lease expiry, and process death releases the operating-system lock.
package file

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"math/rand/v2"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
)

const (
	defaultPollInterval = 100 * time.Millisecond
	defaultFileMode     = fs.FileMode(0o644)
	maxNamePrefix       = 48
)

// Backend grants leases backed by lock files in one directory.
type Backend struct {
	dir          string
	clock        coordination.Clock
	pollInterval time.Duration
	fileMode     fs.FileMode

	mu       sync.Mutex
	released chan struct{}
}

// Option configures a Backend.
type Option func(*config)

type config struct {
	clock        coordination.Clock
	pollInterval time.Duration
	fileMode     fs.FileMode
}

// WithClock sets the clock used for lease timestamps and expiry. A nil clock
// selects the real clock.
func WithClock(clock coordination.Clock) Option {
	return func(c *config) {
		c.clock = clock
	}
}

// WithPollInterval sets the maximum real-time delay between attempts of a
// waiting acquisition. Attempts start at a fraction of the interval and back
// off exponentially up to it.
func WithPollInterval(interval time.Duration) Option {
	return func(c *config) {
		c.pollInterval = interval
	}
}

// WithFileMode sets the permission bits for created lock and state files. The
// directory, when created, receives the same bits plus matching execute bits.
func WithFileMode(mode fs.FileMode) Option {
	return func(c *config) {
		c.fileMode = mode
	}
}

type realClock struct{}

func (realClock) Now() time.Time {
	return time.Now()
}

func (realClock) After(delay time.Duration) <-chan time.Time {
	return time.After(delay)
}

// NewBackend returns a backend that stores lock files in dir, creating it if
// necessary.
func NewBackend(dir string, opts ...Option) (*Backend, error) {
	if err := supported(); err != nil {
		return nil, err
	}
	cfg := config{pollInterval: defaultPollInterval, fileMode: defaultFileMode}
	for _, opt := range opts {
		if opt != nil {
			opt(&cfg)
		}
	}
	if dir == "" || cfg.pollInterval <= 0 || cfg.fileMode&^fs.ModePerm != 0 || cfg.fileMode&0o600 != 0o600 {
		return nil, coordination.ErrInvalidRequest
	}
	if cfg.clock == nil {
		cfg.clock = realClock{}
	}
	abs, err := filepath.Abs(dir)
	if err != nil {
		return nil, err
	}
	if err := os.MkdirAll(abs, cfg.fileMode|(cfg.fileMode&0o444)>>2); err != nil {
		return nil, err
	}
	return &Backend{
		dir:          abs,
		clock:        cfg.clock,
		pollInterval: cfg.pollInterval,
		fileMode:     cfg.fileMode,
		released:     make(chan struct{}),
	}, nil
}

// Acquire grants the lease for request.Key when no other lease handle, in this
// or another process, holds it.
func (b *Backend) Acquire(ctx context.Context, request coordination.AcquireRequest) (coordination.Lease, error) {
	if request.Key == "" || request.Owner == "" || request.TTL <= 0 {
		return nil, coordination.ErrInvalidRequest
	}

	paths := b.pathsFor(request.Key)
	delay := b.pollInterval / 16
	if delay < time.Millisecond {
		delay = min(time.Millisecond, b.pollInterval)
	}
	for {
		if err := ctx.Err(); err != nil {
			return nil, err
		}

		b.mu.Lock()
		released := b.released
		b.mu.Unlock()

		l, err := b.tryAcquire(paths, request)
		if err == nil {
			return l, nil
		}
		if !errors.Is(err, coordination.ErrNotAcquired) || !request.Wait {
			return nil, err
		}

		timer := time.NewTimer(jitter(delay))
		select {
		case <-ctx.Done():
			timer.Stop()
			return nil, ctx.Err()
		case <-released:
			timer.Stop()
		case <-timer.C:
			delay = min(2*delay, b.pollInterval)
		}
	}
}

func jitter(delay time.Duration) time.Duration {
	quarter := int64(delay / 4)
	if quarter <= 0 {
		return delay
	}
	return delay - time.Duration(quarter) + time.Duration(rand.Int64N(2*quarter))
}

type paths struct {
	lock, state, temp string
}

func (b *Backend) pathsFor(key string) paths {
	base := filepath.Join(b.dir, encodeKey(key))
	return paths{lock: base + ".lock", state: base + ".state", temp: base + ".state.tmp"}
}

// encodeKey maps a key to a portable file name: a lowercase readable prefix for
// humans and the SHA-256 of the exact key for identity. The name is safe on
// case-insensitive filesystems and avoids Windows reserved device names.
func encodeKey(key string) string {
	var prefix strings.Builder
	for _, r := range strings.ToLower(key) {
		if prefix.Len() >= maxNamePrefix {
			break
		}
		switch {
		case r >= 'a' && r <= 'z', r >= '0' && r <= '9', r == '-', r == '_':
			prefix.WriteRune(r)
		default:
			prefix.WriteByte('_')
		}
	}
	sum := sha256.Sum256([]byte(key))
	return prefix.String() + "-" + hex.EncodeToString(sum[:])
}

type record struct {
	Key        string                    `json:"key"`
	Owner      string                    `json:"owner"`
	Token      coordination.FencingToken `json:"token"`
	AcquiredAt time.Time                 `json:"acquired_at"`
	ExpiresAt  time.Time                 `json:"expires_at"`
}

func (b *Backend) tryAcquire(p paths, request coordination.AcquireRequest) (*lease, error) {
	f, err := os.OpenFile(p.lock, os.O_RDWR|os.O_CREATE, b.fileMode)
	if err != nil {
		return nil, err
	}
	locked, err := tryLock(f)
	if err != nil || !locked {
		_ = f.Close()
		if err != nil {
			return nil, err
		}
		return nil, coordination.ErrNotAcquired
	}
	fail := func(err error) (*lease, error) {
		_ = unlock(f)
		_ = f.Close()
		return nil, err
	}

	// The lock file may have been removed or replaced between open and lock;
	// holding a lock on an unlinked file would not exclude anyone.
	same, err := sameFile(f, p.lock)
	if err != nil {
		return fail(err)
	}
	if !same {
		return fail(coordination.ErrNotAcquired)
	}

	previous, err := readRecord(p.state, request.Key)
	if err != nil {
		return fail(err)
	}
	now := b.clock.Now()
	info := coordination.LeaseInfo{
		Key:        request.Key,
		Owner:      request.Owner,
		Token:      previous.Token + 1,
		AcquiredAt: now,
		ExpiresAt:  now.Add(request.TTL),
	}
	if info.Token == 0 {
		return fail(fmt.Errorf("file: fencing token overflow for key %q", request.Key))
	}
	if err := b.writeRecord(p, info); err != nil {
		return fail(err)
	}

	l := &lease{backend: b, paths: p, file: f, info: info, done: make(chan struct{})}
	l.scheduleExpiry(request.TTL)
	return l, nil
}

func readRecord(path, key string) (record, error) {
	data, err := os.ReadFile(path)
	if errors.Is(err, fs.ErrNotExist) {
		return record{}, nil
	}
	if err != nil {
		return record{}, err
	}
	var r record
	if err := json.Unmarshal(data, &r); err != nil {
		return record{}, fmt.Errorf("file: corrupt lease state %s: %w", path, err)
	}
	if r.Key != key {
		return record{}, fmt.Errorf("file: lease state %s belongs to key %q, not %q", path, r.Key, key)
	}
	return r, nil
}

// writeRecord atomically replaces the state file. Callers must hold the lock.
func (b *Backend) writeRecord(p paths, info coordination.LeaseInfo) error {
	data, err := json.Marshal(record{
		Key:        info.Key,
		Owner:      info.Owner,
		Token:      info.Token,
		AcquiredAt: info.AcquiredAt,
		ExpiresAt:  info.ExpiresAt,
	})
	if err != nil {
		return err
	}
	f, err := os.OpenFile(p.temp, os.O_WRONLY|os.O_CREATE|os.O_TRUNC, b.fileMode)
	if err != nil {
		return err
	}
	if _, err := f.Write(data); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Sync(); err != nil {
		_ = f.Close()
		return err
	}
	if err := f.Close(); err != nil {
		return err
	}
	if err := os.Rename(p.temp, p.state); err != nil {
		return err
	}
	return syncDir(b.dir)
}

func (b *Backend) notifyReleased() {
	b.mu.Lock()
	defer b.mu.Unlock()
	close(b.released)
	b.released = make(chan struct{})
}

type lease struct {
	backend *Backend
	paths   paths
	done    chan struct{}

	mu   sync.Mutex
	file *os.File
	info coordination.LeaseInfo
	lost bool
}

func sameFile(f *os.File, path string) (bool, error) {
	held, err := f.Stat()
	if err != nil {
		return false, err
	}
	current, err := os.Stat(path)
	if errors.Is(err, fs.ErrNotExist) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	return os.SameFile(held, current), nil
}

func (l *lease) scheduleExpiry(delay time.Duration) {
	expiry := l.backend.clock.After(delay)
	go func() {
		select {
		case <-l.done:
		case <-expiry:
			l.expire()
		}
	}()
}

func (l *lease) expire() {
	l.mu.Lock()
	defer l.mu.Unlock()
	if l.lost || l.info.ExpiresAt.After(l.backend.clock.Now()) {
		return
	}
	l.loseLocked()
}

// loseLocked releases the operating-system lock and closes Done exactly once.
func (l *lease) loseLocked() {
	if l.lost {
		return
	}
	l.lost = true
	_ = unlock(l.file)
	_ = l.file.Close()
	l.file = nil
	close(l.done)
	l.backend.notifyReleased()
}

// checkLocked reports whether the lease is still current, losing it otherwise.
func (l *lease) checkLocked() error {
	if l.lost {
		return coordination.ErrLeaseLost
	}
	if !l.info.ExpiresAt.After(l.backend.clock.Now()) {
		l.loseLocked()
		return coordination.ErrLeaseLost
	}
	same, err := sameFile(l.file, l.paths.lock)
	if err != nil {
		return err
	}
	if !same {
		l.loseLocked()
		return coordination.ErrLeaseLost
	}
	return nil
}

func (l *lease) Info() coordination.LeaseInfo {
	l.mu.Lock()
	defer l.mu.Unlock()
	return l.info
}

func (l *lease) Done() <-chan struct{} {
	return l.done
}

func (l *lease) Renew(ctx context.Context, ttl time.Duration) (coordination.LeaseInfo, error) {
	if ttl <= 0 {
		return coordination.LeaseInfo{}, coordination.ErrInvalidRequest
	}
	if err := ctx.Err(); err != nil {
		return coordination.LeaseInfo{}, err
	}

	l.mu.Lock()
	if err := l.checkLocked(); err != nil {
		l.mu.Unlock()
		return coordination.LeaseInfo{}, err
	}
	info := l.info
	info.ExpiresAt = l.backend.clock.Now().Add(ttl)
	if err := l.backend.writeRecord(l.paths, info); err != nil {
		l.mu.Unlock()
		return coordination.LeaseInfo{}, err
	}
	l.info = info
	l.mu.Unlock()

	l.scheduleExpiry(ttl)
	return info, nil
}

func (l *lease) Release(ctx context.Context) error {
	if err := ctx.Err(); err != nil {
		return err
	}

	l.mu.Lock()
	defer l.mu.Unlock()
	if err := l.checkLocked(); err != nil {
		return err
	}
	l.loseLocked()
	return nil
}
