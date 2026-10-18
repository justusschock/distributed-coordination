package file_test

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strconv"
	"strings"
	"testing"
	"time"

	coordination "github.com/justusschock/go-distributed-coordination"
	"github.com/justusschock/go-distributed-coordination/file"
)

// Cross-process tests re-execute the test binary as a helper process. They use
// real time because a synctest bubble cannot span processes.
const (
	helperModeEnv = "FILE_BACKEND_HELPER_MODE"
	helperDirEnv  = "FILE_BACKEND_HELPER_DIR"
	helperTTLEnv  = "FILE_BACKEND_HELPER_TTL"
	helperIterEnv = "FILE_BACKEND_HELPER_ITERATIONS"
	helperKey     = "repo/lock"
	helperTimeout = 20 * time.Second
)

func TestMain(m *testing.M) {
	if mode := os.Getenv(helperModeEnv); mode != "" {
		if err := runHelper(mode); err != nil {
			fmt.Fprintln(os.Stderr, "helper:", err)
			os.Exit(2)
		}
		os.Exit(0)
	}
	os.Exit(m.Run())
}

// runHelper holds or contends for helperKey in a separate process. It reports
// progress as lines on stdout and waits for stdin to close where noted.
func runHelper(mode string) error {
	backend, err := file.NewBackend(os.Getenv(helperDirEnv))
	if err != nil {
		return err
	}
	ttl, err := time.ParseDuration(os.Getenv(helperTTLEnv))
	if err != nil {
		return err
	}
	ctx, cancel := context.WithTimeout(context.Background(), helperTimeout)
	defer cancel()
	request := coordination.AcquireRequest{Key: helperKey, Owner: fmt.Sprintf("pid-%d", os.Getpid()), TTL: ttl, Wait: true}

	switch mode {
	case "hold":
		// Acquire, report the token, and release once stdin closes.
		lease, err := backend.Acquire(ctx, request)
		if err != nil {
			return err
		}
		fmt.Printf("token %d\n", lease.Info().Token)
		_, _ = io.Copy(io.Discard, os.Stdin)
		if err := lease.Release(context.Background()); err != nil {
			return err
		}
		fmt.Println("released")
		return nil

	case "expire":
		// Acquire, never renew, and observe the lapsed lease while alive.
		lease, err := backend.Acquire(ctx, request)
		if err != nil {
			return err
		}
		fmt.Printf("token %d\n", lease.Info().Token)
		select {
		case <-lease.Done():
		case <-ctx.Done():
			return ctx.Err()
		}
		_, renewErr := lease.Renew(context.Background(), ttl)
		releaseErr := lease.Release(context.Background())
		fmt.Printf("lost renew=%t release=%t\n", errors.Is(renewErr, coordination.ErrLeaseLost), errors.Is(releaseErr, coordination.ErrLeaseLost))
		_, _ = io.Copy(io.Discard, os.Stdin)
		return nil

	case "counter":
		// Perform a non-atomic read-modify-write of a shared counter under the
		// lease and append each token to a log in acquisition order.
		iterations, err := strconv.Atoi(os.Getenv(helperIterEnv))
		if err != nil {
			return err
		}
		dir := os.Getenv(helperDirEnv)
		counterPath := filepath.Join(dir, "counter")
		for range iterations {
			lease, err := backend.Acquire(ctx, request)
			if err != nil {
				return err
			}
			data, err := os.ReadFile(counterPath)
			if err != nil && !errors.Is(err, os.ErrNotExist) {
				return err
			}
			count, _ := strconv.Atoi(string(data))
			time.Sleep(time.Millisecond)
			if err := os.WriteFile(counterPath, []byte(strconv.Itoa(count+1)), 0o644); err != nil {
				return err
			}
			log, err := os.OpenFile(filepath.Join(dir, "tokens"), os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0o644)
			if err != nil {
				return err
			}
			_, writeErr := fmt.Fprintln(log, lease.Info().Token)
			closeErr := log.Close()
			if err := errors.Join(writeErr, closeErr); err != nil {
				return err
			}
			if err := lease.Release(context.Background()); err != nil {
				return err
			}
		}
		return nil
	}
	return fmt.Errorf("unknown mode %q", mode)
}

type helper struct {
	cmd   *exec.Cmd
	stdin io.WriteCloser
	lines chan string
}

func startHelper(t *testing.T, dir, mode string, ttl time.Duration, env ...string) *helper {
	t.Helper()
	cmd := exec.Command(os.Args[0], "-test.run=^$")
	cmd.Env = append(os.Environ(),
		helperModeEnv+"="+mode,
		helperDirEnv+"="+dir,
		helperTTLEnv+"="+ttl.String(),
	)
	cmd.Env = append(cmd.Env, env...)
	cmd.Stderr = os.Stderr
	stdin, err := cmd.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := cmd.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := cmd.Start(); err != nil {
		t.Fatal(err)
	}
	h := &helper{cmd: cmd, stdin: stdin, lines: make(chan string, 16)}
	go func() {
		defer close(h.lines)
		scanner := bufio.NewScanner(stdout)
		for scanner.Scan() {
			h.lines <- scanner.Text()
		}
	}()
	t.Cleanup(func() {
		_ = stdin.Close()
		if cmd.ProcessState == nil {
			_ = cmd.Process.Kill()
			_ = cmd.Wait()
		}
	})
	return h
}

func (h *helper) expect(t *testing.T, prefix string) string {
	t.Helper()
	select {
	case line, ok := <-h.lines:
		if !ok {
			t.Fatalf("helper exited before reporting %q", prefix)
		}
		if !strings.HasPrefix(line, prefix) {
			t.Fatalf("helper reported %q, want prefix %q", line, prefix)
		}
		return strings.TrimSpace(strings.TrimPrefix(line, prefix))
	case <-time.After(helperTimeout):
		t.Fatalf("helper did not report %q", prefix)
	}
	return ""
}

func (h *helper) token(t *testing.T) coordination.FencingToken {
	t.Helper()
	token, err := strconv.ParseUint(h.expect(t, "token "), 10, 64)
	if err != nil {
		t.Fatal(err)
	}
	return coordination.FencingToken(token)
}

func (h *helper) wait(t *testing.T) {
	t.Helper()
	if err := h.cmd.Wait(); err != nil {
		t.Fatalf("helper failed: %v", err)
	}
}

func acquireWithin(t *testing.T, backend coordination.Backend, owner string) coordination.Lease {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), helperTimeout)
	defer cancel()
	lease, err := backend.Acquire(ctx, coordination.AcquireRequest{Key: helperKey, Owner: owner, TTL: time.Minute, Wait: true})
	if err != nil {
		t.Fatal(err)
	}
	return lease
}

// FR-COORD-12
func TestCrossProcessExclusion(t *testing.T) {
	dir := t.TempDir()
	child := startHelper(t, dir, "hold", time.Minute)
	childToken := child.token(t)

	backend := newBackend(t, dir, file.WithPollInterval(20*time.Millisecond))
	_, err := backend.Acquire(context.Background(), coordination.AcquireRequest{Key: helperKey, Owner: "parent", TTL: time.Minute})
	if !errors.Is(err, coordination.ErrNotAcquired) {
		t.Fatalf("contended Acquire() error = %v, want ErrNotAcquired", err)
	}

	result := make(chan coordination.Lease, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), helperTimeout)
		defer cancel()
		lease, err := backend.Acquire(ctx, coordination.AcquireRequest{Key: helperKey, Owner: "parent", TTL: time.Minute, Wait: true})
		if err != nil {
			t.Error(err)
			close(result)
			return
		}
		result <- lease
	}()
	select {
	case <-result:
		t.Fatal("waiting Acquire succeeded while another process held the lease")
	case <-time.After(200 * time.Millisecond):
	}

	if err := child.stdin.Close(); err != nil {
		t.Fatal(err)
	}
	child.expect(t, "released")
	child.wait(t)

	select {
	case lease, ok := <-result:
		if !ok {
			return
		}
		defer func() { _ = lease.Release(context.Background()) }()
		if lease.Info().Token <= childToken {
			t.Fatalf("tokens = %d then %d, want increasing", childToken, lease.Info().Token)
		}
	case <-time.After(helperTimeout):
		t.Fatal("waiting Acquire did not succeed after the other process released")
	}
}

// FR-COORD-12
func TestCrossProcessCrashReleasesLock(t *testing.T) {
	dir := t.TempDir()
	child := startHelper(t, dir, "hold", time.Hour)
	childToken := child.token(t)

	if err := child.cmd.Process.Kill(); err != nil {
		t.Fatal(err)
	}
	_ = child.cmd.Wait()

	lease := acquireWithin(t, newBackend(t, dir, file.WithPollInterval(20*time.Millisecond)), "parent")
	defer func() { _ = lease.Release(context.Background()) }()
	if lease.Info().Token <= childToken {
		t.Fatalf("tokens = %d then %d, want increasing", childToken, lease.Info().Token)
	}
}

// FR-COORD-14
func TestCrossProcessExpiredHolderLosesLease(t *testing.T) {
	dir := t.TempDir()
	child := startHelper(t, dir, "expire", 300*time.Millisecond)
	childToken := child.token(t)

	// The child stays alive but never renews, so the lease lapses.
	lease := acquireWithin(t, newBackend(t, dir, file.WithPollInterval(20*time.Millisecond)), "parent")
	defer func() { _ = lease.Release(context.Background()) }()
	if lease.Info().Token <= childToken {
		t.Fatalf("tokens = %d then %d, want increasing", childToken, lease.Info().Token)
	}
	if got := child.expect(t, "lost "); got != "renew=true release=true" {
		t.Fatalf("helper lost-lease report = %q, want renew=true release=true", got)
	}
	if err := child.stdin.Close(); err != nil {
		t.Fatal(err)
	}
	child.wait(t)
}

// FR-COORD-12, FR-COORD-13
func TestCrossProcessMutualExclusion(t *testing.T) {
	const processes, iterations = 4, 15
	dir := t.TempDir()
	children := make([]*helper, processes)
	for i := range children {
		children[i] = startHelper(t, dir, "counter", time.Minute, helperIterEnv+"="+strconv.Itoa(iterations))
		_ = children[i].stdin.Close()
	}
	for _, child := range children {
		child.wait(t)
	}

	data, err := os.ReadFile(filepath.Join(dir, "counter"))
	if err != nil {
		t.Fatal(err)
	}
	if got := strings.TrimSpace(string(data)); got != strconv.Itoa(processes*iterations) {
		t.Fatalf("counter = %s, want %d (lost updates imply overlapping holders)", got, processes*iterations)
	}
	log, err := os.ReadFile(filepath.Join(dir, "tokens"))
	if err != nil {
		t.Fatal(err)
	}
	tokens := strings.Fields(string(log))
	if len(tokens) != processes*iterations {
		t.Fatalf("logged %d tokens, want %d", len(tokens), processes*iterations)
	}
	for i, field := range tokens {
		token, err := strconv.Atoi(field)
		if err != nil {
			t.Fatal(err)
		}
		if token != i+1 {
			t.Fatalf("token %d in acquisition order = %d, want %d", i, token, i+1)
		}
	}
}
