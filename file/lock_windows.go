//go:build windows

package file

import (
	"errors"
	"os"

	"golang.org/x/sys/windows"
)

func supported() error {
	return nil
}

// tryLock locks the first byte of the lock file. The lock file carries no
// data, so the mandatory byte-range semantics of LockFileEx never block I/O.
func tryLock(f *os.File) (bool, error) {
	var overlapped windows.Overlapped
	err := windows.LockFileEx(windows.Handle(f.Fd()), windows.LOCKFILE_EXCLUSIVE_LOCK|windows.LOCKFILE_FAIL_IMMEDIATELY, 0, 1, 0, &overlapped)
	switch {
	case err == nil:
		return true, nil
	case errors.Is(err, windows.ERROR_LOCK_VIOLATION), errors.Is(err, windows.ERROR_IO_PENDING):
		return false, nil
	default:
		return false, &os.PathError{Op: "LockFileEx", Path: f.Name(), Err: err}
	}
}

func unlock(f *os.File) error {
	var overlapped windows.Overlapped
	return windows.UnlockFileEx(windows.Handle(f.Fd()), 0, 1, 0, &overlapped)
}

// syncDir is a no-op: Windows cannot flush directory handles opened by os.Open,
// and NTFS journals the rename metadata.
func syncDir(string) error {
	return nil
}
