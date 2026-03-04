//go:build linux

package sqlflow

import (
	"fmt"
	"os"

	"golang.org/x/sys/unix"
)

func init() {
	acquireLockFn = acquireSharedLock
}

// acquireSharedLock opens lockPath and acquires a shared flock, blocking until
// it succeeds. The returned file must be closed to release the lock.
func acquireSharedLock(lockPath string) (*os.File, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, fmt.Errorf("opening lock file %q: %w", lockPath, err)
	}
	if err := unix.Flock(int(f.Fd()), unix.LOCK_SH); err != nil {
		f.Close()
		return nil, fmt.Errorf("acquiring shared lock on %q: %w", lockPath, err)
	}
	return f, nil
}

// TryExclusiveLock opens lockPath and tries to acquire an exclusive flock
// without blocking. Returns (file, true, nil) on success, (nil, false, nil)
// if the lock is held by another process, or (nil, false, err) on error.
// The returned file must be closed to release the lock.
func TryExclusiveLock(lockPath string) (*os.File, bool, error) {
	f, err := os.OpenFile(lockPath, os.O_CREATE|os.O_RDWR, 0600)
	if err != nil {
		return nil, false, fmt.Errorf("opening lock file %q: %w", lockPath, err)
	}
	err = unix.Flock(int(f.Fd()), unix.LOCK_EX|unix.LOCK_NB)
	if err == unix.EWOULDBLOCK {
		f.Close()
		return nil, false, nil
	}
	if err != nil {
		f.Close()
		return nil, false, fmt.Errorf("acquiring exclusive lock on %q: %w", lockPath, err)
	}
	return f, true, nil
}
