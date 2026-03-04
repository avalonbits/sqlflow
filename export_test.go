//go:build linux

package sqlflow

import "os"

// AcquireSharedLock exposes acquireSharedLock for flock tests.
func AcquireSharedLock(path string) (*os.File, error) {
	return acquireSharedLock(path)
}

// WaitEviction calls p.Wait() to flush all pending ristretto OnExit callbacks.
// Use immediately after Pool.Evict in tests to synchronise lock-file closure.
func (p *Pool[Queries]) WaitEviction() {
	p.Wait()
}
