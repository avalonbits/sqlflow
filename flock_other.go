//go:build !linux

package sqlflow

import "os"

// TryExclusiveLock is not supported on this platform. Always returns false
// (treats every user as online), so backup is effectively a no-op.
func TryExclusiveLock(_ string) (*os.File, bool, error) {
	return nil, false, nil
}
