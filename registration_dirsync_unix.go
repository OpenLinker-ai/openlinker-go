//go:build !windows

package openlinker

import (
	"os"
	"path/filepath"
	"syscall"
)

// syncRegistrationDirStrict syncs dir and reports a genuine failure; a
// directory that cannot be synced is skipped. A failure to open dir is marked
// as a *registrationDirOpenError, which syncPendingRegistrationDir and
// syncRecordPath remove before any caller sees the error.
func syncRegistrationDirStrict(dir string) error {
	directory, err := os.Open(dir)
	if err != nil {
		return ignoreUnsupportedDirSync(&registrationDirOpenError{err: err})
	}
	defer directory.Close()
	return ignoreUnsupportedDirSync(directory.Sync())
}

// registrationDirIdentity identifies the directory dir resolves to by its
// device and inode, so one directory reached by several spellings is synced
// once.
func registrationDirIdentity(dir string) (any, error) {
	info, err := os.Stat(dir)
	if err != nil {
		return nil, err
	}
	if stat, ok := info.Sys().(*syscall.Stat_t); ok {
		return [2]uint64{uint64(stat.Dev), uint64(stat.Ino)}, nil
	}
	return filepath.Clean(dir), nil
}
