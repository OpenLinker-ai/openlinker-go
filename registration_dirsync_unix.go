//go:build !windows

package openlinker

import (
	"io/fs"
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

// registrationEntryCreatable reports whether MkdirAll could have created
// entry, which exists, inside container, the directory that holds it: entry
// must be a plain directory, not a symlink or other entry, on container's file
// system rather than the root of a mount. Otherwise entry always existed, and
// nothing before it on the path was created on the way to the record.
func registrationEntryCreatable(entry, container string) (bool, error) {
	info, err := os.Lstat(entry)
	if err != nil {
		return false, err
	}
	if info.Mode().Type() != fs.ModeDir {
		return false, nil
	}
	containerInfo, err := os.Stat(container)
	if err != nil {
		return false, err
	}
	return sameRegistrationFileSystem(info, containerInfo), nil
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

// sameRegistrationFileSystem reports whether two entries are on the same
// device; when that is unknown it answers true, so the parent is synced.
func sameRegistrationFileSystem(dir, parent fs.FileInfo) bool {
	dirStat, dirOK := dir.Sys().(*syscall.Stat_t)
	parentStat, parentOK := parent.Sys().(*syscall.Stat_t)
	return !dirOK || !parentOK || dirStat.Dev == parentStat.Dev
}
