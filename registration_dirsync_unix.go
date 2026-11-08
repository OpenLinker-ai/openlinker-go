//go:build !windows

package openlinker

import (
	"io/fs"
	"os"
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

// registrationDirCreatable reports whether MkdirAll could have created dir,
// which exists, inside parent: dir must be a plain directory, not a symlink or
// other entry, on parent's file system rather than the root of a mount.
// Otherwise dir always existed, and neither it nor anything above it was
// created on the way to the record.
func registrationDirCreatable(dir, parent string) (bool, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return false, err
	}
	if info.Mode().Type() != fs.ModeDir {
		return false, nil
	}
	parentInfo, err := os.Stat(parent)
	if err != nil {
		return false, err
	}
	return sameRegistrationFileSystem(info, parentInfo), nil
}

// registrationDotDotIsLexical reports whether "dir/.." names dir's lexical
// parent: dir is a plain directory, not a symlink whose ".." is the parent of
// its target.
func registrationDotDotIsLexical(dir string) (bool, error) {
	info, err := os.Lstat(dir)
	if err != nil {
		return false, err
	}
	return info.Mode().Type() == fs.ModeDir, nil
}

// sameRegistrationFileSystem reports whether two entries are on the same
// device; when that is unknown it answers true, so the parent is synced.
func sameRegistrationFileSystem(dir, parent fs.FileInfo) bool {
	dirStat, dirOK := dir.Sys().(*syscall.Stat_t)
	parentStat, parentOK := parent.Sys().(*syscall.Stat_t)
	return !dirOK || !parentOK || dirStat.Dev == parentStat.Dev
}
