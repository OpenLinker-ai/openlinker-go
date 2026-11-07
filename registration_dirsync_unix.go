//go:build !windows

package openlinker

import "os"

// syncRegistrationDirStrict syncs dir and reports a genuine failure; a
// directory that cannot be synced is skipped.
func syncRegistrationDirStrict(dir string) error {
	directory, err := os.Open(dir)
	if err != nil {
		return ignoreUnsupportedDirSync(err)
	}
	defer directory.Close()
	return ignoreUnsupportedDirSync(directory.Sync())
}
