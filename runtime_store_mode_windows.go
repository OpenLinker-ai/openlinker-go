//go:build windows

package openlinker

import "os"

// Windows does not expose POSIX permission bits through os.FileMode. Files are
// created with the process token's inherited DACL; os.Chmod still applies the
// available read-only protection but cannot be verified as mode 0600 here.
func runtimeFileModeIsPrivate(os.FileMode) bool { return true }
