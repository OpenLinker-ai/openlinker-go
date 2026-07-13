//go:build unix

package openlinker

import "os"

func runtimeFileModeIsPrivate(mode os.FileMode) bool {
	return mode.Perm()&0o077 == 0
}
