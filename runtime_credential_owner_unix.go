//go:build unix

package openlinker

import (
	"errors"
	"os"
	"syscall"
)

func verifyRuntimeCredentialOwner(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	stat, ok := info.Sys().(*syscall.Stat_t)
	if !ok || info.Mode()&os.ModeSymlink != 0 || stat.Uid != uint32(os.Geteuid()) {
		return errors.New("credential preparation must run as the existing Runtime data owner")
	}
	return nil
}
