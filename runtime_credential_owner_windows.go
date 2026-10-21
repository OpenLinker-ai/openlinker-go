//go:build windows

package openlinker

import (
	"errors"
	"golang.org/x/sys/windows"
	"os"
)

func verifyRuntimeCredentialOwner(path string) error {
	info, err := os.Lstat(path)
	if err != nil {
		return err
	}
	if info.Mode()&os.ModeSymlink != 0 {
		return errors.New("credential preparation does not accept symlinks")
	}
	security, err := windows.GetNamedSecurityInfo(path, windows.SE_FILE_OBJECT, windows.OWNER_SECURITY_INFORMATION)
	if err != nil {
		return err
	}
	owner, _, err := security.Owner()
	if err != nil {
		return err
	}
	user, err := windows.GetCurrentProcessToken().GetTokenUser()
	if err != nil {
		return err
	}
	if !owner.Equals(user.User.Sid) {
		return errors.New("credential preparation must run as the existing Runtime data owner")
	}
	return nil
}
