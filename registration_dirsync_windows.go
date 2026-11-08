//go:build windows

package openlinker

import "path/filepath"

// syncRegistrationDirStrict is skipped on Windows, which cannot sync a
// directory handle; completed saves have always ignored that failure there.
func syncRegistrationDirStrict(string) error { return nil }

// registrationDirIdentity is dir's cleaned spelling on Windows, which resolves
// ".." lexically, and makes no file system call.
func registrationDirIdentity(dir string) (any, error) { return filepath.Clean(dir), nil }
