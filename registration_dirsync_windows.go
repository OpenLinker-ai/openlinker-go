//go:build windows

package openlinker

import "path/filepath"

// syncRegistrationDirStrict is skipped on Windows, which cannot sync a
// directory handle; completed saves have always ignored that failure there.
func syncRegistrationDirStrict(string) error { return nil }

// registrationEntryCreatable makes no file system call on Windows: its
// directory syncs are no-ops, so the path is walked by its spelling only and
// cannot fail a registration there.
func registrationEntryCreatable(string, string) (bool, error) { return true, nil }

// registrationDirIdentity is dir's cleaned spelling on Windows, which resolves
// ".." lexically, and makes no file system call.
func registrationDirIdentity(dir string) (any, error) { return filepath.Clean(dir), nil }
