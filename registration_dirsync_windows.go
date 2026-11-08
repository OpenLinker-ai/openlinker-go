//go:build windows

package openlinker

// syncRegistrationDirStrict is skipped on Windows, which cannot sync a
// directory handle; completed saves have always ignored that failure there.
func syncRegistrationDirStrict(string) error { return nil }

// registrationDirCreatable makes no file system call on Windows: its
// directory syncs are no-ops, so the ancestor chain is walked lexically only
// and cannot fail a registration there.
func registrationDirCreatable(string, string) (bool, error) { return true, nil }

// registrationDotDotIsLexical is true on Windows, which resolves ".."
// lexically, and makes no file system call.
func registrationDotDotIsLexical(string) (bool, error) { return true, nil }
