//go:build windows

package openlinker

// syncRegistrationDirStrict is skipped on Windows, which cannot sync a
// directory handle; completed saves have always ignored that failure there.
func syncRegistrationDirStrict(string) error { return nil }
