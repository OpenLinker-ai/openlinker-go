//go:build unix

package openlinker

import (
	"io/fs"
	"os"
	"syscall"
	"testing"
	"time"
)

type registrationDirInfo struct{ sys any }

func (registrationDirInfo) Name() string       { return "dir" }
func (registrationDirInfo) Size() int64        { return 0 }
func (registrationDirInfo) Mode() fs.FileMode  { return fs.ModeDir | 0o700 }
func (registrationDirInfo) ModTime() time.Time { return time.Time{} }
func (registrationDirInfo) IsDir() bool        { return true }
func (i registrationDirInfo) Sys() any         { return i.sys }

func TestRegistrationDirChainStopsAtAnotherFileSystem(t *testing.T) {
	t.Parallel()
	first, second := registrationDirInfo{&syscall.Stat_t{Dev: 1}}, registrationDirInfo{&syscall.Stat_t{Dev: 2}}
	if !sameRegistrationFileSystem(first, registrationDirInfo{&syscall.Stat_t{Dev: 1}}) || sameRegistrationFileSystem(first, second) {
		t.Fatal("device comparison is wrong")
	}
	// Without device information the parent is synced.
	if !sameRegistrationFileSystem(registrationDirInfo{}, second) {
		t.Fatal("unknown device stopped the chain")
	}

	// /dev is the root of its own file system on Linux and macOS: it always
	// existed, so "/" is not synced for it.
	dev, devErr := os.Lstat("/dev")
	root, rootErr := os.Stat("/")
	if devErr != nil || rootErr != nil || dev.Mode().Type() != fs.ModeDir || sameRegistrationFileSystem(dev, root) {
		t.Skip("/dev is not a separately mounted directory here")
	}
	if parent, ok, err := creatableRegistrationDirParent("/dev"); ok || err != nil {
		t.Fatalf("creatableRegistrationDirParent(/dev) = %q, %t, %v", parent, ok, err)
	}
}
