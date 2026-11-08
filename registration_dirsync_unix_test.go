//go:build unix

package openlinker

import (
	"context"
	"errors"
	"io/fs"
	"os"
	"path/filepath"
	"strings"
	"syscall"
	"testing"
	"time"
)

func TestPendingRecordWalkStopsAtAncestorItCannotOpen(t *testing.T) {
	t.Parallel()
	if os.Geteuid() == 0 {
		t.Skip("root can open any directory")
	}
	root := t.TempDir()
	locked := filepath.Join(root, "locked")
	ownDir := filepath.Join(root, "own")
	apps := []string{filepath.Join(locked, "walk"), filepath.Join(locked, "plain"), filepath.Join(locked, "sync-error")}
	for _, dir := range append([]string{ownDir}, apps...) {
		if err := os.MkdirAll(dir, 0o700); err != nil {
			t.Fatal(err)
		}
	}
	// Write and search, no read: the directory can be traversed and written
	// in but not opened for a sync.
	for _, dir := range []string{locked, ownDir} {
		if err := os.Chmod(dir, 0o311); err != nil {
			t.Fatal(err)
		}
		t.Cleanup(func() { _ = os.Chmod(dir, 0o700) })
	}
	if directory, err := os.Open(locked); err == nil {
		_ = directory.Close()
		t.Skip("a 0311 directory can be opened here")
	} else if !errors.Is(err, fs.ErrPermission) {
		t.Fatalf("open 0311 directory: %v", err)
	}

	// ensure runs one start against path; sync stands in for the directory
	// sync, and every directory passed to it is recorded.
	ensureWith := func(core *replayRegistrationCore, path string, sync func(string) error) (*AgentRegistration, []string, []string, error) {
		t.Helper()
		store := NewEnvRegistrationStore(path)
		var synced []string
		if sync != nil {
			pendingDirSyncTestHooks.Store(store, func(dir string) error {
				synced = append(synced, dir)
				return sync(dir)
			})
			defer pendingDirSyncTestHooks.Delete(store)
		}
		registration, err := EnsureAgent(context.Background(), AgentSpec{Slug: "demo-agent", Name: "Demo Agent"},
			WithRegistrationAPIBase(core.server.URL), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(store))
		_, tokens := core.calls()
		return registration, synced, tokens, err
	}
	ensure := func(path string, sync func(string) error) (*AgentRegistration, []string, []string, error) {
		t.Helper()
		return ensureWith(newReplayRegistrationCore(t), path, sync)
	}

	// An ancestor reached only by the walk that cannot be opened is skipped
	// and ends the walk: the start succeeds and root is never synced. The
	// last sync is the record cleanup's, of the record's own directory.
	registration, synced, tokens, err := ensure(filepath.Join(apps[0], ".env"), syncRegistrationDirStrict)
	if err != nil || registration.AgentToken != "ol_agent_minted_1" || len(tokens) != 1 ||
		strings.Join(synced, "|") != apps[0]+"|"+locked+"|"+apps[0] {
		t.Fatalf("walk: registration=%#v err=%v register=%v synced=%v", registration, err, tokens, synced)
	}
	// The same start without any test hook.
	if registration, _, _, err := ensure(filepath.Join(apps[1], ".env"), nil); err != nil || registration.AgentToken != "ol_agent_minted_1" {
		t.Fatalf("plain: registration=%#v err=%v", registration, err)
	}

	// A permission error from the sync itself, not the open, stays strict.
	failSync := func(dir string) error {
		if dir == locked {
			return syscall.EACCES
		}
		return syncRegistrationDirStrict(dir)
	}
	if _, _, tokens, err := ensure(filepath.Join(apps[2], ".env"), failSync); !errors.Is(err, syscall.EACCES) || len(tokens) != 0 {
		t.Fatalf("sync error: err=%v register=%v", err, tokens)
	}

	// A directory this start creates inside it still needs its parent synced:
	// the start fails before the record is written and nothing is redeemed.
	created := filepath.Join(locked, "created", ".env")
	if _, synced, tokens, err := ensure(created, syncRegistrationDirStrict); !strictOpenPermissionError(err) ||
		len(tokens) != 0 || strings.Join(synced, "|") != locked {
		t.Fatalf("created: err=%v register=%v synced=%v", err, tokens, synced)
	}
	if _, err := os.Stat(created); !os.IsNotExist(err) {
		t.Fatalf("record written under an unsynced new directory: %v", err)
	}

	// The record's own directory is always synced strictly: the first start
	// fails after writing the record, and so does its replay, here without
	// any test hook.
	ownCore := newReplayRegistrationCore(t)
	if _, synced, tokens, err := ensureWith(ownCore, filepath.Join(ownDir, ".env"), syncRegistrationDirStrict); !strictOpenPermissionError(err) ||
		len(tokens) != 0 || strings.Join(synced, "|") != ownDir {
		t.Fatalf("own directory: err=%v register=%v synced=%v", err, tokens, synced)
	}
	if _, _, tokens, err := ensureWith(ownCore, filepath.Join(ownDir, ".env"), nil); !strictOpenPermissionError(err) || len(tokens) != 0 {
		t.Fatalf("own directory replay: err=%v register=%v", err, tokens)
	}
	if created, _ := ownCore.calls(); created != 1 {
		t.Fatalf("own directory replay minted again: create-token=%d", created)
	}
}

// strictOpenPermissionError reports whether err is the file system's own
// error for a directory that could not be opened, exactly as EnsureAgent
// returned it before the ancestor walk: a *fs.PathError that os.IsPermission
// and errors.Is(err, fs.ErrPermission) both classify.
func strictOpenPermissionError(err error) bool {
	_, isPathError := err.(*fs.PathError)
	return isPathError && os.IsPermission(err) && errors.Is(err, fs.ErrPermission)
}

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
	if creatable, err := registrationEntryCreatable("/dev", "/"); creatable || err != nil {
		t.Fatalf("registrationEntryCreatable(/dev, /) = %t, %v", creatable, err)
	}
}
