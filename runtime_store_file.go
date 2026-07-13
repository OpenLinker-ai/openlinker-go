package openlinker

import (
	"bytes"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
)

type durableHook func(point, path string) error

const (
	durableBeforeFileWrite = "before_file_write"
	durableAfterFileSync   = "after_file_sync"
	durableAfterRename     = "after_rename"
	durableAfterDirSync    = "after_directory_sync"
	durableAfterWALSync    = "after_wal_sync"
)

func ensurePrivateDataDir(path string) error {
	if path == "" {
		return fmt.Errorf("runtime data directory is required")
	}
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if err := os.MkdirAll(path, 0o700); err != nil {
			return fmt.Errorf("create runtime data directory: %w", err)
		}
		info, err = os.Lstat(path)
	}
	if err != nil {
		return fmt.Errorf("inspect runtime data directory: %w", err)
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return fmt.Errorf("runtime data directory must be a real directory")
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return fmt.Errorf("secure runtime data directory: %w", err)
	}
	return nil
}

func atomicWriteDurable(path string, value []byte, mode os.FileMode, hook durableHook) (retErr error) {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, ".openlinker-tmp-")
	if err != nil {
		return err
	}
	tmpPath := tmp.Name()
	committed := false
	defer func() {
		_ = tmp.Close()
		if !committed {
			_ = os.Remove(tmpPath)
		}
	}()
	if err := tmp.Chmod(mode); err != nil {
		return err
	}
	if hook != nil {
		if err := hook(durableBeforeFileWrite, path); err != nil {
			return err
		}
	}
	if err := writeFull(tmp, value); err != nil {
		return err
	}
	if err := tmp.Sync(); err != nil {
		return err
	}
	if hook != nil {
		if err := hook(durableAfterFileSync, path); err != nil {
			return err
		}
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := atomicReplaceFile(tmpPath, path); err != nil {
		return err
	}
	committed = true
	if hook != nil {
		if err := hook(durableAfterRename, path); err != nil {
			return err
		}
	}
	if err := syncRuntimeDirectory(dir); err != nil {
		return err
	}
	if hook != nil {
		if err := hook(durableAfterDirSync, path); err != nil {
			return err
		}
	}
	return nil
}

func durableRemove(path string) error {
	err := os.Remove(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	return syncRuntimeDirectory(filepath.Dir(path))
}

func cleanupDurableTemps(dir string) error {
	entries, err := os.ReadDir(dir)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	removed := false
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasPrefix(entry.Name(), ".openlinker-tmp-") {
			continue
		}
		if err := os.Remove(filepath.Join(dir, entry.Name())); err != nil && !errors.Is(err, os.ErrNotExist) {
			return err
		}
		removed = true
	}
	if removed {
		return syncRuntimeDirectory(dir)
	}
	return nil
}

func decodeStrictJSON(raw []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		if err == nil {
			return fmt.Errorf("unexpected trailing JSON value")
		}
		return err
	}
	return nil
}

func writeFull(writer io.Writer, value []byte) error {
	for len(value) > 0 {
		n, err := writer.Write(value)
		if err != nil {
			return err
		}
		if n == 0 {
			return io.ErrShortWrite
		}
		value = value[n:]
	}
	return nil
}

func constantChecksumEqual(got, want string) bool {
	gotBytes, gotErr := hex.DecodeString(got)
	wantBytes, wantErr := hex.DecodeString(want)
	if gotErr != nil || wantErr != nil || len(gotBytes) != len(wantBytes) {
		return false
	}
	return subtle.ConstantTimeCompare(gotBytes, wantBytes) == 1
}

func validRuntimeID(value string) bool {
	return value != "" && len(value) <= 255 && !strings.ContainsRune(value, '\x00')
}
