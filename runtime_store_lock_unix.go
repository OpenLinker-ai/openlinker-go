//go:build unix

package openlinker

import (
	"errors"
	"os"
	"path/filepath"
	"syscall"
)

type dataDirLock struct {
	file *os.File
}

func openDataDirLock(dataDir string) (*dataDirLock, error) {
	file, err := os.OpenFile(filepath.Join(dataDir, ".runtime.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := file.Chmod(0o600); err != nil {
		_ = file.Close()
		return nil, err
	}
	if err := syscall.Flock(int(file.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = file.Close()
		if errors.Is(err, syscall.EWOULDBLOCK) || errors.Is(err, syscall.EAGAIN) {
			return nil, ErrDataDirLocked
		}
		return nil, err
	}
	return &dataDirLock{file: file}, nil
}

func (l *dataDirLock) close() error {
	if l == nil || l.file == nil {
		return nil
	}
	err := syscall.Flock(int(l.file.Fd()), syscall.LOCK_UN)
	closeErr := l.file.Close()
	l.file = nil
	if err != nil {
		return err
	}
	return closeErr
}
