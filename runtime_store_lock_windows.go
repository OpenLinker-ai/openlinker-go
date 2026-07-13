//go:build windows

package openlinker

import (
	"os"
	"path/filepath"
	"syscall"
	"unsafe"
)

const (
	lockFileExclusiveLock   = 0x00000002
	lockFileFailImmediately = 0x00000001
	errorSharingViolation   = syscall.Errno(32)
	errorLockViolation      = syscall.Errno(33)
)

var (
	kernel32FileLock = syscall.NewLazyDLL("kernel32.dll")
	procLockFileEx   = kernel32FileLock.NewProc("LockFileEx")
	procUnlockFileEx = kernel32FileLock.NewProc("UnlockFileEx")
)

type dataDirLock struct {
	file       *os.File
	overlapped syscall.Overlapped
}

func openDataDirLock(dataDir string) (*dataDirLock, error) {
	file, err := os.OpenFile(filepath.Join(dataDir, ".runtime.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	lock := &dataDirLock{file: file}
	r1, _, callErr := procLockFileEx.Call(
		file.Fd(),
		uintptr(lockFileExclusiveLock|lockFileFailImmediately),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&lock.overlapped)),
	)
	if r1 == 0 {
		_ = file.Close()
		if errno, ok := callErr.(syscall.Errno); ok && (errno == errorLockViolation || errno == errorSharingViolation) {
			return nil, ErrDataDirLocked
		}
		if callErr != syscall.Errno(0) {
			return nil, callErr
		}
		return nil, syscall.EINVAL
	}
	return lock, nil
}

func (l *dataDirLock) close() error {
	if l == nil || l.file == nil {
		return nil
	}
	r1, _, callErr := procUnlockFileEx.Call(
		l.file.Fd(),
		0,
		1,
		0,
		uintptr(unsafe.Pointer(&l.overlapped)),
	)
	closeErr := l.file.Close()
	l.file = nil
	if r1 == 0 && callErr != syscall.Errno(0) {
		return callErr
	}
	return closeErr
}
