//go:build windows

package openlinker

import (
	"math"

	"golang.org/x/sys/windows"
)

func runtimeDiskAvailableBytes(path string) (int64, error) {
	directory, err := windows.UTF16PtrFromString(path)
	if err != nil {
		return 0, err
	}
	var available uint64
	if err = windows.GetDiskFreeSpaceEx(directory, &available, nil, nil); err != nil {
		return 0, err
	}
	if available > math.MaxInt64 {
		return math.MaxInt64, nil
	}
	return int64(available), nil
}
