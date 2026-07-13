//go:build !windows

package openlinker

import (
	"math"

	"golang.org/x/sys/unix"
)

func runtimeDiskAvailableBytes(path string) (int64, error) {
	var stats unix.Statfs_t
	if err := unix.Statfs(path, &stats); err != nil {
		return 0, err
	}
	if stats.Bavail <= 0 || stats.Bsize <= 0 {
		return 0, nil
	}
	blocks, blockSize := uint64(stats.Bavail), uint64(stats.Bsize)
	if blocks > uint64(math.MaxInt64)/blockSize {
		return math.MaxInt64, nil
	}
	return int64(blocks * blockSize), nil
}
