//go:build unix

package openlinker

import (
	"os"
)

func atomicReplaceFile(source, target string) error {
	return os.Rename(source, target)
}

func syncRuntimeDirectory(path string) error {
	dir, err := os.Open(path)
	if err != nil {
		return err
	}
	defer dir.Close()
	return dir.Sync()
}
