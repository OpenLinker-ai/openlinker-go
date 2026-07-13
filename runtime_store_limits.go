package openlinker

import (
	"errors"
	"fmt"
)

const (
	runtimeSpoolMaximumBytes        int64 = 512 << 20
	runtimeSpoolMaximumRecords      int64 = 10_000
	runtimeSpoolControlReserveBytes int64 = 16 << 20
	runtimeSpoolBackpressurePercent int64 = 80
)

type RuntimeSpoolUsage struct {
	Bytes          int64
	Records        int64
	MaximumBytes   int64
	MaximumRecords int64
	ReserveBytes   int64
	AcceptingRuns  bool
}

func (store *FileRuntimeStore) SpoolUsage() (RuntimeSpoolUsage, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return RuntimeSpoolUsage{}, err
	}
	return store.spoolUsageLocked(), nil
}

func (store *FileRuntimeStore) AcceptsNewRuns() bool {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.readyLocked() != nil {
		return false
	}
	return store.acceptsNewRunsLocked()
}

func (store *FileRuntimeStore) spoolUsageLocked() RuntimeSpoolUsage {
	return RuntimeSpoolUsage{
		Bytes: store.spoolBytes, Records: store.spoolRecords,
		MaximumBytes: store.spoolMaxBytes, MaximumRecords: store.spoolMaxRecords,
		ReserveBytes:  store.spoolReserveBytes,
		AcceptingRuns: store.acceptsNewRunsLocked(),
	}
}

func (store *FileRuntimeStore) acceptsNewRunsLocked() bool {
	if store.spoolMaxBytes <= 0 || store.spoolMaxRecords <= 0 || store.spoolReserveBytes < 0 ||
		store.spoolReserveBytes >= store.spoolMaxBytes {
		return false
	}
	if store.spoolBytes*100 >= store.spoolMaxBytes*runtimeSpoolBackpressurePercent ||
		store.spoolRecords*100 >= store.spoolMaxRecords*runtimeSpoolBackpressurePercent {
		return false
	}
	available, err := store.diskAvailable(store.dataDir)
	return err == nil && available > store.spoolReserveBytes+spoolDiskRecordMax
}

func (store *FileRuntimeStore) ensureSpoolWriteLocked(path string, size int64) error {
	if size <= 0 || size > spoolDiskRecordMax {
		return ErrRuntimeMessageTooLarge
	}
	if _, exists := store.spoolSizes[path]; exists {
		return ErrSpoolRecordConflict
	}
	dataLimit := store.spoolMaxBytes - store.spoolReserveBytes
	if dataLimit <= 0 || store.spoolBytes+size > dataLimit || store.spoolRecords+1 > store.spoolMaxRecords {
		return ErrRuntimeSpoolFull
	}
	available, err := store.diskAvailable(store.dataDir)
	if err != nil {
		return fmt.Errorf("inspect runtime spool capacity: %w", err)
	}
	if available < size+store.spoolReserveBytes {
		return ErrRuntimeSpoolFull
	}
	return nil
}

func (store *FileRuntimeStore) trackSpoolFileLocked(path string, size int64) error {
	if size <= 0 {
		return ErrRuntimeRecordCorrupt
	}
	if existing, exists := store.spoolSizes[path]; exists {
		if existing == size {
			return nil
		}
		return ErrSpoolRecordConflict
	}
	store.spoolSizes[path] = size
	store.spoolBytes += size
	store.spoolRecords++
	return nil
}

func (store *FileRuntimeStore) removeSpoolFileLocked(path string) error {
	if err := durableRemove(path); err != nil {
		return err
	}
	if size, exists := store.spoolSizes[path]; exists {
		delete(store.spoolSizes, path)
		store.spoolBytes -= size
		store.spoolRecords--
		if store.spoolBytes < 0 || store.spoolRecords < 0 {
			return ErrRuntimeRecordCorrupt
		}
	}
	return nil
}

func (store *FileRuntimeStore) setSpoolLimitsForTest(maximumBytes, maximumRecords, reserveBytes int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if maximumBytes <= 0 || maximumRecords <= 0 || reserveBytes < 0 || reserveBytes >= maximumBytes {
		return errors.New("invalid test spool limits")
	}
	store.spoolMaxBytes = maximumBytes
	store.spoolMaxRecords = maximumRecords
	store.spoolReserveBytes = reserveBytes
	return nil
}
