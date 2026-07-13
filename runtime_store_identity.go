package openlinker

import (
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
)

const (
	runtimeIdentityVersion = 1
	runtimeIdentityFile    = "runtime-identity.json"
)

// RuntimeIdentity is the durable installation identity and the identity of
// this process start. WorkerID remains stable for the data directory. A new
// RuntimeSessionID is generated and SessionEpoch is durably incremented by
// every successful OpenFileRuntimeStore call.
type RuntimeIdentity struct {
	WorkerID         string `json:"worker_id"`
	RuntimeSessionID string `json:"runtime_session_id"`
	SessionEpoch     int64  `json:"session_epoch"`
}

type runtimeIdentityDisk struct {
	Version      int    `json:"version"`
	WorkerID     string `json:"worker_id"`
	SessionEpoch int64  `json:"session_epoch"`
	Checksum     string `json:"checksum"`
}

func loadRuntimeIdentity(dataDir string, durableRecordsExist bool) (runtimeIdentityDisk, error) {
	path := filepath.Join(dataDir, runtimeIdentityFile)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if durableRecordsExist {
			return runtimeIdentityDisk{}, ErrRuntimeIdentityMissing
		}
		workerID, idErr := newRuntimeUUID()
		if idErr != nil {
			return runtimeIdentityDisk{}, fmt.Errorf("generate worker identity: %w", idErr)
		}
		disk := runtimeIdentityDisk{
			Version:  runtimeIdentityVersion,
			WorkerID: workerID,
		}
		if err := persistRuntimeIdentityDisk(dataDir, disk); err != nil {
			return runtimeIdentityDisk{}, err
		}
		return disk, nil
	}
	if err != nil {
		return runtimeIdentityDisk{}, fmt.Errorf("inspect runtime identity: %w", err)
	}
	if !info.Mode().IsRegular() || !runtimeFileModeIsPrivate(info.Mode()) || info.Size() <= 0 || info.Size() > 4096 {
		return runtimeIdentityDisk{}, ErrRuntimeIdentityCorrupt
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		return runtimeIdentityDisk{}, fmt.Errorf("read runtime identity: %w", err)
	}
	var disk runtimeIdentityDisk
	if err := decodeStrictJSON(raw, &disk); err != nil {
		return runtimeIdentityDisk{}, ErrRuntimeIdentityCorrupt
	}
	if disk.Version != runtimeIdentityVersion || !validRuntimeID(disk.WorkerID) || disk.SessionEpoch < 0 {
		return runtimeIdentityDisk{}, ErrRuntimeIdentityCorrupt
	}
	if !constantChecksumEqual(disk.Checksum, runtimeIdentityChecksum(disk)) {
		return runtimeIdentityDisk{}, ErrRuntimeIdentityCorrupt
	}
	return disk, nil
}

func startRuntimeSession(dataDir string, disk runtimeIdentityDisk) (RuntimeIdentity, error) {
	if disk.SessionEpoch == math.MaxInt64 {
		return RuntimeIdentity{}, fmt.Errorf("runtime session epoch exhausted")
	}
	sessionID, err := newRuntimeUUID()
	if err != nil {
		return RuntimeIdentity{}, fmt.Errorf("generate runtime session identity: %w", err)
	}
	disk.Version = runtimeIdentityVersion
	disk.SessionEpoch++
	if err := persistRuntimeIdentityDisk(dataDir, disk); err != nil {
		return RuntimeIdentity{}, err
	}
	return RuntimeIdentity{
		WorkerID:         disk.WorkerID,
		RuntimeSessionID: sessionID,
		SessionEpoch:     disk.SessionEpoch,
	}, nil
}

func persistRuntimeIdentityDisk(dataDir string, disk runtimeIdentityDisk) error {
	disk.Checksum = runtimeIdentityChecksum(disk)
	raw, err := json.Marshal(disk)
	if err != nil {
		return fmt.Errorf("encode runtime identity: %w", err)
	}
	if err := atomicWriteDurable(filepath.Join(dataDir, runtimeIdentityFile), raw, 0o600, nil); err != nil {
		return fmt.Errorf("persist runtime identity: %w", err)
	}
	return nil
}

func runtimeIdentityChecksum(disk runtimeIdentityDisk) string {
	sum := sha256.Sum256([]byte(fmt.Sprintf("%d\x00%s\x00%d", disk.Version, disk.WorkerID, disk.SessionEpoch)))
	return hex.EncodeToString(sum[:])
}

func newRuntimeUUID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", err
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x",
		value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}
