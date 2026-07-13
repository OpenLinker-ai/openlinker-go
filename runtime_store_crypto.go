package openlinker

import (
	"crypto/aes"
	"crypto/cipher"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
)

const (
	runtimeCryptoVersion = 1
	runtimeSpoolKeyFile  = "spool.key"
	runtimeSpoolKeyBytes = 32
)

type diskRecordHeader struct {
	Version             int    `json:"version"`
	Kind                string `json:"kind"`
	NodeID              string `json:"node_id,omitempty"`
	AgentID             string `json:"agent_id,omitempty"`
	WorkerID            string `json:"worker_id"`
	RuntimeSessionID    string `json:"runtime_session_id,omitempty"`
	SessionEpoch        int64  `json:"session_epoch,omitempty"`
	AssignmentMessageID string `json:"assignment_message_id,omitempty"`
	RunID               string `json:"run_id,omitempty"`
	AttemptID           string `json:"attempt_id,omitempty"`
	OfferID             string `json:"offer_id,omitempty"`
	LeaseID             string `json:"lease_id,omitempty"`
	FencingToken        int64  `json:"fencing_token,omitempty"`
	MessageID           string `json:"message_id"`
}

type encryptedDiskRecord struct {
	Version    int              `json:"version"`
	Header     diskRecordHeader `json:"header"`
	Nonce      []byte           `json:"nonce"`
	Ciphertext []byte           `json:"ciphertext"`
	Checksum   string           `json:"checksum"`
}

func loadOrCreateRuntimeSpoolKey(dataDir string, durableRecordsExist bool) ([]byte, error) {
	path := filepath.Join(dataDir, runtimeSpoolKeyFile)
	info, err := os.Lstat(path)
	if errors.Is(err, os.ErrNotExist) {
		if durableRecordsExist {
			return nil, ErrRuntimeSpoolKeyMissing
		}
		key := make([]byte, runtimeSpoolKeyBytes)
		if _, err := rand.Read(key); err != nil {
			return nil, fmt.Errorf("generate runtime spool key: %w", err)
		}
		if err := atomicWriteDurable(path, key, 0o600, nil); err != nil {
			zeroBytes(key)
			return nil, fmt.Errorf("persist runtime spool key: %w", err)
		}
		return key, nil
	}
	if err != nil {
		return nil, fmt.Errorf("inspect runtime spool key: %w", err)
	}
	if !info.Mode().IsRegular() || !runtimeFileModeIsPrivate(info.Mode()) || info.Size() != runtimeSpoolKeyBytes {
		return nil, ErrRuntimeSpoolKeyInvalid
	}
	key, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read runtime spool key: %w", err)
	}
	if len(key) != runtimeSpoolKeyBytes {
		zeroBytes(key)
		return nil, ErrRuntimeSpoolKeyInvalid
	}
	return key, nil
}

func sealRuntimeRecord(key []byte, header diskRecordHeader, body any) ([]byte, error) {
	if len(key) != runtimeSpoolKeyBytes {
		return nil, ErrRuntimeSpoolKeyInvalid
	}
	header.Version = runtimeCryptoVersion
	headerJSON, err := json.Marshal(header)
	if err != nil {
		return nil, err
	}
	bodyJSON, err := json.Marshal(body)
	if err != nil {
		return nil, err
	}
	defer zeroBytes(bodyJSON)
	block, err := aes.NewCipher(key)
	if err != nil {
		return nil, err
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil {
		return nil, err
	}
	nonce := make([]byte, gcm.NonceSize())
	if _, err := rand.Read(nonce); err != nil {
		return nil, err
	}
	ciphertext := gcm.Seal(nil, nonce, bodyJSON, headerJSON)
	envelope := encryptedDiskRecord{
		Version:    runtimeCryptoVersion,
		Header:     header,
		Nonce:      nonce,
		Ciphertext: ciphertext,
	}
	envelope.Checksum = encryptedRecordChecksum(headerJSON, nonce, ciphertext)
	return json.Marshal(envelope)
}

func openRuntimeRecord(key, raw []byte, expectedKind string, target any) (diskRecordHeader, error) {
	var envelope encryptedDiskRecord
	if err := decodeStrictJSON(raw, &envelope); err != nil {
		return diskRecordHeader{}, ErrRuntimeRecordCorrupt
	}
	if envelope.Version != runtimeCryptoVersion || envelope.Header.Version != runtimeCryptoVersion || envelope.Header.Kind != expectedKind {
		return diskRecordHeader{}, ErrRuntimeRecordCorrupt
	}
	headerJSON, err := json.Marshal(envelope.Header)
	if err != nil {
		return diskRecordHeader{}, ErrRuntimeRecordCorrupt
	}
	if !constantChecksumEqual(envelope.Checksum, encryptedRecordChecksum(headerJSON, envelope.Nonce, envelope.Ciphertext)) {
		return diskRecordHeader{}, ErrRuntimeRecordCorrupt
	}
	block, err := aes.NewCipher(key)
	if err != nil {
		return diskRecordHeader{}, ErrRuntimeSpoolKeyInvalid
	}
	gcm, err := cipher.NewGCM(block)
	if err != nil || len(envelope.Nonce) != gcm.NonceSize() {
		return diskRecordHeader{}, ErrRuntimeRecordCorrupt
	}
	plaintext, err := gcm.Open(nil, envelope.Nonce, envelope.Ciphertext, headerJSON)
	if err != nil {
		return diskRecordHeader{}, ErrRuntimeRecordCorrupt
	}
	defer zeroBytes(plaintext)
	if err := decodeStrictJSON(plaintext, target); err != nil {
		return diskRecordHeader{}, ErrRuntimeRecordCorrupt
	}
	return envelope.Header, nil
}

func encryptedRecordChecksum(headerJSON, nonce, ciphertext []byte) string {
	hash := sha256.New()
	_, _ = hash.Write(headerJSON)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(nonce)
	_, _ = hash.Write([]byte{0})
	_, _ = hash.Write(ciphertext)
	return hex.EncodeToString(hash.Sum(nil))
}

func headerForAttempt(kind, messageID string, identity AttemptIdentity) diskRecordHeader {
	return diskRecordHeader{
		Version:             runtimeCryptoVersion,
		Kind:                kind,
		NodeID:              identity.NodeID,
		AgentID:             identity.AgentID,
		WorkerID:            identity.WorkerID,
		RuntimeSessionID:    identity.RuntimeSessionID,
		SessionEpoch:        identity.SessionEpoch,
		AssignmentMessageID: identity.AssignmentMessageID,
		RunID:               identity.RunID,
		AttemptID:           identity.AttemptID,
		OfferID:             identity.OfferID,
		LeaseID:             identity.LeaseID,
		FencingToken:        identity.FencingToken,
		MessageID:           messageID,
	}
}

func headerMatchesAttempt(header diskRecordHeader, kind, messageID string, identity AttemptIdentity) bool {
	return header == headerForAttempt(kind, messageID, identity)
}

func zeroBytes(value []byte) {
	for index := range value {
		value[index] = 0
	}
}
