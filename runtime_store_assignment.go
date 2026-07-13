package openlinker

import (
	"encoding/json"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"time"
)

const (
	assignmentPayloadVersion = 1
	assignmentSpoolDirectory = "assignment-spool"
	assignmentSpoolKind      = "runtime_assignment"
)

// DurableAssignmentPayload is encrypted at rest. It is the minimum capability
// required to start a confirmed Attempt after an ACK response is lost. The
// long-lived Agent Token is deliberately not part of this record.
type DurableAssignmentPayload struct {
	Version              int             `json:"version"`
	Identity             AttemptIdentity `json:"identity"`
	Input                json.RawMessage `json:"input"`
	Metadata             json.RawMessage `json:"metadata"`
	NodeEnvelope         string          `json:"node_envelope"`
	AgentInvocationToken string          `json:"agent_invocation_token"`
	OfferExpiresAt       time.Time       `json:"offer_expires_at"`
	AttemptDeadlineAt    time.Time       `json:"attempt_deadline_at"`
	RunDeadlineAt        time.Time       `json:"run_deadline_at"`
	CreatedAt            time.Time       `json:"created_at"`
}

func (store *FileRuntimeStore) StoreAssignmentPayload(payload DurableAssignmentPayload) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return err
	}
	if payload.Version == 0 {
		payload.Version = assignmentPayloadVersion
	}
	if payload.CreatedAt.IsZero() {
		payload.CreatedAt = time.Now().UTC()
	}
	payload.Input = normalizeRawMessage(payload.Input)
	payload.Metadata = normalizeRawMessage(payload.Metadata)
	return store.storeAssignmentPayloadLocked(payload)
}

func (store *FileRuntimeStore) storeAssignmentPayloadLocked(payload DurableAssignmentPayload) error {
	if err := validateAssignmentPayload(payload); err != nil {
		return err
	}
	entry, err := store.assignmentForAttemptLocked(payload.Identity.AttemptID)
	if err != nil {
		return err
	}
	if entry.Record.Identity != payload.Identity {
		return ErrSpoolRecordConflict
	}
	switch entry.Record.State {
	case AssignmentStateReceived, AssignmentStateACKSent, AssignmentStateConfirmed, AssignmentStateStarted, AssignmentStateFinished:
	default:
		return ErrAssignmentTransition
	}
	if existing, exists := store.payloads[payload.Identity.AttemptID]; exists {
		candidate := cloneAssignmentPayload(payload)
		candidate.CreatedAt = existing.CreatedAt
		if reflect.DeepEqual(existing, candidate) {
			return nil
		}
		return ErrSpoolRecordConflict
	}
	path := store.assignmentPayloadPath(payload.Identity.AttemptID, payload.Identity.AssignmentMessageID)
	raw, err := sealRuntimeRecord(
		store.key,
		headerForAttempt(assignmentSpoolKind, payload.Identity.AssignmentMessageID, payload.Identity),
		payload,
	)
	if err != nil {
		return err
	}
	if len(raw) > spoolDiskRecordMax {
		return ErrRuntimeMessageTooLarge
	}
	if err := store.ensureSpoolWriteLocked(path, int64(len(raw))); err != nil {
		return err
	}
	if err := atomicWriteDurable(path, raw, 0o600, store.hook); err != nil {
		store.poisoned = err
		return err
	}
	if err := store.trackSpoolFileLocked(path, int64(len(raw))); err != nil {
		store.poisoned = err
		return err
	}
	store.payloads[payload.Identity.AttemptID] = cloneAssignmentPayload(payload)
	return nil
}

func (store *FileRuntimeStore) AssignmentPayload(attemptID string) (DurableAssignmentPayload, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return DurableAssignmentPayload{}, err
	}
	payload, exists := store.payloads[attemptID]
	if !exists {
		return DurableAssignmentPayload{}, ErrSpoolRecordNotFound
	}
	return cloneAssignmentPayload(payload), nil
}

func (store *FileRuntimeStore) loadAssignmentSpool() error {
	directory := filepath.Join(store.dataDir, assignmentSpoolDirectory)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, diskEntry := range entries {
		if diskEntry.IsDir() || filepath.Ext(diskEntry.Name()) != spoolRecordExtension {
			return &RuntimeRecordError{Kind: "assignment spool", Reason: ErrRuntimeRecordCorrupt}
		}
		path := filepath.Join(directory, diskEntry.Name())
		raw, err := readBoundedFile(path, spoolDiskRecordMax)
		if err != nil {
			return &RuntimeRecordError{Kind: "assignment spool", Reason: ErrRuntimeRecordCorrupt}
		}
		var payload DurableAssignmentPayload
		header, err := openRuntimeRecord(store.key, raw, assignmentSpoolKind, &payload)
		if err != nil || validateAssignmentPayload(payload) != nil ||
			!headerMatchesAttempt(header, assignmentSpoolKind, payload.Identity.AssignmentMessageID, payload.Identity) {
			return &RuntimeRecordError{Kind: "assignment spool", Reason: ErrRuntimeRecordCorrupt}
		}
		if filepath.Base(store.assignmentPayloadPath(payload.Identity.AttemptID, payload.Identity.AssignmentMessageID)) != diskEntry.Name() ||
			payload.Identity.WorkerID != store.workerID {
			return &RuntimeRecordError{Kind: "assignment spool", Reason: ErrRuntimeRecordCorrupt}
		}
		if _, exists := store.payloads[payload.Identity.AttemptID]; exists {
			return &RuntimeRecordError{Kind: "assignment spool", Reason: ErrSpoolRecordConflict}
		}
		if err := store.trackSpoolFileLocked(path, int64(len(raw))); err != nil {
			return &RuntimeRecordError{Kind: "assignment spool", Reason: err}
		}
		store.payloads[payload.Identity.AttemptID] = cloneAssignmentPayload(payload)
	}
	return nil
}

func (store *FileRuntimeStore) reconcileAssignmentPayloadsLocked() error {
	for attemptID, payload := range store.payloads {
		messageID, exists := store.attempts[attemptID]
		if !exists {
			return &RuntimeRecordError{Kind: "assignment spool", Reason: ErrAssignmentNotFound}
		}
		entry := store.entries[messageID]
		if entry.Deleted {
			if err := store.removeAssignmentPayloadLocked(payload); err != nil {
				return err
			}
			continue
		}
		if entry.Record.Identity != payload.Identity {
			return &RuntimeRecordError{Kind: "assignment spool", Reason: ErrSpoolRecordConflict}
		}
	}
	return nil
}

func (store *FileRuntimeStore) removeAssignmentPayloadLocked(payload DurableAssignmentPayload) error {
	path := store.assignmentPayloadPath(payload.Identity.AttemptID, payload.Identity.AssignmentMessageID)
	if err := store.removeSpoolFileLocked(path); err != nil {
		return err
	}
	delete(store.payloads, payload.Identity.AttemptID)
	return nil
}

func (store *FileRuntimeStore) assignmentPayloadPath(attemptID, assignmentMessageID string) string {
	return store.spoolRecordPath(assignmentSpoolKind, attemptID, assignmentMessageID)
}

func validateAssignmentPayload(payload DurableAssignmentPayload) error {
	if payload.Version != assignmentPayloadVersion || payload.Identity.validate() != nil ||
		!json.Valid(payload.Input) || !json.Valid(payload.Metadata) ||
		len(payload.Input)+len(payload.Metadata) > spoolPayloadMax ||
		!strings.HasPrefix(payload.NodeEnvelope, "ol_ctx_v2.") || len(payload.NodeEnvelope) > 8192 ||
		!strings.HasPrefix(payload.AgentInvocationToken, "ol_inv_v2.") || len(payload.AgentInvocationToken) > 8192 ||
		payload.OfferExpiresAt.IsZero() || payload.AttemptDeadlineAt.IsZero() || payload.RunDeadlineAt.IsZero() || payload.CreatedAt.IsZero() {
		return ErrRuntimeRecordCorrupt
	}
	return nil
}

func cloneAssignmentPayload(payload DurableAssignmentPayload) DurableAssignmentPayload {
	payload.Input = cloneRawMessage(payload.Input)
	payload.Metadata = cloneRawMessage(payload.Metadata)
	return payload
}
