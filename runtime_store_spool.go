package openlinker

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"time"
)

const (
	spoolRecordVersion   = 1
	eventSpoolDirectory  = "event-spool"
	resultSpoolDirectory = "result-spool"
	spoolRecordExtension = ".record"
	eventSpoolKind       = "run_event"
	resultSpoolKind      = "run_result"
	spoolPayloadMax      = 4 << 20
	spoolDiskRecordMax   = 8 << 20
)

type EventSpoolRecord struct {
	Version        int             `json:"version"`
	Identity       AttemptIdentity `json:"identity"`
	ClientEventID  string          `json:"client_event_id"`
	ClientEventSeq int64           `json:"client_event_seq"`
	EventType      string          `json:"event_type"`
	Payload        json.RawMessage `json:"payload"`
	CreatedAt      time.Time       `json:"created_at"`
}

type ResultSpoolRecord struct {
	Version             int             `json:"version"`
	Identity            AttemptIdentity `json:"identity"`
	ResultID            string          `json:"result_id"`
	FinalClientEventSeq int64           `json:"final_client_event_seq"`
	Status              string          `json:"status"`
	Payload             json.RawMessage `json:"payload"`
	CreatedAt           time.Time       `json:"created_at"`
}

// AppendEvent allocates a stable UUID and the next per-Attempt sequence while
// holding the store lock, fsyncs the encrypted record, and only then returns it
// to the caller for transmission.
func (store *FileRuntimeStore) AppendEvent(identity AttemptIdentity, eventType string, payload json.RawMessage) (EventSpoolRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return EventSpoolRecord{}, err
	}
	entry, err := store.assignmentForAttemptLocked(identity.AttemptID)
	if err != nil {
		return EventSpoolRecord{}, err
	}
	if entry.Record.Identity != identity || entry.Record.State != AssignmentStateStarted {
		return EventSpoolRecord{}, ErrAssignmentTransition
	}
	if entry.Record.LastClientEventSeq == math.MaxInt64 {
		return EventSpoolRecord{}, ErrEventSequence
	}
	eventID, err := newRuntimeUUID()
	if err != nil {
		return EventSpoolRecord{}, err
	}
	record := EventSpoolRecord{
		Version:        spoolRecordVersion,
		Identity:       identity,
		ClientEventID:  eventID,
		ClientEventSeq: entry.Record.LastClientEventSeq + 1,
		EventType:      eventType,
		Payload:        normalizeRawMessage(payload),
		CreatedAt:      time.Now().UTC(),
	}
	if err := store.storeEventLocked(record); err != nil {
		return record, err
	}
	return cloneEventRecord(record), nil
}

// StoreEvent persists a caller-provided stable event ID and sequence. It is
// idempotent only when every immutable field is identical.
func (store *FileRuntimeStore) StoreEvent(record EventSpoolRecord) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return err
	}
	if record.Version == 0 {
		record.Version = spoolRecordVersion
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	record.Payload = normalizeRawMessage(record.Payload)
	return store.storeEventLocked(record)
}

func (store *FileRuntimeStore) storeEventLocked(record EventSpoolRecord) error {
	if err := validateEventSpoolRecord(record); err != nil {
		return err
	}
	entry, err := store.assignmentForAttemptLocked(record.Identity.AttemptID)
	if err != nil {
		return err
	}
	if entry.Record.Identity != record.Identity || entry.Record.State != AssignmentStateStarted {
		return ErrAssignmentTransition
	}
	if existing, exists := store.events[record.ClientEventID]; exists {
		if reflect.DeepEqual(existing, record) {
			return nil
		}
		return ErrSpoolRecordConflict
	}
	if record.ClientEventSeq != entry.Record.LastClientEventSeq+1 {
		return ErrEventSequence
	}
	if bySequence := store.eventSeqs[record.Identity.AttemptID]; bySequence != nil {
		if _, exists := bySequence[record.ClientEventSeq]; exists {
			return ErrEventSequence
		}
	}
	path := store.spoolRecordPath(eventSpoolKind, record.Identity.AttemptID, record.ClientEventID)
	raw, err := sealRuntimeRecord(store.key, headerForAttempt(eventSpoolKind, record.ClientEventID, record.Identity), record)
	if err != nil {
		return err
	}
	if len(raw) > spoolDiskRecordMax {
		return fmt.Errorf("event durable record exceeds limit")
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
	store.events[record.ClientEventID] = cloneEventRecord(record)
	if store.eventSeqs[record.Identity.AttemptID] == nil {
		store.eventSeqs[record.Identity.AttemptID] = make(map[int64]string)
	}
	store.eventSeqs[record.Identity.AttemptID][record.ClientEventSeq] = record.ClientEventID
	nextRecord := cloneAssignmentRecord(entry.Record)
	nextRecord.LastClientEventSeq = record.ClientEventSeq
	nextRecord.UpdatedAt = time.Now().UTC()
	if err := store.appendJournalLocked(assignmentJournalEntry{Record: nextRecord}); err != nil {
		return err
	}
	return nil
}

func (store *FileRuntimeStore) PendingEvents(attemptID string) ([]EventSpoolRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return nil, err
	}
	entry, err := store.assignmentForAttemptLocked(attemptID)
	if err != nil {
		return nil, err
	}
	bySequence := store.eventSeqs[attemptID]
	records := make([]EventSpoolRecord, 0, len(bySequence))
	for _, eventID := range bySequence {
		record := store.events[eventID]
		if !eventSequenceACKed(entry.Record, record.ClientEventSeq) {
			records = append(records, cloneEventRecord(record))
		}
	}
	sort.Slice(records, func(left, right int) bool {
		return records[left].ClientEventSeq < records[right].ClientEventSeq
	})
	return records, nil
}

func (store *FileRuntimeStore) EventsInRanges(attemptID string, ranges []RuntimeEventRange) ([]EventSpoolRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return nil, err
	}
	entry, err := store.assignmentForAttemptLocked(attemptID)
	if err != nil {
		return nil, err
	}
	bySequence := store.eventSeqs[attemptID]
	records := make([]EventSpoolRecord, 0)
	previous := int64(0)
	for _, eventRange := range ranges {
		if eventRange.Start < 1 || eventRange.End < eventRange.Start || eventRange.Start <= previous || eventRange.End > entry.Record.LastClientEventSeq {
			return nil, ErrEventSequence
		}
		for sequence := eventRange.Start; sequence <= eventRange.End; sequence++ {
			eventID, exists := bySequence[sequence]
			if !exists {
				return nil, ErrSpoolRecordNotFound
			}
			records = append(records, cloneEventRecord(store.events[eventID]))
		}
		previous = eventRange.End
	}
	return records, nil
}

// AckEvent durably records the business ACK. The encrypted Event remains until
// the Result is ACKed so an EVENTS_MISSING response can request an exact replay.
func (store *FileRuntimeStore) AckEvent(attemptID, clientEventID string, clientEventSeq int64) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return err
	}
	if clientEventSeq <= 0 {
		return ErrEventSequence
	}
	entry, err := store.assignmentForAttemptLocked(attemptID)
	if err != nil {
		return err
	}
	if eventSequenceACKed(entry.Record, clientEventSeq) {
		if record, exists := store.events[clientEventID]; exists {
			if record.Identity.AttemptID != attemptID || record.ClientEventSeq != clientEventSeq {
				return ErrSpoolRecordConflict
			}
			if entry.Record.State == AssignmentStateResultACKed || entry.Record.State == AssignmentStateRevoked {
				return store.removeEventLocked(record)
			}
		}
		return nil
	}
	record, exists := store.events[clientEventID]
	if !exists {
		return ErrSpoolRecordNotFound
	}
	if record.Identity.AttemptID != attemptID || record.ClientEventSeq != clientEventSeq {
		return ErrSpoolRecordConflict
	}
	nextRecord := cloneAssignmentRecord(entry.Record)
	applyEventACK(&nextRecord, clientEventSeq)
	nextRecord.UpdatedAt = time.Now().UTC()
	if err := store.appendJournalLocked(assignmentJournalEntry{Record: nextRecord}); err != nil {
		return err
	}
	if entry.Record.State == AssignmentStateResultACKed || entry.Record.State == AssignmentStateRevoked {
		return store.removeEventLocked(record)
	}
	return nil
}

func (store *FileRuntimeStore) removeEventLocked(record EventSpoolRecord) error {
	path := store.spoolRecordPath(eventSpoolKind, record.Identity.AttemptID, record.ClientEventID)
	if err := store.removeSpoolFileLocked(path); err != nil {
		return err
	}
	delete(store.events, record.ClientEventID)
	if bySequence := store.eventSeqs[record.Identity.AttemptID]; bySequence != nil {
		delete(bySequence, record.ClientEventSeq)
		if len(bySequence) == 0 {
			delete(store.eventSeqs, record.Identity.AttemptID)
		}
	}
	return nil
}

// AppendResult fixes final_client_event_seq to the last durably allocated
// Event sequence. The encrypted Result is fsynced before the journal moves from
// started to finished.
func (store *FileRuntimeStore) AppendResult(identity AttemptIdentity, status string, payload json.RawMessage) (ResultSpoolRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return ResultSpoolRecord{}, err
	}
	entry, err := store.assignmentForAttemptLocked(identity.AttemptID)
	if err != nil {
		return ResultSpoolRecord{}, err
	}
	if entry.Record.Identity != identity {
		return ResultSpoolRecord{}, ErrSpoolRecordConflict
	}
	if existing, exists := store.results[identity.AttemptID]; exists {
		return cloneResultRecord(existing), nil
	}
	if entry.Record.State != AssignmentStateStarted {
		return ResultSpoolRecord{}, ErrAssignmentTransition
	}
	resultID, err := newRuntimeUUID()
	if err != nil {
		return ResultSpoolRecord{}, err
	}
	record := ResultSpoolRecord{
		Version:             spoolRecordVersion,
		Identity:            identity,
		ResultID:            resultID,
		FinalClientEventSeq: entry.Record.LastClientEventSeq,
		Status:              status,
		Payload:             normalizeRawMessage(payload),
		CreatedAt:           time.Now().UTC(),
	}
	if err := store.storeResultLocked(record); err != nil {
		return record, err
	}
	return cloneResultRecord(record), nil
}

func (store *FileRuntimeStore) StoreResult(record ResultSpoolRecord) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return err
	}
	if record.Version == 0 {
		record.Version = spoolRecordVersion
	}
	if record.CreatedAt.IsZero() {
		record.CreatedAt = time.Now().UTC()
	}
	record.Payload = normalizeRawMessage(record.Payload)
	return store.storeResultLocked(record)
}

func (store *FileRuntimeStore) storeResultLocked(record ResultSpoolRecord) error {
	if err := validateResultSpoolRecord(record); err != nil {
		return err
	}
	entry, err := store.assignmentForAttemptLocked(record.Identity.AttemptID)
	if err != nil {
		return err
	}
	if entry.Record.Identity != record.Identity {
		return ErrSpoolRecordConflict
	}
	if existing, exists := store.results[record.Identity.AttemptID]; exists {
		if reflect.DeepEqual(existing, record) {
			return nil
		}
		return ErrResultAlreadyExists
	}
	if entry.Record.State != AssignmentStateStarted || record.FinalClientEventSeq != entry.Record.LastClientEventSeq {
		return ErrAssignmentTransition
	}
	path := store.spoolRecordPath(resultSpoolKind, record.Identity.AttemptID, record.ResultID)
	raw, err := sealRuntimeRecord(store.key, headerForAttempt(resultSpoolKind, record.ResultID, record.Identity), record)
	if err != nil {
		return err
	}
	if len(raw) > spoolDiskRecordMax {
		return fmt.Errorf("result durable record exceeds limit")
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
	store.results[record.Identity.AttemptID] = cloneResultRecord(record)
	nextRecord := cloneAssignmentRecord(entry.Record)
	nextRecord.State = AssignmentStateFinished
	nextRecord.ResultID = record.ResultID
	nextRecord.FinalClientEventSeq = record.FinalClientEventSeq
	nextRecord.UpdatedAt = time.Now().UTC()
	if err := store.appendJournalLocked(assignmentJournalEntry{Record: nextRecord}); err != nil {
		return err
	}
	return nil
}

func (store *FileRuntimeStore) PendingResult(attemptID string) (ResultSpoolRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return ResultSpoolRecord{}, err
	}
	if _, err := store.assignmentForAttemptLocked(attemptID); err != nil {
		return ResultSpoolRecord{}, err
	}
	record, exists := store.results[attemptID]
	if !exists {
		return ResultSpoolRecord{}, ErrSpoolRecordNotFound
	}
	return cloneResultRecord(record), nil
}

func (store *FileRuntimeStore) AckResult(attemptID, resultID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return err
	}
	entry, err := store.assignmentForAttemptLocked(attemptID)
	if err != nil {
		return err
	}
	if entry.Record.State == AssignmentStateResultACKed {
		if entry.Record.ResultID != resultID {
			return ErrSpoolRecordConflict
		}
		if existing, exists := store.results[attemptID]; exists {
			if err := store.removeResultLocked(existing); err != nil {
				return err
			}
		}
		return store.removeAttemptEventsLocked(attemptID)
	}
	record, exists := store.results[attemptID]
	if !exists {
		return ErrSpoolRecordNotFound
	}
	if entry.Record.State == AssignmentStateRevoked {
		if record.ResultID != resultID || entry.Record.ResultID != resultID {
			return ErrSpoolRecordConflict
		}
		if err := store.removeResultLocked(record); err != nil {
			return err
		}
		return store.removeAttemptEventsLocked(attemptID)
	}
	if entry.Record.State != AssignmentStateFinished || record.ResultID != resultID || entry.Record.ResultID != resultID {
		return ErrSpoolRecordConflict
	}
	nextRecord := cloneAssignmentRecord(entry.Record)
	nextRecord.State = AssignmentStateResultACKed
	nextRecord.UpdatedAt = time.Now().UTC()
	if err := store.appendJournalLocked(assignmentJournalEntry{Record: nextRecord}); err != nil {
		return err
	}
	if err := store.removeResultLocked(record); err != nil {
		return err
	}
	return store.removeAttemptEventsLocked(attemptID)
}

func (store *FileRuntimeStore) removeResultLocked(record ResultSpoolRecord) error {
	path := store.spoolRecordPath(resultSpoolKind, record.Identity.AttemptID, record.ResultID)
	if err := store.removeSpoolFileLocked(path); err != nil {
		return err
	}
	delete(store.results, record.Identity.AttemptID)
	return nil
}

func (store *FileRuntimeStore) removeAttemptEventsLocked(attemptID string) error {
	bySequence := store.eventSeqs[attemptID]
	sequences := make([]int64, 0, len(bySequence))
	for sequence := range bySequence {
		sequences = append(sequences, sequence)
	}
	sort.Slice(sequences, func(left, right int) bool { return sequences[left] < sequences[right] })
	for _, sequence := range sequences {
		eventID := bySequence[sequence]
		if err := store.removeEventLocked(store.events[eventID]); err != nil {
			return err
		}
	}
	return nil
}

func (store *FileRuntimeStore) ClearTerminalEvents(attemptID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return err
	}
	entry, err := store.assignmentForAttemptLocked(attemptID)
	if err != nil {
		return err
	}
	if entry.Record.State != AssignmentStateResultACKed && entry.Record.State != AssignmentStateRevoked {
		return ErrAssignmentCleanup
	}
	return store.removeAttemptEventsLocked(attemptID)
}

func (store *FileRuntimeStore) loadSpool() error {
	for _, directory := range []string{assignmentSpoolDirectory, eventSpoolDirectory, resultSpoolDirectory} {
		path := filepath.Join(store.dataDir, directory)
		created, err := ensurePrivateSpoolDirectory(path)
		if err != nil {
			return err
		}
		if created {
			if err := syncRuntimeDirectory(store.dataDir); err != nil {
				return err
			}
		}
		if err := cleanupDurableTemps(path); err != nil {
			return err
		}
	}
	if err := store.loadAssignmentSpool(); err != nil {
		return err
	}
	if err := store.loadEventSpool(); err != nil {
		return err
	}
	return store.loadResultSpool()
}

func ensurePrivateSpoolDirectory(path string) (bool, error) {
	info, err := os.Lstat(path)
	created := false
	if errors.Is(err, os.ErrNotExist) {
		if err := os.Mkdir(path, 0o700); err != nil {
			return false, err
		}
		created = true
		info, err = os.Lstat(path)
	}
	if err != nil {
		return false, err
	}
	if !info.IsDir() || info.Mode()&os.ModeSymlink != 0 {
		return false, ErrRuntimeRecordCorrupt
	}
	if err := os.Chmod(path, 0o700); err != nil {
		return false, err
	}
	return created, nil
}

func (store *FileRuntimeStore) loadEventSpool() error {
	directory := filepath.Join(store.dataDir, eventSpoolDirectory)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != spoolRecordExtension {
			return &RuntimeRecordError{Kind: "event spool", Reason: ErrRuntimeRecordCorrupt}
		}
		path := filepath.Join(directory, entry.Name())
		raw, err := readBoundedFile(path, spoolDiskRecordMax)
		if err != nil {
			return &RuntimeRecordError{Kind: "event spool", Reason: ErrRuntimeRecordCorrupt}
		}
		var record EventSpoolRecord
		header, err := openRuntimeRecord(store.key, raw, eventSpoolKind, &record)
		if err != nil || validateEventSpoolRecord(record) != nil || !headerMatchesAttempt(header, eventSpoolKind, record.ClientEventID, record.Identity) {
			return &RuntimeRecordError{Kind: "event spool", Reason: ErrRuntimeRecordCorrupt}
		}
		if filepath.Base(store.spoolRecordPath(eventSpoolKind, record.Identity.AttemptID, record.ClientEventID)) != entry.Name() || record.Identity.WorkerID != store.workerID {
			return &RuntimeRecordError{Kind: "event spool", Reason: ErrRuntimeRecordCorrupt}
		}
		if _, exists := store.events[record.ClientEventID]; exists {
			return &RuntimeRecordError{Kind: "event spool", Reason: ErrSpoolRecordConflict}
		}
		if err := store.trackSpoolFileLocked(path, int64(len(raw))); err != nil {
			return &RuntimeRecordError{Kind: "event spool", Reason: err}
		}
		if store.eventSeqs[record.Identity.AttemptID] == nil {
			store.eventSeqs[record.Identity.AttemptID] = make(map[int64]string)
		}
		if _, exists := store.eventSeqs[record.Identity.AttemptID][record.ClientEventSeq]; exists {
			return &RuntimeRecordError{Kind: "event spool", Reason: ErrEventSequence}
		}
		store.events[record.ClientEventID] = cloneEventRecord(record)
		store.eventSeqs[record.Identity.AttemptID][record.ClientEventSeq] = record.ClientEventID
	}
	return nil
}

func (store *FileRuntimeStore) loadResultSpool() error {
	directory := filepath.Join(store.dataDir, resultSpoolDirectory)
	entries, err := os.ReadDir(directory)
	if err != nil {
		return err
	}
	for _, entry := range entries {
		if entry.IsDir() || filepath.Ext(entry.Name()) != spoolRecordExtension {
			return &RuntimeRecordError{Kind: "result spool", Reason: ErrRuntimeRecordCorrupt}
		}
		path := filepath.Join(directory, entry.Name())
		raw, err := readBoundedFile(path, spoolDiskRecordMax)
		if err != nil {
			return &RuntimeRecordError{Kind: "result spool", Reason: ErrRuntimeRecordCorrupt}
		}
		var record ResultSpoolRecord
		header, err := openRuntimeRecord(store.key, raw, resultSpoolKind, &record)
		if err != nil || validateResultSpoolRecord(record) != nil || !headerMatchesAttempt(header, resultSpoolKind, record.ResultID, record.Identity) {
			return &RuntimeRecordError{Kind: "result spool", Reason: ErrRuntimeRecordCorrupt}
		}
		if filepath.Base(store.spoolRecordPath(resultSpoolKind, record.Identity.AttemptID, record.ResultID)) != entry.Name() || record.Identity.WorkerID != store.workerID {
			return &RuntimeRecordError{Kind: "result spool", Reason: ErrRuntimeRecordCorrupt}
		}
		if _, exists := store.results[record.Identity.AttemptID]; exists {
			return &RuntimeRecordError{Kind: "result spool", Reason: ErrResultAlreadyExists}
		}
		if err := store.trackSpoolFileLocked(path, int64(len(raw))); err != nil {
			return &RuntimeRecordError{Kind: "result spool", Reason: err}
		}
		store.results[record.Identity.AttemptID] = cloneResultRecord(record)
	}
	return nil
}

func (store *FileRuntimeStore) reconcileSpoolAndJournalLocked() error {
	if err := store.reconcileAssignmentPayloadsLocked(); err != nil {
		return err
	}
	for attemptID, bySequence := range cloneEventSequenceIndex(store.eventSeqs) {
		entry, err := store.assignmentForAttemptLocked(attemptID)
		if err != nil {
			return &RuntimeRecordError{Kind: "event spool", Reason: err}
		}
		for sequence, eventID := range bySequence {
			record := store.events[eventID]
			if record.Identity != entry.Record.Identity {
				return &RuntimeRecordError{Kind: "event spool", Reason: ErrSpoolRecordConflict}
			}
			if eventSequenceACKed(entry.Record, sequence) &&
				(entry.Record.State == AssignmentStateResultACKed || entry.Record.State == AssignmentStateRevoked) {
				if err := store.removeEventLocked(record); err != nil {
					return err
				}
			}
		}
	}
	for attemptID, messageID := range store.attempts {
		entry := store.entries[messageID]
		if entry.Deleted {
			continue
		}
		maxSequence := entry.Record.LastClientEventSeq
		for sequence := range store.eventSeqs[attemptID] {
			if sequence > maxSequence {
				maxSequence = sequence
			}
		}
		if maxSequence > entry.Record.LastClientEventSeq {
			if entry.Record.State != AssignmentStateStarted || entry.Record.ResultID != "" {
				return &RuntimeRecordError{Kind: "event spool", Reason: ErrEventSequence}
			}
			nextRecord := cloneAssignmentRecord(entry.Record)
			nextRecord.LastClientEventSeq = maxSequence
			nextRecord.UpdatedAt = time.Now().UTC()
			if err := store.appendJournalLocked(assignmentJournalEntry{Record: nextRecord}); err != nil {
				return err
			}
			entry = store.entries[messageID]
		}
		if err := store.validateEventCoverageLocked(entry.Record); err != nil {
			return &RuntimeRecordError{Kind: "event spool", Reason: err}
		}
		result, hasResult := store.results[attemptID]
		switch entry.Record.State {
		case AssignmentStateStarted:
			if hasResult {
				if result.Identity != entry.Record.Identity || result.FinalClientEventSeq != entry.Record.LastClientEventSeq {
					return &RuntimeRecordError{Kind: "result spool", Reason: ErrSpoolRecordConflict}
				}
				nextRecord := cloneAssignmentRecord(entry.Record)
				nextRecord.State = AssignmentStateFinished
				nextRecord.ResultID = result.ResultID
				nextRecord.FinalClientEventSeq = result.FinalClientEventSeq
				nextRecord.UpdatedAt = time.Now().UTC()
				if err := store.appendJournalLocked(assignmentJournalEntry{Record: nextRecord}); err != nil {
					return err
				}
			}
		case AssignmentStateFinished:
			if !hasResult || result.Identity != entry.Record.Identity || result.ResultID != entry.Record.ResultID || result.FinalClientEventSeq != entry.Record.FinalClientEventSeq {
				return &RuntimeRecordError{Kind: "result spool", Reason: ErrRuntimeRecordCorrupt}
			}
		case AssignmentStateResultACKed:
			if hasResult {
				if result.ResultID != entry.Record.ResultID || result.Identity != entry.Record.Identity {
					return &RuntimeRecordError{Kind: "result spool", Reason: ErrSpoolRecordConflict}
				}
				if err := store.removeResultLocked(result); err != nil {
					return err
				}
			}
		case AssignmentStateRevoked:
			if hasResult && (result.Identity != entry.Record.Identity || result.ResultID != entry.Record.ResultID || result.FinalClientEventSeq != entry.Record.FinalClientEventSeq) {
				return &RuntimeRecordError{Kind: "result spool", Reason: ErrSpoolRecordConflict}
			}
		default:
			if hasResult {
				return &RuntimeRecordError{Kind: "result spool", Reason: ErrAssignmentTransition}
			}
		}
	}
	return nil
}

func (store *FileRuntimeStore) validateEventCoverageLocked(record AssignmentJournalRecord) error {
	ackedOutOfOrder := make(map[int64]struct{}, len(record.AckedOutOfOrderEventSeqs))
	for _, sequence := range record.AckedOutOfOrderEventSeqs {
		ackedOutOfOrder[sequence] = struct{}{}
	}
	pending := store.eventSeqs[record.Identity.AttemptID]
	for sequence := record.AckedClientEventSeq + 1; sequence <= record.LastClientEventSeq; sequence++ {
		if _, acked := ackedOutOfOrder[sequence]; acked {
			continue
		}
		if _, exists := pending[sequence]; !exists {
			return ErrRuntimeRecordCorrupt
		}
	}
	return nil
}

func (store *FileRuntimeStore) spoolRecordPath(kind, attemptID, messageID string) string {
	hash := sha256.Sum256([]byte(kind + "\x00" + attemptID + "\x00" + messageID))
	directory := eventSpoolDirectory
	if kind == assignmentSpoolKind {
		directory = assignmentSpoolDirectory
	} else if kind == resultSpoolKind {
		directory = resultSpoolDirectory
	}
	return filepath.Join(store.dataDir, directory, hex.EncodeToString(hash[:])+spoolRecordExtension)
}

func validateEventSpoolRecord(record EventSpoolRecord) error {
	if record.Version != spoolRecordVersion || record.Identity.validate() != nil || !validRuntimeID(record.ClientEventID) || record.ClientEventSeq <= 0 || !validRuntimeID(record.EventType) || record.CreatedAt.IsZero() {
		return ErrRuntimeRecordCorrupt
	}
	if len(record.Payload) > spoolPayloadMax || !json.Valid(record.Payload) {
		return ErrRuntimeRecordCorrupt
	}
	return nil
}

func validateResultSpoolRecord(record ResultSpoolRecord) error {
	if record.Version != spoolRecordVersion || record.Identity.validate() != nil || !validRuntimeID(record.ResultID) || record.FinalClientEventSeq < 0 || !validRuntimeID(record.Status) || record.CreatedAt.IsZero() {
		return ErrRuntimeRecordCorrupt
	}
	if len(record.Payload) > spoolPayloadMax || !json.Valid(record.Payload) {
		return ErrRuntimeRecordCorrupt
	}
	return nil
}

func eventSequenceACKed(record AssignmentJournalRecord, sequence int64) bool {
	if sequence <= record.AckedClientEventSeq {
		return true
	}
	index := sort.Search(len(record.AckedOutOfOrderEventSeqs), func(index int) bool {
		return record.AckedOutOfOrderEventSeqs[index] >= sequence
	})
	return index < len(record.AckedOutOfOrderEventSeqs) && record.AckedOutOfOrderEventSeqs[index] == sequence
}

func applyEventACK(record *AssignmentJournalRecord, sequence int64) {
	if sequence == record.AckedClientEventSeq+1 {
		record.AckedClientEventSeq = sequence
		remaining := record.AckedOutOfOrderEventSeqs[:0]
		for _, candidate := range record.AckedOutOfOrderEventSeqs {
			if candidate == record.AckedClientEventSeq+1 {
				record.AckedClientEventSeq = candidate
				continue
			}
			remaining = append(remaining, candidate)
		}
		record.AckedOutOfOrderEventSeqs = append([]int64(nil), remaining...)
		return
	}
	record.AckedOutOfOrderEventSeqs = sortedUniqueSequences(append(record.AckedOutOfOrderEventSeqs, sequence))
}

func readBoundedFile(path string, maximum int64) ([]byte, error) {
	info, err := os.Lstat(path)
	if err != nil {
		return nil, err
	}
	if !info.Mode().IsRegular() || info.Size() <= 0 || info.Size() > maximum || !runtimeFileModeIsPrivate(info.Mode()) {
		return nil, ErrRuntimeRecordCorrupt
	}
	return os.ReadFile(path)
}

func cloneRawMessage(value json.RawMessage) json.RawMessage {
	return append(json.RawMessage(nil), value...)
}

func normalizeRawMessage(value json.RawMessage) json.RawMessage {
	if len(value) == 0 {
		return json.RawMessage("null")
	}
	return cloneRawMessage(value)
}

func cloneEventRecord(record EventSpoolRecord) EventSpoolRecord {
	record.Payload = cloneRawMessage(record.Payload)
	return record
}

func cloneResultRecord(record ResultSpoolRecord) ResultSpoolRecord {
	record.Payload = cloneRawMessage(record.Payload)
	return record
}

func cloneEventSequenceIndex(index map[string]map[int64]string) map[string]map[int64]string {
	cloned := make(map[string]map[int64]string, len(index))
	for attemptID, values := range index {
		cloned[attemptID] = make(map[int64]string, len(values))
		for sequence, eventID := range values {
			cloned[attemptID][sequence] = eventID
		}
	}
	return cloned
}
