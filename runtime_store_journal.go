package openlinker

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"reflect"
	"sort"
	"strconv"
	"sync"
	"time"
)

const (
	assignmentJournalVersion      = 1
	assignmentJournalWALFile      = "assignment-journal.wal"
	assignmentJournalSnapshotFile = "assignment-journal.snapshot"
	assignmentJournalFrameMax     = 4 << 20
	assignmentJournalSnapshotKind = "assignment_journal_snapshot"
	assignmentJournalEntryKind    = "assignment_journal_entry"
	durableBeforeWALWrite         = "before_wal_write"
)

var assignmentJournalMagic = []byte{'O', 'L', 'J', 'R', 'N', '2', '\r', '\n'}

// AttemptIdentity is the complete durable execution ownership tuple. It binds
// every journal, Event, and Result record to the authenticated RuntimeWorker/Agent and
// the exact offer, lease, fence, worker, and runtime session that received it.
type AttemptIdentity struct {
	NodeID              string `json:"node_id"`
	AgentID             string `json:"agent_id"`
	WorkerID            string `json:"worker_id"`
	RuntimeSessionID    string `json:"runtime_session_id"`
	SessionEpoch        int64  `json:"session_epoch"`
	AssignmentMessageID string `json:"assignment_message_id"`
	RunID               string `json:"run_id"`
	AttemptID           string `json:"attempt_id"`
	OfferID             string `json:"offer_id"`
	LeaseID             string `json:"lease_id"`
	FencingToken        int64  `json:"fencing_token"`
}

func (identity AttemptIdentity) validate() error {
	values := []string{
		identity.NodeID,
		identity.AgentID,
		identity.WorkerID,
		identity.RuntimeSessionID,
		identity.AssignmentMessageID,
		identity.RunID,
		identity.AttemptID,
		identity.OfferID,
		identity.LeaseID,
	}
	for _, value := range values {
		if !validRuntimeID(value) {
			return ErrRuntimeRecordCorrupt
		}
	}
	if identity.SessionEpoch <= 0 || identity.FencingToken <= 0 {
		return ErrRuntimeRecordCorrupt
	}
	return nil
}

type AssignmentState string

const (
	AssignmentStateReceived    AssignmentState = "received"
	AssignmentStateACKSent     AssignmentState = "ack_sent"
	AssignmentStateConfirmed   AssignmentState = "confirmed"
	AssignmentStateStarted     AssignmentState = "started"
	AssignmentStateFinished    AssignmentState = "finished"
	AssignmentStateResultACKed AssignmentState = "result_acked"
	AssignmentStateRejectSent  AssignmentState = "reject_sent"
	AssignmentStateRejected    AssignmentState = "rejected"
	AssignmentStateRevoked     AssignmentState = "revoked"
)

// AssignmentJournalRecord contains digests only. Raw assignment input,
// invocation tokens, and signed contexts are intentionally not accepted by
// this API and therefore cannot leak into its WAL or errors.
type AssignmentJournalRecord struct {
	Identity                 AttemptIdentity `json:"identity"`
	InputDigest              string          `json:"input_digest"`
	SignedContextDigest      string          `json:"signed_context_digest"`
	State                    AssignmentState `json:"state"`
	LastClientEventSeq       int64           `json:"last_client_event_seq"`
	AckedClientEventSeq      int64           `json:"acked_client_event_seq"`
	AckedOutOfOrderEventSeqs []int64         `json:"acked_out_of_order_event_seqs,omitempty"`
	ResultID                 string          `json:"result_id,omitempty"`
	FinalClientEventSeq      int64           `json:"final_client_event_seq,omitempty"`
	UpdatedAt                time.Time       `json:"updated_at"`
}

type assignmentJournalEntry struct {
	Record  AssignmentJournalRecord `json:"record"`
	Deleted bool                    `json:"deleted"`
}

type assignmentJournalWALRecord struct {
	Version  int                    `json:"version"`
	Sequence uint64                 `json:"sequence"`
	Entry    assignmentJournalEntry `json:"entry"`
}

type assignmentJournalSnapshot struct {
	Version      int                      `json:"version"`
	WorkerID     string                   `json:"worker_id"`
	LastSequence uint64                   `json:"last_sequence"`
	Entries      []assignmentJournalEntry `json:"entries"`
}

// FileRuntimeStore owns the data-directory process lock for its lifetime.
// All mutation methods serialize locally; the OS lock excludes other RuntimeWorker
// processes. Call Close to release it.
type FileRuntimeStore struct {
	mu sync.Mutex

	dataDir           string
	dataLock          *dataDirLock
	key               []byte
	workerID          string
	identity          RuntimeIdentity
	journal           *os.File
	sequence          uint64
	entries           map[string]assignmentJournalEntry
	attempts          map[string]string
	events            map[string]EventSpoolRecord
	eventSeqs         map[string]map[int64]string
	results           map[string]ResultSpoolRecord
	payloads          map[string]DurableAssignmentPayload
	spoolSizes        map[string]int64
	spoolBytes        int64
	spoolRecords      int64
	spoolMaxBytes     int64
	spoolMaxRecords   int64
	spoolReserveBytes int64
	diskAvailable     func(string) (int64, error)

	closed   bool
	poisoned error
	hook     durableHook
}

func OpenFileRuntimeStore(dataDir string) (_ *FileRuntimeStore, retErr error) {
	absDir, err := filepath.Abs(dataDir)
	if err != nil {
		return nil, fmt.Errorf("resolve runtime data directory: %w", err)
	}
	if err := ensurePrivateDataDir(absDir); err != nil {
		return nil, err
	}
	lock, err := openDataDirLock(absDir)
	if err != nil {
		return nil, err
	}
	defer func() {
		if retErr != nil {
			_ = lock.close()
		}
	}()
	if err := cleanupDurableTemps(absDir); err != nil {
		return nil, fmt.Errorf("clean incomplete durable writes: %w", err)
	}

	durableRecordsExist, err := runtimeDurableRecordsExist(absDir)
	if err != nil {
		return nil, err
	}
	identityDisk, err := loadRuntimeIdentity(absDir, durableRecordsExist)
	if err != nil {
		return nil, err
	}
	key, err := loadOrCreateRuntimeSpoolKey(absDir, durableRecordsExist)
	if err != nil {
		return nil, err
	}
	store := &FileRuntimeStore{
		dataDir:           absDir,
		dataLock:          lock,
		key:               key,
		workerID:          identityDisk.WorkerID,
		entries:           make(map[string]assignmentJournalEntry),
		attempts:          make(map[string]string),
		events:            make(map[string]EventSpoolRecord),
		eventSeqs:         make(map[string]map[int64]string),
		results:           make(map[string]ResultSpoolRecord),
		payloads:          make(map[string]DurableAssignmentPayload),
		spoolSizes:        make(map[string]int64),
		spoolMaxBytes:     runtimeSpoolMaximumBytes,
		spoolMaxRecords:   runtimeSpoolMaximumRecords,
		spoolReserveBytes: runtimeSpoolControlReserveBytes,
		diskAvailable:     runtimeDiskAvailableBytes,
	}
	defer func() {
		if retErr != nil {
			if store.journal != nil {
				_ = store.journal.Close()
			}
			zeroBytes(store.key)
		}
	}()
	if err := store.openAndReplayJournal(); err != nil {
		return nil, err
	}
	if err := store.loadSpool(); err != nil {
		return nil, err
	}
	if err := store.reconcileSpoolAndJournalLocked(); err != nil {
		return nil, err
	}
	identity, err := startRuntimeSession(absDir, identityDisk)
	if err != nil {
		return nil, err
	}
	store.identity = identity
	if err := store.writeJournalSnapshotLocked(); err != nil {
		return nil, fmt.Errorf("persist assignment journal snapshot: %w", err)
	}
	return store, nil
}

func (store *FileRuntimeStore) Identity() RuntimeIdentity {
	store.mu.Lock()
	defer store.mu.Unlock()
	return store.identity
}

func (store *FileRuntimeStore) Close() error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if store.closed {
		return nil
	}
	store.closed = true
	var firstErr error
	if store.journal != nil {
		firstErr = store.journal.Close()
		store.journal = nil
	}
	zeroBytes(store.key)
	store.key = nil
	if err := store.dataLock.close(); firstErr == nil {
		firstErr = err
	}
	return firstErr
}

func (store *FileRuntimeStore) CreateAssignment(record AssignmentJournalRecord) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return err
	}
	if err := validateNewAssignment(record, store.identity); err != nil {
		return err
	}
	messageID := record.Identity.AssignmentMessageID
	if existing, ok := store.entries[messageID]; ok {
		if !existing.Deleted && existing.Record.Identity == record.Identity &&
			existing.Record.InputDigest == record.InputDigest &&
			existing.Record.SignedContextDigest == record.SignedContextDigest {
			return nil
		}
		return ErrAssignmentAlreadyExists
	}
	if existingMessageID, ok := store.attempts[record.Identity.AttemptID]; ok && existingMessageID != messageID {
		return ErrAttemptAlreadyExists
	}
	record.State = AssignmentStateReceived
	record.UpdatedAt = time.Now().UTC()
	record.AckedOutOfOrderEventSeqs = nil
	return store.appendJournalLocked(assignmentJournalEntry{Record: record})
}

func (store *FileRuntimeStore) AdvanceAssignment(assignmentMessageID string, next AssignmentState) (AssignmentJournalRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return AssignmentJournalRecord{}, err
	}
	entry, ok := store.entries[assignmentMessageID]
	if !ok || entry.Deleted {
		return AssignmentJournalRecord{}, ErrAssignmentNotFound
	}
	if next == AssignmentStateFinished || next == AssignmentStateResultACKed {
		return AssignmentJournalRecord{}, ErrAssignmentTransition
	}
	if entry.Record.State == next {
		return cloneAssignmentRecord(entry.Record), nil
	}
	if err := validateAssignmentStateTransition(entry.Record.State, next); err != nil {
		return AssignmentJournalRecord{}, err
	}
	nextRecord := cloneAssignmentRecord(entry.Record)
	nextRecord.State = next
	nextRecord.UpdatedAt = time.Now().UTC()
	if err := store.appendJournalLocked(assignmentJournalEntry{Record: nextRecord}); err != nil {
		return AssignmentJournalRecord{}, err
	}
	return cloneAssignmentRecord(nextRecord), nil
}

func (store *FileRuntimeStore) Assignment(assignmentMessageID string) (AssignmentJournalRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return AssignmentJournalRecord{}, err
	}
	entry, ok := store.entries[assignmentMessageID]
	if !ok || entry.Deleted {
		return AssignmentJournalRecord{}, ErrAssignmentNotFound
	}
	return cloneAssignmentRecord(entry.Record), nil
}

func (store *FileRuntimeStore) Assignments() ([]AssignmentJournalRecord, error) {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return nil, err
	}
	records := make([]AssignmentJournalRecord, 0, len(store.entries))
	for _, entry := range store.entries {
		if !entry.Deleted {
			records = append(records, cloneAssignmentRecord(entry.Record))
		}
	}
	sort.Slice(records, func(left, right int) bool {
		return records[left].Identity.AssignmentMessageID < records[right].Identity.AssignmentMessageID
	})
	return records, nil
}

func (store *FileRuntimeStore) DeleteAssignment(assignmentMessageID string) error {
	store.mu.Lock()
	defer store.mu.Unlock()
	if err := store.readyLocked(); err != nil {
		return err
	}
	entry, ok := store.entries[assignmentMessageID]
	if !ok || entry.Deleted {
		return ErrAssignmentNotFound
	}
	switch entry.Record.State {
	case AssignmentStateResultACKed, AssignmentStateRejected, AssignmentStateRevoked:
	default:
		return ErrAssignmentCleanup
	}
	attemptID := entry.Record.Identity.AttemptID
	if len(store.eventSeqs[attemptID]) != 0 {
		return ErrAssignmentCleanup
	}
	if _, ok := store.results[attemptID]; ok {
		return ErrAssignmentCleanup
	}
	entry.Deleted = true
	entry.Record.UpdatedAt = time.Now().UTC()
	if err := store.appendJournalLocked(entry); err != nil {
		return err
	}
	if payload, exists := store.payloads[attemptID]; exists {
		return store.removeAssignmentPayloadLocked(payload)
	}
	return nil
}

func (store *FileRuntimeStore) readyLocked() error {
	if store.closed {
		return ErrRuntimeStoreClosed
	}
	if store.poisoned != nil {
		return fmt.Errorf("%w: %v", ErrRuntimeStorePoisoned, store.poisoned)
	}
	return nil
}

func validateNewAssignment(record AssignmentJournalRecord, identity RuntimeIdentity) error {
	if err := record.Identity.validate(); err != nil {
		return err
	}
	if record.Identity.WorkerID != identity.WorkerID ||
		record.Identity.RuntimeSessionID != identity.RuntimeSessionID ||
		record.Identity.SessionEpoch != identity.SessionEpoch {
		return ErrSpoolRecordConflict
	}
	if !validSHA256Digest(record.InputDigest) || !validSHA256Digest(record.SignedContextDigest) {
		return ErrRuntimeRecordCorrupt
	}
	if record.State != "" && record.State != AssignmentStateReceived {
		return ErrAssignmentTransition
	}
	if record.LastClientEventSeq != 0 || record.AckedClientEventSeq != 0 || len(record.AckedOutOfOrderEventSeqs) != 0 || record.ResultID != "" || record.FinalClientEventSeq != 0 {
		return ErrRuntimeRecordCorrupt
	}
	return nil
}

func validSHA256Digest(value string) bool {
	decoded, err := hex.DecodeString(value)
	return err == nil && len(decoded) == sha256.Size && value == hex.EncodeToString(decoded)
}

func validateAssignmentStateTransition(current, next AssignmentState) error {
	allowed := false
	switch current {
	case AssignmentStateReceived:
		allowed = next == AssignmentStateACKSent || next == AssignmentStateRejectSent || next == AssignmentStateRevoked
	case AssignmentStateACKSent:
		allowed = next == AssignmentStateConfirmed || next == AssignmentStateRevoked
	case AssignmentStateConfirmed:
		allowed = next == AssignmentStateStarted || next == AssignmentStateRevoked
	case AssignmentStateStarted:
		allowed = next == AssignmentStateFinished || next == AssignmentStateRevoked
	case AssignmentStateFinished:
		allowed = next == AssignmentStateResultACKed || next == AssignmentStateRevoked
	case AssignmentStateRejectSent:
		allowed = next == AssignmentStateRejected || next == AssignmentStateRevoked
	}
	if allowed {
		return nil
	}
	if assignmentStateBranch(current) != assignmentStateBranch(next) && assignmentStateBranch(current) != "open" && next != AssignmentStateRevoked {
		return ErrAssignmentBranchConflict
	}
	if assignmentStateRank(next) <= assignmentStateRank(current) {
		return ErrAssignmentStateRegression
	}
	return ErrAssignmentTransition
}

func assignmentStateBranch(state AssignmentState) string {
	switch state {
	case AssignmentStateACKSent, AssignmentStateConfirmed, AssignmentStateStarted, AssignmentStateFinished, AssignmentStateResultACKed:
		return "accept"
	case AssignmentStateRejectSent, AssignmentStateRejected:
		return "reject"
	case AssignmentStateReceived:
		return "open"
	case AssignmentStateRevoked:
		return "revoked"
	default:
		return "invalid"
	}
}

func assignmentStateRank(state AssignmentState) int {
	switch state {
	case AssignmentStateReceived:
		return 1
	case AssignmentStateACKSent, AssignmentStateRejectSent:
		return 2
	case AssignmentStateConfirmed, AssignmentStateRejected:
		return 3
	case AssignmentStateStarted:
		return 4
	case AssignmentStateFinished:
		return 5
	case AssignmentStateResultACKed, AssignmentStateRevoked:
		return 6
	default:
		return -1
	}
}

func (store *FileRuntimeStore) openAndReplayJournal() error {
	path := filepath.Join(store.dataDir, assignmentJournalWALFile)
	if info, err := os.Lstat(path); err == nil {
		if !info.Mode().IsRegular() || !runtimeFileModeIsPrivate(info.Mode()) {
			return &RuntimeRecordError{Kind: "assignment journal", Reason: ErrRuntimeRecordCorrupt}
		}
	} else if !errors.Is(err, os.ErrNotExist) {
		return err
	}
	file, err := os.OpenFile(path, os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return fmt.Errorf("open assignment journal: %w", err)
	}
	store.journal = file
	if err := file.Chmod(0o600); err != nil {
		return err
	}
	info, err := file.Stat()
	if err != nil {
		return err
	}
	if info.Size() == 0 {
		if err := writeFull(file, assignmentJournalMagic); err != nil {
			return err
		}
		if err := file.Sync(); err != nil {
			return err
		}
		if err := syncRuntimeDirectory(store.dataDir); err != nil {
			return err
		}
	}
	snapshot, err := store.loadJournalSnapshot()
	if err != nil {
		return err
	}
	if _, err := file.Seek(0, io.SeekStart); err != nil {
		return err
	}
	magic := make([]byte, len(assignmentJournalMagic))
	if _, err := io.ReadFull(file, magic); err != nil || !reflect.DeepEqual(magic, assignmentJournalMagic) {
		return &RuntimeRecordError{Kind: "assignment journal", Reason: ErrRuntimeRecordCorrupt}
	}
	var entriesAtSnapshot map[string]assignmentJournalEntry
	for {
		var lengthBytes [4]byte
		n, err := io.ReadFull(file, lengthBytes[:])
		if errors.Is(err, io.EOF) && n == 0 {
			break
		}
		if err != nil {
			return &RuntimeRecordError{Kind: "assignment journal", Reason: ErrRuntimeRecordCorrupt}
		}
		length := binary.BigEndian.Uint32(lengthBytes[:])
		if length == 0 || length > assignmentJournalFrameMax {
			return &RuntimeRecordError{Kind: "assignment journal", Reason: ErrRuntimeRecordCorrupt}
		}
		encrypted := make([]byte, length)
		if _, err := io.ReadFull(file, encrypted); err != nil {
			return &RuntimeRecordError{Kind: "assignment journal", Reason: ErrRuntimeRecordCorrupt}
		}
		var checksum [sha256.Size]byte
		if _, err := io.ReadFull(file, checksum[:]); err != nil {
			return &RuntimeRecordError{Kind: "assignment journal", Reason: ErrRuntimeRecordCorrupt}
		}
		actual := sha256.Sum256(encrypted)
		if actual != checksum {
			return &RuntimeRecordError{Kind: "assignment journal", Reason: ErrRuntimeRecordCorrupt}
		}
		var walRecord assignmentJournalWALRecord
		header, err := openRuntimeRecord(store.key, encrypted, assignmentJournalEntryKind, &walRecord)
		if err != nil {
			return &RuntimeRecordError{Kind: "assignment journal", Reason: err}
		}
		if walRecord.Version != assignmentJournalVersion || walRecord.Sequence != store.sequence+1 ||
			!headerMatchesAttempt(header, assignmentJournalEntryKind, walRecord.Entry.Record.Identity.AssignmentMessageID, walRecord.Entry.Record.Identity) {
			return &RuntimeRecordError{Kind: "assignment journal", Reason: ErrRuntimeRecordCorrupt}
		}
		if err := store.applyJournalEntryLocked(walRecord.Entry, true); err != nil {
			return &RuntimeRecordError{Kind: "assignment journal", Reason: err}
		}
		store.sequence = walRecord.Sequence
		if snapshot != nil && store.sequence == snapshot.LastSequence {
			entriesAtSnapshot = cloneJournalEntries(store.entries)
		}
	}
	if snapshot != nil {
		if snapshot.LastSequence > store.sequence {
			return &RuntimeRecordError{Kind: "assignment journal snapshot", Reason: ErrRuntimeRecordCorrupt}
		}
		if snapshot.LastSequence == 0 {
			entriesAtSnapshot = map[string]assignmentJournalEntry{}
		}
		if !reflect.DeepEqual(snapshotEntriesMap(snapshot.Entries), entriesAtSnapshot) {
			return &RuntimeRecordError{Kind: "assignment journal snapshot", Reason: ErrRuntimeRecordCorrupt}
		}
	}
	_, err = file.Seek(0, io.SeekEnd)
	return err
}

func (store *FileRuntimeStore) loadJournalSnapshot() (*assignmentJournalSnapshot, error) {
	path := filepath.Join(store.dataDir, assignmentJournalSnapshotFile)
	raw, err := readBoundedFile(path, 64<<20)
	if errors.Is(err, os.ErrNotExist) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	var snapshot assignmentJournalSnapshot
	header, err := openRuntimeRecord(store.key, raw, assignmentJournalSnapshotKind, &snapshot)
	if err != nil {
		return nil, &RuntimeRecordError{Kind: "assignment journal snapshot", Reason: err}
	}
	if snapshot.Version != assignmentJournalVersion || snapshot.WorkerID != store.workerID ||
		header.WorkerID != store.workerID || header.MessageID != strconv.FormatUint(snapshot.LastSequence, 10) {
		return nil, &RuntimeRecordError{Kind: "assignment journal snapshot", Reason: ErrRuntimeRecordCorrupt}
	}
	if snapshotEntriesMap(snapshot.Entries) == nil {
		return nil, &RuntimeRecordError{Kind: "assignment journal snapshot", Reason: ErrRuntimeRecordCorrupt}
	}
	return &snapshot, nil
}

func (store *FileRuntimeStore) appendJournalLocked(entry assignmentJournalEntry) error {
	if err := store.readyLocked(); err != nil {
		return err
	}
	sequence := store.sequence + 1
	walRecord := assignmentJournalWALRecord{
		Version:  assignmentJournalVersion,
		Sequence: sequence,
		Entry:    entry,
	}
	encrypted, err := sealRuntimeRecord(
		store.key,
		headerForAttempt(assignmentJournalEntryKind, entry.Record.Identity.AssignmentMessageID, entry.Record.Identity),
		walRecord,
	)
	if err != nil {
		return err
	}
	if len(encrypted) > assignmentJournalFrameMax {
		return fmt.Errorf("assignment journal frame exceeds durable limit")
	}
	frame := make([]byte, 4+len(encrypted)+sha256.Size)
	binary.BigEndian.PutUint32(frame[:4], uint32(len(encrypted)))
	copy(frame[4:], encrypted)
	checksum := sha256.Sum256(encrypted)
	copy(frame[4+len(encrypted):], checksum[:])
	if _, err := store.journal.Seek(0, io.SeekEnd); err != nil {
		return err
	}
	if store.hook != nil {
		if err := store.hook(durableBeforeWALWrite, filepath.Join(store.dataDir, assignmentJournalWALFile)); err != nil {
			store.poisoned = err
			return err
		}
	}
	if err := writeFull(store.journal, frame); err != nil {
		store.poisoned = err
		return err
	}
	if err := store.journal.Sync(); err != nil {
		store.poisoned = err
		return err
	}
	if store.hook != nil {
		if err := store.hook(durableAfterWALSync, filepath.Join(store.dataDir, assignmentJournalWALFile)); err != nil {
			store.poisoned = err
			return err
		}
	}
	if err := store.applyJournalEntryLocked(entry, false); err != nil {
		store.poisoned = err
		return err
	}
	store.sequence = sequence
	if err := store.writeJournalSnapshotLocked(); err != nil {
		// The fsynced append-only WAL remains authoritative. Leave a valid older
		// snapshot in place; a reopen will replay and atomically refresh it.
		return nil
	}
	return nil
}

func (store *FileRuntimeStore) applyJournalEntryLocked(entry assignmentJournalEntry, replay bool) error {
	record := entry.Record
	if err := record.Identity.validate(); err != nil {
		return err
	}
	if record.Identity.WorkerID != store.workerID || !validSHA256Digest(record.InputDigest) || !validSHA256Digest(record.SignedContextDigest) {
		return ErrRuntimeRecordCorrupt
	}
	messageID := record.Identity.AssignmentMessageID
	existing, exists := store.entries[messageID]
	if !exists {
		if entry.Deleted || record.UpdatedAt.IsZero() || record.State != AssignmentStateReceived || record.LastClientEventSeq != 0 || record.AckedClientEventSeq != 0 || len(record.AckedOutOfOrderEventSeqs) != 0 || record.ResultID != "" || record.FinalClientEventSeq != 0 {
			return ErrRuntimeRecordCorrupt
		}
		if otherMessageID, exists := store.attempts[record.Identity.AttemptID]; exists && otherMessageID != messageID {
			return ErrAttemptAlreadyExists
		}
		store.entries[messageID] = cloneJournalEntry(entry)
		store.attempts[record.Identity.AttemptID] = messageID
		return nil
	}
	if existing.Deleted {
		return ErrRuntimeRecordCorrupt
	}
	if existing.Record.Identity != record.Identity || existing.Record.InputDigest != record.InputDigest || existing.Record.SignedContextDigest != record.SignedContextDigest {
		return ErrSpoolRecordConflict
	}
	if entry.Deleted {
		if !isAssignmentTerminal(existing.Record.State) {
			return ErrAssignmentCleanup
		}
		store.entries[messageID] = cloneJournalEntry(entry)
		return nil
	}
	if record.State != existing.Record.State {
		if err := validateAssignmentStateTransition(existing.Record.State, record.State); err != nil {
			return err
		}
	}
	if err := validateJournalProgress(existing.Record, record); err != nil {
		return err
	}
	store.entries[messageID] = cloneJournalEntry(entry)
	_ = replay
	return nil
}

func validateJournalProgress(previous, next AssignmentJournalRecord) error {
	if next.UpdatedAt.IsZero() {
		return ErrRuntimeRecordCorrupt
	}
	if next.LastClientEventSeq < 0 || next.AckedClientEventSeq < 0 || next.FinalClientEventSeq < 0 ||
		next.LastClientEventSeq < previous.LastClientEventSeq || next.AckedClientEventSeq < previous.AckedClientEventSeq || next.AckedClientEventSeq > next.LastClientEventSeq {
		return ErrEventSequence
	}
	if next.ResultID != previous.ResultID && previous.ResultID != "" {
		return ErrSpoolRecordConflict
	}
	if next.FinalClientEventSeq != previous.FinalClientEventSeq && previous.ResultID != "" {
		return ErrSpoolRecordConflict
	}
	if next.ResultID != "" && (!validRuntimeID(next.ResultID) || next.FinalClientEventSeq != next.LastClientEventSeq) {
		return ErrRuntimeRecordCorrupt
	}
	switch next.State {
	case AssignmentStateReceived, AssignmentStateACKSent, AssignmentStateConfirmed, AssignmentStateStarted, AssignmentStateRejectSent, AssignmentStateRejected:
		if next.ResultID != "" || next.FinalClientEventSeq != 0 {
			return ErrRuntimeRecordCorrupt
		}
	case AssignmentStateFinished, AssignmentStateResultACKed:
		if next.ResultID == "" {
			return ErrRuntimeRecordCorrupt
		}
	case AssignmentStateRevoked:
		// A revoked Attempt may retain a finished, unacknowledged Result.
	default:
		return ErrRuntimeRecordCorrupt
	}
	seen := make(map[int64]struct{}, len(next.AckedOutOfOrderEventSeqs))
	last := int64(0)
	for _, sequence := range next.AckedOutOfOrderEventSeqs {
		if sequence <= next.AckedClientEventSeq || sequence > next.LastClientEventSeq || sequence <= last {
			return ErrEventSequence
		}
		if _, exists := seen[sequence]; exists {
			return ErrEventSequence
		}
		seen[sequence] = struct{}{}
		last = sequence
	}
	for _, sequence := range previous.AckedOutOfOrderEventSeqs {
		if sequence <= next.AckedClientEventSeq {
			continue
		}
		if _, exists := seen[sequence]; !exists {
			return ErrEventSequence
		}
	}
	return nil
}

func (store *FileRuntimeStore) writeJournalSnapshotLocked() error {
	entries := make([]assignmentJournalEntry, 0, len(store.entries))
	for _, entry := range store.entries {
		entries = append(entries, cloneJournalEntry(entry))
	}
	sort.Slice(entries, func(left, right int) bool {
		return entries[left].Record.Identity.AssignmentMessageID < entries[right].Record.Identity.AssignmentMessageID
	})
	snapshot := assignmentJournalSnapshot{
		Version:      assignmentJournalVersion,
		WorkerID:     store.workerID,
		LastSequence: store.sequence,
		Entries:      entries,
	}
	header := diskRecordHeader{
		Version:   runtimeCryptoVersion,
		Kind:      assignmentJournalSnapshotKind,
		WorkerID:  store.workerID,
		MessageID: strconv.FormatUint(store.sequence, 10),
	}
	raw, err := sealRuntimeRecord(store.key, header, snapshot)
	if err != nil {
		return err
	}
	return atomicWriteDurable(filepath.Join(store.dataDir, assignmentJournalSnapshotFile), raw, 0o600, store.hook)
}

func runtimeDurableRecordsExist(dataDir string) (bool, error) {
	for _, name := range []string{assignmentJournalWALFile, assignmentJournalSnapshotFile} {
		_, err := os.Lstat(filepath.Join(dataDir, name))
		if err == nil {
			return true, nil
		}
		if !errors.Is(err, os.ErrNotExist) {
			return false, err
		}
	}
	for _, subdirectory := range []string{assignmentSpoolDirectory, eventSpoolDirectory, resultSpoolDirectory} {
		entries, err := os.ReadDir(filepath.Join(dataDir, subdirectory))
		if errors.Is(err, os.ErrNotExist) {
			continue
		}
		if err != nil {
			return false, err
		}
		for _, entry := range entries {
			if !entry.IsDir() && filepath.Ext(entry.Name()) == spoolRecordExtension {
				return true, nil
			}
		}
	}
	return false, nil
}

func snapshotEntriesMap(entries []assignmentJournalEntry) map[string]assignmentJournalEntry {
	result := make(map[string]assignmentJournalEntry, len(entries))
	for _, entry := range entries {
		messageID := entry.Record.Identity.AssignmentMessageID
		if messageID == "" {
			return nil
		}
		if _, exists := result[messageID]; exists {
			return nil
		}
		result[messageID] = cloneJournalEntry(entry)
	}
	return result
}

func cloneJournalEntries(entries map[string]assignmentJournalEntry) map[string]assignmentJournalEntry {
	cloned := make(map[string]assignmentJournalEntry, len(entries))
	for key, entry := range entries {
		cloned[key] = cloneJournalEntry(entry)
	}
	return cloned
}

func cloneJournalEntry(entry assignmentJournalEntry) assignmentJournalEntry {
	entry.Record = cloneAssignmentRecord(entry.Record)
	return entry
}

func cloneAssignmentRecord(record AssignmentJournalRecord) AssignmentJournalRecord {
	record.AckedOutOfOrderEventSeqs = append([]int64(nil), record.AckedOutOfOrderEventSeqs...)
	return record
}

func isAssignmentTerminal(state AssignmentState) bool {
	return state == AssignmentStateResultACKed || state == AssignmentStateRejected || state == AssignmentStateRevoked
}

func (store *FileRuntimeStore) setDurableHookForTest(hook durableHook) {
	store.mu.Lock()
	defer store.mu.Unlock()
	store.hook = hook
}

func (store *FileRuntimeStore) assignmentForAttemptLocked(attemptID string) (assignmentJournalEntry, error) {
	messageID, exists := store.attempts[attemptID]
	if !exists {
		return assignmentJournalEntry{}, ErrAssignmentNotFound
	}
	entry, exists := store.entries[messageID]
	if !exists || entry.Deleted {
		return assignmentJournalEntry{}, ErrAssignmentNotFound
	}
	return entry, nil
}

func sortedUniqueSequences(values []int64) []int64 {
	result := append([]int64(nil), values...)
	sort.Slice(result, func(left, right int) bool { return result[left] < result[right] })
	write := 0
	for _, value := range result {
		if write > 0 && result[write-1] == value {
			continue
		}
		result[write] = value
		write++
	}
	return result[:write]
}
