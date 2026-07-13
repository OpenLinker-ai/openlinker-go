package openlinker

import (
	"bytes"
	"encoding/json"
	"errors"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"
)

func TestSpoolBackpressureRecordLimitAndUsageSurviveRestart(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "quota-records")
	store.diskAvailable = func(string) (int64, error) { return 1 << 40, nil }
	if err := store.setSpoolLimitsForTest(1<<30, 5, 16<<20); err != nil {
		t.Fatal(err)
	}
	for index := 1; index <= 4; index++ {
		if _, err := store.AppendEvent(identity, "run.progress", json.RawMessage(`{"step":`+jsonNumber(index)+`}`)); err != nil {
			t.Fatal(err)
		}
	}
	usage, err := store.SpoolUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage.Records != 4 || usage.AcceptingRuns || store.AcceptsNewRuns() {
		t.Fatalf("80%% usage gate = %#v", usage)
	}
	if _, err = store.AppendEvent(identity, "run.progress", json.RawMessage(`{"step":5}`)); err != nil {
		t.Fatalf("hard limit should still admit the fifth durable record: %v", err)
	}
	if _, err = store.AppendEvent(identity, "run.progress", json.RawMessage(`{"step":6}`)); !errors.Is(err, ErrRuntimeSpoolFull) {
		t.Fatalf("sixth record error = %v", err)
	}
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	defer store.Close()
	usage, err = store.SpoolUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage.Records != 5 || usage.Bytes <= 0 {
		t.Fatalf("reloaded spool usage = %#v", usage)
	}
}

func TestSpoolPreservesControlReserveAndUnackedResultAtDiskLimit(t *testing.T) {
	store := openRuntimeStoreForTest(t, t.TempDir())
	defer store.Close()
	identity := persistStartedAssignmentForTest(t, store, "quota-reserve")
	store.diskAvailable = func(string) (int64, error) { return 1 << 40, nil }
	event, err := store.AppendEvent(identity, "run.progress", json.RawMessage(`{"step":1}`))
	if err != nil {
		t.Fatal(err)
	}
	usage, err := store.SpoolUsage()
	if err != nil {
		t.Fatal(err)
	}
	reserve := int64(1024)
	if err = store.setSpoolLimitsForTest(usage.Bytes+reserve+usage.Bytes/2, 100, reserve); err != nil {
		t.Fatal(err)
	}
	if _, err = store.AppendEvent(identity, "run.progress", json.RawMessage(`{"step":2}`)); !errors.Is(err, ErrRuntimeSpoolFull) {
		t.Fatalf("logical reserve error = %v", err)
	}
	if err = store.AckEvent(identity.AttemptID, event.ClientEventID, event.ClientEventSeq); err != nil {
		t.Fatal(err)
	}
	store.spoolMaxBytes = 1 << 30
	store.spoolReserveBytes = 16 << 20
	result, err := store.AppendResult(identity, "success", json.RawMessage(`{"answer":42}`))
	if err != nil {
		t.Fatal(err)
	}
	store.diskAvailable = func(string) (int64, error) { return store.spoolReserveBytes, nil }
	if store.AcceptsNewRuns() {
		t.Fatal("disk reserve exhaustion must stop new Runs")
	}
	replayed, err := store.PendingResult(identity.AttemptID)
	if err != nil || replayed.ResultID != result.ResultID {
		t.Fatalf("unacknowledged Result was not preserved: %#v, %v", replayed, err)
	}
	if _, err = store.SpoolUsage(); err != nil {
		t.Fatal(err)
	}
}

func TestEventSpoolKeepsStableIDsAndMonotonicSequenceAcrossRestart(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "events")

	first, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"step":1}`))
	if err != nil {
		t.Fatal(err)
	}
	second, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"step":2}`))
	if err != nil {
		t.Fatal(err)
	}
	if first.ClientEventID == second.ClientEventID || first.ClientEventSeq != 1 || second.ClientEventSeq != 2 {
		t.Fatalf("unexpected stable event identities: first=%#v second=%#v", first, second)
	}
	if err := store.AckEvent(identity.AttemptID, second.ClientEventID, second.ClientEventSeq); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	pending, err := store.PendingEvents(identity.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ClientEventID != first.ClientEventID {
		t.Fatalf("pending after restart = %#v", pending)
	}
	if err := store.AckEvent(identity.AttemptID, first.ClientEventID, first.ClientEventSeq); err != nil {
		t.Fatal(err)
	}
	third, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"step":3}`))
	if err != nil {
		t.Fatal(err)
	}
	if third.ClientEventSeq != 3 {
		t.Fatalf("event sequence after acknowledged restart = %d, want 3", third.ClientEventSeq)
	}
}

func TestResultSpoolPersistsUntilBusinessACK(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "result")
	event, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"done":true}`))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.AckEvent(identity.AttemptID, event.ClientEventID, event.ClientEventSeq); err != nil {
		t.Fatal(err)
	}
	result, err := store.AppendResult(identity, "success", json.RawMessage(`{"answer":42}`))
	if err != nil {
		t.Fatal(err)
	}
	if result.FinalClientEventSeq != 1 {
		t.Fatalf("final client event sequence = %d, want 1", result.FinalClientEventSeq)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	replayed, err := store.PendingResult(identity.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ResultID != result.ResultID || !bytes.Equal(replayed.Payload, result.Payload) {
		t.Fatalf("replayed result differs: got=%#v want=%#v", replayed, result)
	}
	if err := store.AckResult(identity.AttemptID, result.ResultID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.PendingResult(identity.AttemptID); !errors.Is(err, ErrSpoolRecordNotFound) {
		t.Fatalf("pending result after ACK error = %v", err)
	}
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != AssignmentStateResultACKed || record.ResultID != result.ResultID {
		t.Fatalf("journal after result ACK = %#v", record)
	}
	if err := store.DeleteAssignment(identity.AssignmentMessageID); err != nil {
		t.Fatalf("delete fully acknowledged assignment: %v", err)
	}
}

func TestSpoolCannotStartBeforeAssignmentConfirmed(t *testing.T) {
	store := openRuntimeStoreForTest(t, t.TempDir())
	identity := testAttemptIdentity(store.Identity(), "confirm-gate")
	if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateACKSent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendEvent(identity, "progress", json.RawMessage(`{}`)); !errors.Is(err, ErrAssignmentTransition) {
		t.Fatalf("event before confirmed/start error = %v", err)
	}
	if _, err := store.AppendResult(identity, "failed", json.RawMessage(`{}`)); !errors.Is(err, ErrAssignmentTransition) {
		t.Fatalf("result before confirmed/start error = %v", err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateConfirmed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendEvent(identity, "progress", json.RawMessage(`{}`)); !errors.Is(err, ErrAssignmentTransition) {
		t.Fatalf("event before started error = %v", err)
	}
}

func TestEventRenameCrashRecoversDurableRecordAndCounter(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "event-rename")
	injected := errors.New("simulated crash after rename")
	store.setDurableHookForTest(func(point, path string) error {
		if point == durableAfterRename && strings.HasSuffix(path, spoolRecordExtension) {
			return injected
		}
		return nil
	})
	written, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"durable":true}`))
	if !errors.Is(err, injected) {
		t.Fatalf("append error = %v, want injected crash", err)
	}
	if written.ClientEventID == "" {
		t.Fatal("append did not return its stable event ID")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	pending, err := store.PendingEvents(identity.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 1 || pending[0].ClientEventID != written.ClientEventID || pending[0].ClientEventSeq != 1 {
		t.Fatalf("recovered pending events = %#v", pending)
	}
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if record.LastClientEventSeq != 1 {
		t.Fatalf("reconciled last sequence = %d, want 1", record.LastClientEventSeq)
	}
}

func TestEventCrashBeforeRenameDoesNotCreateSendableRecord(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "event-before-rename")
	injected := errors.New("simulated crash before rename")
	store.setDurableHookForTest(func(point, path string) error {
		if point == durableAfterFileSync && strings.HasSuffix(path, spoolRecordExtension) {
			return injected
		}
		return nil
	})
	if _, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"not_committed":true}`)); !errors.Is(err, injected) {
		t.Fatalf("append error = %v, want injected crash", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	pending, err := store.PendingEvents(identity.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("pre-rename record became sendable: %#v", pending)
	}
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if record.LastClientEventSeq != 0 {
		t.Fatalf("event counter advanced before rename: %d", record.LastClientEventSeq)
	}
}

func TestEventCrashBeforeWriteDoesNotCreateDurableRecord(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "event-before-write")
	injected := errors.New("simulated crash before write")
	store.setDurableHookForTest(func(point, path string) error {
		if point == durableBeforeFileWrite && strings.HasSuffix(path, spoolRecordExtension) {
			return injected
		}
		return nil
	})
	if _, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"not_written":true}`)); !errors.Is(err, injected) {
		t.Fatalf("append error = %v, want injected crash", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openRuntimeStoreForTest(t, dataDir)
	defer store.Close()
	pending, err := store.PendingEvents(identity.AttemptID)
	if err != nil || len(pending) != 0 {
		t.Fatalf("pre-write record became durable: %#v, %v", pending, err)
	}
}

func TestEventDirectorySyncCrashRecoversCommittedRecord(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "event-dir-sync")
	injected := errors.New("simulated crash after directory sync")
	store.setDurableHookForTest(func(point, path string) error {
		if point == durableAfterDirSync && strings.HasSuffix(path, spoolRecordExtension) {
			return injected
		}
		return nil
	})
	written, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"committed":true}`))
	if !errors.Is(err, injected) {
		t.Fatalf("append error = %v, want injected crash", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openRuntimeStoreForTest(t, dataDir)
	defer store.Close()
	pending, err := store.PendingEvents(identity.AttemptID)
	if err != nil || len(pending) != 1 || pending[0].ClientEventID != written.ClientEventID {
		t.Fatalf("directory-synced record was not recovered: %#v, %v", pending, err)
	}
}

func TestStartedWALCrashBoundaryDistinguishesBeforeAndAfterCommit(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		point     string
		wantState AssignmentState
	}{
		{name: "before_wal_write", point: durableBeforeWALWrite, wantState: AssignmentStateConfirmed},
		{name: "after_wal_sync", point: durableAfterWALSync, wantState: AssignmentStateStarted},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dataDir := t.TempDir()
			store := openRuntimeStoreForTest(t, dataDir)
			identity := testAttemptIdentity(store.Identity(), testCase.name)
			if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
				t.Fatal(err)
			}
			for _, state := range []AssignmentState{AssignmentStateACKSent, AssignmentStateConfirmed} {
				if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, state); err != nil {
					t.Fatal(err)
				}
			}
			injected := errors.New("simulated crash at started WAL boundary")
			store.setDurableHookForTest(func(point, _ string) error {
				if point == testCase.point {
					return injected
				}
				return nil
			})
			if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateStarted); !errors.Is(err, injected) {
				t.Fatalf("started transition error = %v", err)
			}
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			store = openRuntimeStoreForTest(t, dataDir)
			defer store.Close()
			record, err := store.Assignment(identity.AssignmentMessageID)
			if err != nil || record.State != testCase.wantState {
				t.Fatalf("recovered state = %s, want %s, err=%v", record.State, testCase.wantState, err)
			}
		})
	}
}

func TestResultRenameCrashPromotesJournalOnRecovery(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "result-rename")
	injected := errors.New("simulated crash after rename")
	store.setDurableHookForTest(func(point, path string) error {
		if point == durableAfterRename && strings.HasSuffix(path, spoolRecordExtension) {
			return injected
		}
		return nil
	})
	written, err := store.AppendResult(identity, "success", json.RawMessage(`{"durable":true}`))
	if !errors.Is(err, injected) {
		t.Fatalf("append result error = %v, want injected crash", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	replayed, err := store.PendingResult(identity.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if replayed.ResultID != written.ResultID {
		t.Fatalf("recovered result ID = %q, want %q", replayed.ResultID, written.ResultID)
	}
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != AssignmentStateFinished || record.ResultID != written.ResultID {
		t.Fatalf("recovered journal = %#v", record)
	}
}

func TestEventACKCrashNeverLosesUnacknowledgedState(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "event-ack-crash")
	event, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("simulated crash after ACK WAL fsync")
	store.setDurableHookForTest(func(point, _ string) error {
		if point == durableAfterWALSync {
			return injected
		}
		return nil
	})
	if err := store.AckEvent(identity.AttemptID, event.ClientEventID, event.ClientEventSeq); !errors.Is(err, injected) {
		t.Fatalf("ACK error = %v, want injected crash", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	pending, err := store.PendingEvents(identity.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if len(pending) != 0 {
		t.Fatalf("business-ACKed event was not cleaned on recovery: %#v", pending)
	}
}

func TestResultACKCrashCleansOnlyAfterDurableBusinessACK(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "result-ack-crash")
	result, err := store.AppendResult(identity, "success", json.RawMessage(`{"ok":true}`))
	if err != nil {
		t.Fatal(err)
	}
	injected := errors.New("simulated crash after Result ACK WAL fsync")
	store.setDurableHookForTest(func(point, _ string) error {
		if point == durableAfterWALSync {
			return injected
		}
		return nil
	})
	if err := store.AckResult(identity.AttemptID, result.ResultID); !errors.Is(err, injected) {
		t.Fatalf("result ACK error = %v, want injected crash", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	if _, err := store.PendingResult(identity.AttemptID); !errors.Is(err, ErrSpoolRecordNotFound) {
		t.Fatalf("ACKed result remained pending: %v", err)
	}
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != AssignmentStateResultACKed {
		t.Fatalf("recovered state = %s, want result_acked", record.State)
	}
}

func TestSpoolDetectsTruncatedAndCorruptRecords(t *testing.T) {
	for _, testCase := range []struct {
		name   string
		mutate func(*testing.T, string)
	}{
		{
			name: "truncated",
			mutate: func(t *testing.T, path string) {
				info, err := os.Stat(path)
				if err != nil {
					t.Fatal(err)
				}
				if err := os.Truncate(path, info.Size()/2); err != nil {
					t.Fatal(err)
				}
			},
		},
		{name: "corrupt", mutate: corruptFileByteForTest},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			dataDir := t.TempDir()
			store := openRuntimeStoreForTest(t, dataDir)
			identity := persistStartedAssignmentForTest(t, store, testCase.name)
			event, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"secret":"safe"}`))
			if err != nil {
				t.Fatal(err)
			}
			path := store.spoolRecordPath(eventSpoolKind, identity.AttemptID, event.ClientEventID)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			testCase.mutate(t, path)
			if _, err := OpenFileRuntimeStore(dataDir); !errors.Is(err, ErrRuntimeRecordCorrupt) {
				t.Fatalf("open damaged spool error = %v", err)
			}
		})
	}
}

func TestDurableStoreDoesNotPersistSensitivePlaintext(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "secrets")
	secret := "super-secret-invocation-token-do-not-log"
	if _, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"token":"`+secret+`"}`)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AppendResult(identity, "success", json.RawMessage(`{"output":"`+secret+`"}`)); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	if err := filepath.WalkDir(dataDir, func(path string, entry os.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			return nil
		}
		raw, err := os.ReadFile(path)
		if err != nil {
			return err
		}
		if bytes.Contains(raw, []byte(secret)) {
			t.Fatalf("sensitive plaintext persisted in %s", filepath.Base(path))
		}
		return nil
	}); err != nil {
		t.Fatal(err)
	}
}

func TestConcurrentEventAppendsAllocateUniqueSequence(t *testing.T) {
	store := openRuntimeStoreForTest(t, t.TempDir())
	identity := persistStartedAssignmentForTest(t, store, "concurrent-events")
	const count = 32
	results := make(chan EventSpoolRecord, count)
	errorsChannel := make(chan error, count)
	var wait sync.WaitGroup
	for index := 0; index < count; index++ {
		wait.Add(1)
		go func(index int) {
			defer wait.Done()
			record, err := store.AppendEvent(identity, "progress", json.RawMessage(`{"index":`+jsonNumber(index)+`}`))
			if err != nil {
				errorsChannel <- err
				return
			}
			results <- record
		}(index)
	}
	wait.Wait()
	close(results)
	close(errorsChannel)
	for err := range errorsChannel {
		t.Fatalf("concurrent append: %v", err)
	}
	seenIDs := make(map[string]struct{}, count)
	seenSequences := make(map[int64]struct{}, count)
	for record := range results {
		seenIDs[record.ClientEventID] = struct{}{}
		seenSequences[record.ClientEventSeq] = struct{}{}
	}
	if len(seenIDs) != count || len(seenSequences) != count {
		t.Fatalf("unique IDs=%d sequences=%d, want %d", len(seenIDs), len(seenSequences), count)
	}
	for sequence := int64(1); sequence <= count; sequence++ {
		if _, exists := seenSequences[sequence]; !exists {
			t.Fatalf("missing sequence %d", sequence)
		}
	}
}

func jsonNumber(value int) string {
	const digits = "0123456789"
	if value == 0 {
		return "0"
	}
	var buffer [20]byte
	index := len(buffer)
	for value > 0 {
		index--
		buffer[index] = digits[value%10]
		value /= 10
	}
	return string(buffer[index:])
}
