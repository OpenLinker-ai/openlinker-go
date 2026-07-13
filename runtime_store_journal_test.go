package openlinker

import (
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestAssignmentJournalAcceptPathOnlyMovesForward(t *testing.T) {
	store := openRuntimeStoreForTest(t, t.TempDir())
	identity := testAttemptIdentity(store.Identity(), "forward")
	if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateStarted); !errors.Is(err, ErrAssignmentTransition) {
		t.Fatalf("started before confirmed error = %v", err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateACKSent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateACKSent); err != nil {
		t.Fatalf("exact state replay should be idempotent: %v", err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateRejectSent); !errors.Is(err, ErrAssignmentBranchConflict) {
		t.Fatalf("accept-to-reject error = %v, want branch conflict", err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateConfirmed); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateACKSent); !errors.Is(err, ErrAssignmentStateRegression) {
		t.Fatalf("regression error = %v, want state regression", err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateStarted); err != nil {
		t.Fatal(err)
	}
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != AssignmentStateStarted {
		t.Fatalf("state = %s, want started", record.State)
	}
}

func TestAssignmentJournalRejectPathCannotBecomeAccepted(t *testing.T) {
	store := openRuntimeStoreForTest(t, t.TempDir())
	identity := testAttemptIdentity(store.Identity(), "reject")
	if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateRejectSent); err != nil {
		t.Fatal(err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateACKSent); !errors.Is(err, ErrAssignmentBranchConflict) {
		t.Fatalf("reject-to-accept error = %v, want branch conflict", err)
	}
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateRejected); err != nil {
		t.Fatal(err)
	}
	if err := store.DeleteAssignment(identity.AssignmentMessageID); err != nil {
		t.Fatal(err)
	}
	if _, err := store.Assignment(identity.AssignmentMessageID); !errors.Is(err, ErrAssignmentNotFound) {
		t.Fatalf("deleted assignment lookup error = %v", err)
	}
}

func TestAssignmentJournalReplaysAfterRestart(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := persistStartedAssignmentForTest(t, store, "replay")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if record.Identity != identity || record.State != AssignmentStateStarted {
		t.Fatalf("replayed record = %#v", record)
	}
}

func TestAssignmentJournalWALSyncIsRecoveryPoint(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := testAttemptIdentity(store.Identity(), "wal-crash")
	if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
		t.Fatal(err)
	}
	injected := errors.New("simulated process crash")
	store.setDurableHookForTest(func(point, path string) error {
		if point == durableAfterWALSync {
			return injected
		}
		return nil
	})
	if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateACKSent); !errors.Is(err, injected) {
		t.Fatalf("advance error = %v, want injected crash", err)
	}
	if _, err := store.Assignment(identity.AssignmentMessageID); !errors.Is(err, ErrRuntimeStorePoisoned) {
		t.Fatalf("poisoned store error = %v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if record.State != AssignmentStateACKSent {
		t.Fatalf("recovered state = %s, want ack_sent", record.State)
	}
}

func TestAssignmentJournalDetectsTruncation(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	persistStartedAssignmentForTest(t, store, "truncated")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(dataDir, assignmentJournalWALFile)
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if err := os.Truncate(path, info.Size()-1); err != nil {
		t.Fatal(err)
	}
	if _, err := OpenFileRuntimeStore(dataDir); !errors.Is(err, ErrRuntimeRecordCorrupt) {
		t.Fatalf("open truncated WAL error = %v", err)
	}
}

func TestAssignmentJournalDetectsSnapshotCorruption(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	persistStartedAssignmentForTest(t, store, "snapshot-corrupt")
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	corruptFileByteForTest(t, filepath.Join(dataDir, assignmentJournalSnapshotFile))
	if _, err := OpenFileRuntimeStore(dataDir); !errors.Is(err, ErrRuntimeRecordCorrupt) {
		t.Fatalf("open corrupt snapshot error = %v", err)
	}
}

func TestRuntimeStoreDetectsIdentityCorruption(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	corruptFileByteForTest(t, filepath.Join(dataDir, runtimeIdentityFile))
	if _, err := OpenFileRuntimeStore(dataDir); !errors.Is(err, ErrRuntimeIdentityCorrupt) {
		t.Fatalf("open corrupt identity error = %v", err)
	}
}

func TestRuntimeStoreFailsClosedWhenKeyOrIdentityIsLost(t *testing.T) {
	for _, missing := range []string{runtimeSpoolKeyFile, runtimeIdentityFile} {
		t.Run(missing, func(t *testing.T) {
			dataDir := t.TempDir()
			store := openRuntimeStoreForTest(t, dataDir)
			persistStartedAssignmentForTest(t, store, missing)
			if err := store.Close(); err != nil {
				t.Fatal(err)
			}
			if err := os.Remove(filepath.Join(dataDir, missing)); err != nil {
				t.Fatal(err)
			}
			_, err := OpenFileRuntimeStore(dataDir)
			want := ErrRuntimeSpoolKeyMissing
			if missing == runtimeIdentityFile {
				want = ErrRuntimeIdentityMissing
			}
			if !errors.Is(err, want) {
				t.Fatalf("open error = %v, want %v", err, want)
			}
		})
	}
}

func corruptFileByteForTest(t *testing.T, path string) {
	t.Helper()
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if len(raw) < 8 {
		t.Fatalf("file %s too short to corrupt", path)
	}
	raw[len(raw)/2] ^= 0x5a
	if err := os.WriteFile(path, raw, 0o600); err != nil {
		t.Fatal(err)
	}
}
