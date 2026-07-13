package openlinker

import (
	"bufio"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestRuntimeIdentityPersistsWorkerAndRotatesSession(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	first := store.Identity()
	if first.WorkerID == "" || first.RuntimeSessionID == "" || first.SessionEpoch != 1 {
		t.Fatalf("unexpected first identity: %#v", first)
	}
	if err := store.Close(); err != nil {
		t.Fatalf("close first store: %v", err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	second := store.Identity()
	if second.WorkerID != first.WorkerID {
		t.Fatalf("worker ID changed: %q != %q", second.WorkerID, first.WorkerID)
	}
	if second.RuntimeSessionID == first.RuntimeSessionID {
		t.Fatal("runtime session ID did not rotate")
	}
	if second.SessionEpoch != first.SessionEpoch+1 {
		t.Fatalf("session epoch = %d, want %d", second.SessionEpoch, first.SessionEpoch+1)
	}

	for _, name := range []string{runtimeIdentityFile, runtimeSpoolKeyFile} {
		info, err := os.Stat(filepath.Join(dataDir, name))
		if err != nil {
			t.Fatalf("stat %s: %v", name, err)
		}
		if got := info.Mode().Perm(); got != 0o600 {
			t.Fatalf("%s mode = %o, want 600", name, got)
		}
	}
	if info, err := os.Stat(dataDir); err != nil || info.Mode().Perm() != 0o700 {
		t.Fatalf("data directory is not mode 0700: info=%v err=%v", info, err)
	}
}

func TestRuntimeDataDirLockRejectsSecondOpen(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	defer store.Close()

	_, err := OpenFileRuntimeStore(dataDir)
	if !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("second open error = %v, want %v", err, ErrDataDirLocked)
	}
}

func TestRuntimeDataDirProcessLock(t *testing.T) {
	if os.Getenv("OPENLINKER_TEST_LOCK_HELPER") == "1" {
		store, err := OpenFileRuntimeStore(os.Getenv("OPENLINKER_TEST_LOCK_DIR"))
		if err != nil {
			fmt.Fprintf(os.Stdout, "ERROR %v\n", err)
			os.Exit(2)
		}
		fmt.Fprintln(os.Stdout, "LOCKED")
		_, _ = io.ReadAll(os.Stdin)
		_ = store.Close()
		return
	}

	dataDir := t.TempDir()
	command := exec.Command(os.Args[0], "-test.run=^TestRuntimeDataDirProcessLock$")
	command.Env = append(os.Environ(),
		"OPENLINKER_TEST_LOCK_HELPER=1",
		"OPENLINKER_TEST_LOCK_DIR="+dataDir,
	)
	stdin, err := command.StdinPipe()
	if err != nil {
		t.Fatal(err)
	}
	stdout, err := command.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	command.Stderr = os.Stderr
	if err := command.Start(); err != nil {
		t.Fatal(err)
	}
	line, err := bufio.NewReader(stdout).ReadString('\n')
	if err != nil || line != "LOCKED\n" {
		_ = stdin.Close()
		_ = command.Wait()
		t.Fatalf("lock helper did not acquire lock: line=%q err=%v", line, err)
	}
	if _, err := OpenFileRuntimeStore(dataDir); !errors.Is(err, ErrDataDirLocked) {
		_ = stdin.Close()
		_ = command.Wait()
		t.Fatalf("cross-process open error = %v, want %v", err, ErrDataDirLocked)
	}
	if err := stdin.Close(); err != nil {
		t.Fatal(err)
	}
	if err := command.Wait(); err != nil {
		t.Fatal(err)
	}
}

func openRuntimeStoreForTest(t *testing.T, dataDir string) *FileRuntimeStore {
	t.Helper()
	store, err := OpenFileRuntimeStore(dataDir)
	if err != nil {
		t.Fatalf("open runtime store: %v", err)
	}
	t.Cleanup(func() { _ = store.Close() })
	return store
}

func testAttemptIdentity(identity RuntimeIdentity, suffix string) AttemptIdentity {
	return AttemptIdentity{
		NodeID:              "node-" + suffix,
		AgentID:             "agent-" + suffix,
		WorkerID:            identity.WorkerID,
		RuntimeSessionID:    identity.RuntimeSessionID,
		SessionEpoch:        identity.SessionEpoch,
		AssignmentMessageID: "assignment-" + suffix,
		RunID:               "run-" + suffix,
		AttemptID:           "attempt-" + suffix,
		OfferID:             "offer-" + suffix,
		LeaseID:             "lease-" + suffix,
		FencingToken:        1,
	}
}

func testAssignmentRecord(identity AttemptIdentity) AssignmentJournalRecord {
	return AssignmentJournalRecord{
		Identity:            identity,
		InputDigest:         "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa",
		SignedContextDigest: "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb",
	}
}

func persistStartedAssignmentForTest(t *testing.T, store *FileRuntimeStore, suffix string) AttemptIdentity {
	t.Helper()
	identity := testAttemptIdentity(store.Identity(), suffix)
	if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
		t.Fatalf("create assignment: %v", err)
	}
	for _, state := range []AssignmentState{AssignmentStateACKSent, AssignmentStateConfirmed, AssignmentStateStarted} {
		if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, state); err != nil {
			t.Fatalf("advance assignment to %s: %v", state, err)
		}
	}
	return identity
}
