package openlinker

import (
	"bytes"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"testing"
)

func TestPrepareRuntimeCredentialPreservesIdentityAndRequiresStoppedEmptyStore(t *testing.T) {
	dir := t.TempDir()
	store := openRuntimeStoreForTest(t, dir)
	identity := store.Identity()
	options := RuntimeCredentialPreparation{DataDir: dir, NodeID: testNodeID, ConfiguredNodeID: testNodeID, AgentID: testAgentID, WorkerID: identity.WorkerID}
	if _, err := PrepareRuntimeCredential(options); !errors.Is(err, ErrDataDirLocked) {
		t.Fatalf("live store error=%v", err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	before := map[string][]byte{}
	for _, file := range []string{runtimeIdentityFile, runtimeSpoolKeyFile} {
		raw, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatal(err)
		}
		before[file] = raw
	}
	prepared, err := PrepareRuntimeCredential(options)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.NodeID != testNodeID || prepared.AgentID != testAgentID || prepared.WorkerID != identity.WorkerID || prepared.SessionEpoch != identity.SessionEpoch {
		t.Fatalf("identity changed: %+v", prepared)
	}
	block, _ := pem.Decode([]byte(prepared.CSRPEM))
	if block == nil {
		t.Fatal("missing public CSR")
	}
	csr, err := x509.ParseCertificateRequest(block.Bytes)
	if err != nil || csr.CheckSignature() != nil {
		t.Fatal("invalid proof of possession")
	}
	second, err := PrepareRuntimeCredential(options)
	if err != nil {
		t.Fatal(err)
	}
	if prepared.PublicKeyThumbprint != second.PublicKeyThumbprint {
		t.Fatal("repeat preparation rotated key")
	}
	for file, raw := range before {
		after, err := os.ReadFile(filepath.Join(dir, file))
		if err != nil {
			t.Fatal(err)
		}
		if !bytes.Equal(raw, after) {
			t.Fatalf("preparation changed %s", file)
		}
	}
	disk, err := loadOrCreateRuntimeCredential(dir, testNodeID)
	if err != nil {
		t.Fatal(err)
	}
	if disk.MTLSRequired || disk.CertificateChainPEM != "" {
		t.Fatal("offline preparation cannot pin or claim completed migration")
	}
	wrong := options
	wrong.WorkerID = "wrong-worker"
	if _, err := PrepareRuntimeCredential(wrong); err == nil {
		t.Fatal("wrong Worker accepted")
	}
	store = openRuntimeStoreForTest(t, dir)
	persistStartedAssignmentForTest(t, store, "pending-migration")
	if err = store.Close(); err != nil {
		t.Fatal(err)
	}
	if _, err = PrepareRuntimeCredential(options); err == nil {
		t.Fatal("unacknowledged assignment accepted")
	}
}

func TestPrepareRuntimeCredentialNeverCreatesInstallation(t *testing.T) {
	dir := t.TempDir()
	_, err := PrepareRuntimeCredential(RuntimeCredentialPreparation{DataDir: dir, NodeID: testNodeID, ConfiguredNodeID: testNodeID, AgentID: testAgentID, WorkerID: "not-enrolled"})
	if err == nil {
		t.Fatal("empty directory accepted")
	}
	if _, err = os.Stat(filepath.Join(dir, runtimeCredentialFile)); !errors.Is(err, os.ErrNotExist) {
		t.Fatal("key created for missing installation")
	}
}

func TestPrepareRuntimeCredentialRejectsWrongConfiguredNodeBeforeWriting(t *testing.T) {
	for _, derived := range []bool{false, true} {
		dir := t.TempDir()
		store := openRuntimeStoreForTest(t, dir)
		identity := store.Identity()
		if err := store.Close(); err != nil {
			t.Fatal(err)
		}
		options := RuntimeCredentialPreparation{DataDir: dir, NodeID: testNodeID, AgentID: testAgentID, WorkerID: identity.WorkerID, ConfiguredNodeID: testRunID}
		if derived {
			options.ConfiguredNodeID = ""
			options.AgentToken = "ol_agent_preparation_synthetic"
		}
		if _, err := PrepareRuntimeCredential(options); err == nil {
			t.Fatal("mismatched configured Node accepted")
		}
		if _, err := os.Stat(filepath.Join(dir, runtimeCredentialFile)); !errors.Is(err, os.ErrNotExist) {
			t.Fatal("rejected preparation wrote credentials")
		}
		if derived {
			options.NodeID = tokenScopedRuntimeNodeID(options.AgentToken)
		} else {
			options.NodeID = options.ConfiguredNodeID
		}
		if _, err := PrepareRuntimeCredential(options); err != nil {
			t.Fatal(err)
		}
	}
}
