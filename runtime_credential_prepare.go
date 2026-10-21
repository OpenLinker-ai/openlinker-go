package openlinker

import (
	"crypto/rand"
	"crypto/x509"
	"encoding/pem"
	"errors"
	"os"
	"path/filepath"
	"strings"
)

// RuntimeCredentialPreparation selects an existing, stopped installation. The
// explicit Worker ID prevents accidentally preparing a different data directory.
type RuntimeCredentialPreparation struct {
	DataDir  string
	NodeID   string
	AgentID  string
	WorkerID string
	// ConfiguredNodeID is the installation's existing product configuration,
	// independently read by the command. AgentToken is used only when the
	// installation derives its Node ID from that token; neither is persisted here.
	ConfiguredNodeID string
	AgentToken       string
}

// PreparedRuntimeCredential contains public material only. It grants no Core
// permission and does not enroll, rotate, start a Worker, or change its epoch.
type PreparedRuntimeCredential struct {
	NodeID              string `json:"node_id"`
	AgentID             string `json:"agent_id"`
	WorkerID            string `json:"worker_id"`
	SessionEpoch        int64  `json:"session_epoch"`
	PublicKeyThumbprint string `json:"public_key_thumbprint"`
	CSRPEM              string `json:"csr_pem"`
	CertificateSerial   string `json:"certificate_serial,omitempty"`
}

func PrepareRuntimeCredential(options RuntimeCredentialPreparation) (PreparedRuntimeCredential, error) {
	if options.DataDir == "" || !validRuntimeUUID(options.NodeID) || !validRuntimeUUID(options.AgentID) || options.WorkerID == "" {
		return PreparedRuntimeCredential{}, errors.New("existing data directory, Node, Agent, and Worker identities are required")
	}
	expectedNodeID := strings.TrimSpace(options.ConfiguredNodeID)
	if expectedNodeID == "" && strings.TrimSpace(options.AgentToken) != "" {
		expectedNodeID = tokenScopedRuntimeNodeID(strings.TrimSpace(options.AgentToken))
	}
	if expectedNodeID == "" || expectedNodeID != options.NodeID {
		return PreparedRuntimeCredential{}, errors.New("credential preparation Node ID must match the existing installation configuration")
	}
	for _, name := range []string{"", runtimeIdentityFile, runtimeSpoolKeyFile, runtimeCredentialFile} {
		path := filepath.Join(options.DataDir, name)
		if err := verifyRuntimeCredentialOwner(path); err != nil {
			if name == runtimeCredentialFile && errors.Is(err, os.ErrNotExist) {
				continue
			}
			return PreparedRuntimeCredential{}, err
		}
	}
	store, err := openFileRuntimeStore(options.DataDir, false)
	if err != nil {
		return PreparedRuntimeCredential{}, err
	}
	defer store.Close()
	identity := store.Identity()
	if identity.WorkerID != options.WorkerID || identity.SessionEpoch < 1 {
		return PreparedRuntimeCredential{}, errors.New("credential preparation Worker identity mismatch")
	}
	status, err := store.SpoolStatus()
	if err != nil {
		return PreparedRuntimeCredential{}, err
	}
	if !status.Empty {
		return PreparedRuntimeCredential{}, errors.New("credential preparation requires an empty, acknowledged Runtime spool")
	}
	disk, err := loadOrCreateRuntimeCredential(options.DataDir, options.NodeID)
	if err != nil {
		return PreparedRuntimeCredential{}, err
	}
	if disk.NodeID != options.NodeID || (disk.AgentID != "" && disk.AgentID != options.AgentID) {
		return PreparedRuntimeCredential{}, errors.New("credential preparation conflicts with stored identity")
	}
	if disk.AgentID == "" {
		disk.AgentID = options.AgentID
		if err = persistRuntimeCredential(options.DataDir, disk); err != nil {
			return PreparedRuntimeCredential{}, err
		}
	}
	key, err := parseRuntimeCredentialPrivateKey(disk.PrivateKeyPEM)
	if err != nil {
		return PreparedRuntimeCredential{}, err
	}
	csr, err := x509.CreateCertificateRequest(rand.Reader, &x509.CertificateRequest{}, key)
	if err != nil {
		return PreparedRuntimeCredential{}, err
	}
	return PreparedRuntimeCredential{NodeID: disk.NodeID, AgentID: disk.AgentID, WorkerID: identity.WorkerID, SessionEpoch: identity.SessionEpoch,
		PublicKeyThumbprint: disk.PublicKeyThumbprint, CSRPEM: string(pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE REQUEST", Bytes: csr})), CertificateSerial: disk.CertificateSerial}, nil
}
