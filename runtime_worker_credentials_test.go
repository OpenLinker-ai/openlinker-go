package openlinker

import (
	"context"
	"crypto/ecdsa"
	"crypto/elliptic"
	"crypto/rand"
	"crypto/sha256"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/hex"
	"encoding/json"
	"encoding/pem"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeTokenOnlySecuritySkipsAutomaticCredentialManager(t *testing.T) {
	node := &RuntimeWorker{
		AgentID:     testAgentID,
		AgentToken:  "ol_agent_token_only",
		NodeVersion: runtimeWorkerSDKAgent,
		DataDir:     t.TempDir(),
		Capacity:    1,
	}
	connection := runtimeConnectionInformation{
		RuntimeURL:         "https://runtime.example.test",
		MTLSRequired:       false,
		CredentialEndpoint: "not-a-valid-credential-endpoint",
	}

	if err := node.configureRuntimeSecurity(context.Background(), connection); err != nil {
		t.Fatalf("configure token-only security: %v", err)
	}
	if node.MTLS.credentialManager != nil || node.MTLS.tlsConfig != nil {
		t.Fatalf("token-only transport initialized mTLS state: %#v", node.MTLS)
	}
	if !node.MTLS.Disabled {
		t.Fatal("token-only transport did not disable mTLS")
	}
	if node.NodeID != tokenScopedRuntimeNodeID(node.AgentToken) || !validRuntimeUUID(node.NodeID) {
		t.Fatalf("token-only Node ID = %q", node.NodeID)
	}
	if node.NodeID != "d6bb911d-7ad6-528b-9a8e-34e2785975fd" {
		t.Fatalf("cross-SDK token-only Node ID = %q", node.NodeID)
	}
}

func TestRuntimeTokenOnlySecurityRequiresAgentIdentity(t *testing.T) {
	node := &RuntimeWorker{AgentToken: "ol_agent_token_only"}
	err := node.configureRuntimeSecurity(context.Background(), runtimeConnectionInformation{MTLSRequired: false})
	if err == nil || !strings.Contains(err.Error(), "Agent ID is required") {
		t.Fatalf("token-only identity error = %v", err)
	}
}

func TestRuntimeCredentialManagerGeneratesBindsAndRenewsOneKey(t *testing.T) {
	caKey, err := ecdsa.GenerateKey(elliptic.P256(), rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "Runtime test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(365 * 24 * time.Hour),
		IsCA: true, BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	ca, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})
	var calls atomic.Int32
	var firstThumbprint string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("authorization") != "Bearer ol_agent_automatic_test" {
			http.Error(w, "token", http.StatusUnauthorized)
			return
		}
		var body runtimeCredentialIssueRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			http.Error(w, err.Error(), http.StatusBadRequest)
			return
		}
		block, _ := pem.Decode([]byte(body.CSRPEM))
		csr, err := x509.ParseCertificateRequest(block.Bytes)
		if err != nil || csr.CheckSignature() != nil {
			http.Error(w, "csr", http.StatusBadRequest)
			return
		}
		spki := sha256.Sum256(csr.RawSubjectPublicKeyInfo)
		thumbprint := hex.EncodeToString(spki[:])
		if firstThumbprint == "" {
			firstThumbprint = thumbprint
		} else if thumbprint != firstThumbprint {
			http.Error(w, "key changed", http.StatusConflict)
			return
		}
		serial := big.NewInt(int64(calls.Add(1) + 10))
		leafTemplate := &x509.Certificate{
			SerialNumber: serial, Subject: pkix.Name{CommonName: "runtime-node"},
			NotBefore: now.Add(-time.Minute), NotAfter: now.Add(24 * time.Hour),
			KeyUsage: x509.KeyUsageDigitalSignature, ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth},
		}
		leafDER, err := x509.CreateCertificate(rand.Reader, leafTemplate, ca, csr.PublicKey, caKey)
		if err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
			return
		}
		leafPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: leafDER})
		_ = json.NewEncoder(w).Encode(runtimeCredentialIssueResponse{
			NodeID: body.NodeID, AgentID: testAgentID,
			CertificatePEM: string(leafPEM), CertificateChainPEM: string(append(leafPEM, caPEM...)),
			TrustBundlePEM: string(caPEM), CertificateSerial: serial.Text(16),
			PublicKeyThumbprint: thumbprint, NotBefore: leafTemplate.NotBefore,
			NotAfter: leafTemplate.NotAfter, RenewAfter: now.Add(13 * time.Hour), CertificateLifetimeHrs: 24,
		})
	}))
	defer server.Close()

	directory := t.TempDir()
	manager, err := newRuntimeCredentialManager(
		directory, server.URL, "ol_agent_automatic_test", "", "", runtimeWorkerSDKAgent, 1, nil,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = manager.Ensure(t.Context(), false); err != nil {
		t.Fatal(err)
	}
	nodeID, agentID := manager.Identity()
	if !validRuntimeUUID(nodeID) || agentID != testAgentID || calls.Load() != 1 {
		t.Fatalf("identity=%s/%s calls=%d", nodeID, agentID, calls.Load())
	}
	info, err := os.Stat(filepath.Join(directory, runtimeCredentialFile))
	if err != nil || info.Mode().Perm() != 0o600 {
		t.Fatalf("credential file mode=%v err=%v", info.Mode(), err)
	}
	if err = manager.Ensure(t.Context(), false); err != nil || calls.Load() != 1 {
		t.Fatalf("unexpected eager renewal: calls=%d err=%v", calls.Load(), err)
	}
	if err = manager.Ensure(t.Context(), true); err != nil || calls.Load() != 2 {
		t.Fatalf("forced recovery renewal: calls=%d err=%v", calls.Load(), err)
	}
	tlsConfig, err := manager.TLSConfig()
	if err != nil || tlsConfig.GetClientCertificate == nil || tlsConfig.RootCAs == nil {
		t.Fatalf("automatic TLS config=%#v err=%v", tlsConfig, err)
	}
}
