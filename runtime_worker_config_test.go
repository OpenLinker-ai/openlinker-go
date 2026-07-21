package openlinker

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/pem"
	"errors"
	"math/big"
	"net/http"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestRuntimeWorkerConfigReportsAllMissingValues(t *testing.T) {
	config := RuntimeWorkerConfig{Transport: TransportAuto, Capacity: 1}
	err := config.Validate(true)
	var configErr *RuntimeConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("error = %T %v", err, err)
	}
	for _, key := range []string{EnvNodeID, EnvAgentID, EnvRuntimeBase, EnvAPIBase, EnvAgentToken, EnvRuntimeDataDir} {
		if !strings.Contains(err.Error(), key) {
			t.Fatalf("error %q does not contain %s", err, key)
		}
	}
}

func TestRuntimeWorkerConfigAllowsDiscoveryWithoutMTLS(t *testing.T) {
	config := RuntimeWorkerConfig{
		PlatformURL: "https://api.example.com",
		NodeID:      testNodeID,
		AgentID:     testAgentID,
		AgentToken:  "ol_agent_test",
		DataDir:     t.TempDir(),
		Transport:   TransportAuto,
		Capacity:    1,
	}
	if err := config.Validate(true); err != nil {
		t.Fatalf("discovery-backed token-only config: %v", err)
	}
}

func TestRuntimeWorkerConfigDirectRuntimeStillRequiresMTLS(t *testing.T) {
	config := RuntimeWorkerConfig{
		RuntimeURL: "https://runtime.example.com",
		NodeID:     testNodeID,
		AgentID:    testAgentID,
		AgentToken: "ol_agent_test",
		DataDir:    t.TempDir(),
		Transport:  TransportAuto,
		Capacity:   1,
	}
	err := config.Validate(true)
	if err == nil {
		t.Fatal("direct Runtime config without mTLS unexpectedly succeeded")
	}
	for _, key := range []string{EnvNodeCertFile, EnvNodeKeyFile, EnvRuntimeCAFile} {
		if !strings.Contains(err.Error(), key) {
			t.Fatalf("error %q does not contain %s", err, key)
		}
	}
}

func TestRuntimeWorkerConfigRejectsPartialMTLSWithDiscovery(t *testing.T) {
	config := RuntimeWorkerConfig{
		PlatformURL: "https://api.example.com",
		NodeID:      testNodeID,
		AgentID:     testAgentID,
		AgentToken:  "ol_agent_test",
		DataDir:     t.TempDir(),
		Transport:   TransportAuto,
		Capacity:    1,
		MTLS:        RuntimeMTLSConfig{CertFile: "node.crt"},
	}
	err := config.Validate(true)
	if err == nil || !strings.Contains(err.Error(), "must be configured together") {
		t.Fatalf("partial mTLS error = %v", err)
	}
}

func TestLoadRuntimeWorkerConfigDefaults(t *testing.T) {
	clearRuntimeConfigEnv(t)
	t.Setenv(EnvAPIBase, "https://api.example.com")
	t.Setenv(EnvNodeID, testNodeID)
	t.Setenv(EnvAgentID, testAgentID)
	config, err := LoadRuntimeWorkerConfig()
	if err != nil {
		t.Fatal(err)
	}
	if config.PlatformURL != "https://api.example.com" || config.Transport != TransportAuto || config.Capacity != 1 {
		t.Fatalf("config = %#v", config)
	}
	if want := defaultRuntimeDataDir(testAgentID); config.DataDir != want {
		t.Fatalf("data dir = %q, want %q", config.DataDir, want)
	}
}

func TestLoadRuntimeWorkerConfigSupportsLegacyStatePath(t *testing.T) {
	clearRuntimeConfigEnv(t)
	t.Setenv(EnvRuntimeStatePath, filepath.Join("state", "runtime.json"))
	config, err := LoadRuntimeWorkerConfig()
	if err != nil || config.DataDir != filepath.Join("state", "runtime") {
		t.Fatalf("config=%#v err=%v", config, err)
	}
}

func TestLoadRuntimeWorkerConfigRejectsInvalidTransportOrCapacity(t *testing.T) {
	clearRuntimeConfigEnv(t)
	t.Setenv(EnvRuntimeTransport, "telepathy")
	if _, err := LoadRuntimeWorkerConfig(); err == nil || !strings.Contains(err.Error(), EnvRuntimeTransport) {
		t.Fatalf("transport error = %v", err)
	}
	clearRuntimeConfigEnv(t)
	t.Setenv(EnvRuntimeCapacity, "many")
	if _, err := LoadRuntimeWorkerConfig(); err == nil || !strings.Contains(err.Error(), EnvRuntimeCapacity) {
		t.Fatalf("capacity error = %v", err)
	}
}

func TestNewRuntimeHTTPClientLoadsMTLSFiles(t *testing.T) {
	certFile, keyFile, caFile := writeRuntimeTestCertificates(t)
	client, err := NewRuntimeHTTPClient(RuntimeTLSConfig{
		CertificateFile: certFile, PrivateKeyFile: keyFile, CAFile: caFile, ServerName: "runtime.example.com",
	})
	if err != nil {
		t.Fatal(err)
	}
	transport, ok := client.Transport.(*http.Transport)
	if !ok || transport.TLSClientConfig == nil || transport.TLSClientConfig.ServerName != "runtime.example.com" ||
		len(transport.TLSClientConfig.Certificates) != 1 || transport.TLSClientConfig.RootCAs == nil {
		t.Fatalf("transport = %#v", client.Transport)
	}
}

func TestNewRuntimeFromEnv(t *testing.T) {
	clearRuntimeConfigEnv(t)
	certFile, keyFile, caFile := writeRuntimeTestCertificates(t)
	t.Setenv(EnvRuntimeBase, "https://runtime.example.com")
	t.Setenv(EnvNodeID, testNodeID)
	t.Setenv(EnvAgentID, testAgentID)
	t.Setenv(EnvAgentToken, "ol_agent_test")
	t.Setenv(EnvNodeCertFile, certFile)
	t.Setenv(EnvNodeKeyFile, keyFile)
	t.Setenv(EnvRuntimeCAFile, caFile)
	runtimeClient, err := NewRuntimeFromEnv()
	if err != nil {
		t.Fatal(err)
	}
	if runtimeClient == nil || runtimeClient.client == nil || runtimeClient.client.agentToken != "ol_agent_test" {
		t.Fatalf("runtime = %#v", runtimeClient)
	}
}

func TestNativeRunnerExplicitConfigOverridesEnvironment(t *testing.T) {
	clearRuntimeConfigEnv(t)
	t.Setenv(EnvNodeID, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa")
	t.Setenv(EnvAgentID, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb")
	store, err := OpenFileRuntimeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := Native(func(context.Context, NativeRun) (any, error) { return Success(nil), nil }).
		WithNodeID(testNodeID).WithAgentID(testAgentID).WithStore(store).WithCapacity(3).WithTransportMode(TransportHTTP)
	runner.runtimeClient = newFakeRuntimeClient()
	worker, err := runner.buildWorker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if worker.NodeID != testNodeID || worker.AgentID != testAgentID || worker.Capacity != 3 || worker.Transport != RuntimeTransportPull {
		t.Fatalf("worker = %#v", worker)
	}
}

func TestNativeRunnerBuildsDiscoveryBackedWorkerWithoutMTLS(t *testing.T) {
	clearRuntimeConfigEnv(t)
	store, err := OpenFileRuntimeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	runner := Native(func(context.Context, NativeRun) (any, error) { return Success(nil), nil }).
		WithAPIBase("https://api.example.com").
		WithNodeID(testNodeID).
		WithAgentID(testAgentID).
		WithAgentToken("ol_agent_test").
		WithStore(store)
	worker, err := runner.buildWorker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if worker.PlatformURL != "https://api.example.com" || worker.MTLS.CertFile != "" || worker.MTLS.KeyFile != "" || worker.MTLS.CAFile != "" {
		t.Fatalf("discovery-backed worker = %#v", worker)
	}
}

func clearRuntimeConfigEnv(t *testing.T) {
	t.Helper()
	for _, key := range []string{
		EnvRuntimeBase, EnvAPIBase, EnvNodeID, EnvAgentID, EnvAgentToken, EnvRuntimeTransport,
		EnvRuntimeCapacity, EnvRuntimeDataDir, EnvRuntimeStatePath, EnvNodeCertFile, EnvNodeKeyFile,
		EnvRuntimeCAFile, EnvRuntimeServerName,
	} {
		t.Setenv(key, "")
	}
}

func writeRuntimeTestCertificates(t *testing.T) (string, string, string) {
	t.Helper()
	now := time.Now().UTC()
	caKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	caTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(1), Subject: pkix.Name{CommonName: "OpenLinker test CA"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour), IsCA: true,
		BasicConstraintsValid: true, KeyUsage: x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, &caKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	clientKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		t.Fatal(err)
	}
	clientTemplate := &x509.Certificate{
		SerialNumber: big.NewInt(2), Subject: pkix.Name{CommonName: "OpenLinker test Node"},
		NotBefore: now.Add(-time.Hour), NotAfter: now.Add(time.Hour),
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageClientAuth}, KeyUsage: x509.KeyUsageDigitalSignature,
	}
	clientDER, err := x509.CreateCertificate(rand.Reader, clientTemplate, caTemplate, &clientKey.PublicKey, caKey)
	if err != nil {
		t.Fatal(err)
	}
	dir := t.TempDir()
	certFile, keyFile, caFile := filepath.Join(dir, "node.crt"), filepath.Join(dir, "node.key"), filepath.Join(dir, "runtime-ca.crt")
	writePEMFile(t, certFile, "CERTIFICATE", clientDER, 0o600)
	writePEMFile(t, keyFile, "RSA PRIVATE KEY", x509.MarshalPKCS1PrivateKey(clientKey), 0o600)
	writePEMFile(t, caFile, "CERTIFICATE", caDER, 0o600)
	return certFile, keyFile, caFile
}

func writePEMFile(t *testing.T, path, blockType string, data []byte, mode os.FileMode) {
	t.Helper()
	raw := pem.EncodeToMemory(&pem.Block{Type: blockType, Bytes: data})
	if err := os.WriteFile(path, raw, mode); err != nil {
		t.Fatal(err)
	}
}
