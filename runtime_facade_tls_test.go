package openlinker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestRuntimeHTTPClientRejectsMissingFilesKeyMismatchAndInvalidCA(t *testing.T) {
	t.Parallel()
	_, err := NewRuntimeHTTPClient(RuntimeTLSConfig{})
	var configErr *RuntimeConfigError
	if !errors.As(err, &configErr) {
		t.Fatalf("missing file error = %T %v", err, err)
	}
	for _, key := range []string{EnvNodeCertFile, EnvNodeKeyFile, EnvRuntimeCAFile} {
		if !strings.Contains(err.Error(), key) {
			t.Fatalf("missing file error %q does not contain %s", err, key)
		}
	}

	caPEM, _, clientCertPEM, _ := runtimeTestPKI(t)
	_, _, _, otherClientKeyPEM := runtimeTestPKI(t)
	directory := t.TempDir()
	certPath := filepath.Join(directory, "client.crt")
	keyPath := filepath.Join(directory, "client.key")
	caPath := filepath.Join(directory, "ca.crt")
	if err = os.WriteFile(certPath, clientCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, otherClientKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewRuntimeHTTPClient(RuntimeTLSConfig{
		CertificateFile: certPath, PrivateKeyFile: keyPath, CAFile: caPath,
	}); err == nil || !strings.Contains(strings.ToLower(err.Error()), "private key") {
		t.Fatalf("key mismatch error = %v", err)
	}

	_, _, clientCertPEM, clientKeyPEM := runtimeTestPKI(t)
	if err = os.WriteFile(certPath, clientCertPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(keyPath, clientKeyPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	if err = os.WriteFile(caPath, []byte("not a PEM certificate"), 0o600); err != nil {
		t.Fatal(err)
	}
	if _, err = NewRuntimeHTTPClient(RuntimeTLSConfig{
		CertificateFile: certPath, PrivateKeyFile: keyPath, CAFile: caPath,
	}); err == nil || !strings.Contains(err.Error(), "valid PEM") {
		t.Fatalf("invalid CA error = %v", err)
	}
	if _, err = NewRuntimeHTTPClient(RuntimeTLSConfig{
		CertificateFile: filepath.Join(directory, "missing.crt"), PrivateKeyFile: keyPath, CAFile: caPath,
	}); err == nil || !strings.Contains(err.Error(), "load Runtime Node certificate") {
		t.Fatalf("missing certificate error = %v", err)
	}
}

func TestRuntimeHTTPClientRejectsWrongCAAndServerName(t *testing.T) {
	t.Parallel()
	caPEM, serverCertificate, clientCertPEM, clientKeyPEM := runtimeTestPKI(t)
	wrongCAPEM, _, _, _ := runtimeTestPKI(t)
	clientRoots := x509.NewCertPool()
	if !clientRoots.AppendCertsFromPEM(caPEM) {
		t.Fatal("append client CA")
	}
	server := httptest.NewUnstartedServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusNoContent)
	}))
	server.TLS = &tls.Config{
		MinVersion: tls.VersionTLS13, MaxVersion: tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCertificate}, ClientAuth: tls.RequireAndVerifyClientCert, ClientCAs: clientRoots,
	}
	server.StartTLS()
	defer server.Close()

	directory := t.TempDir()
	certPath := filepath.Join(directory, "client.crt")
	keyPath := filepath.Join(directory, "client.key")
	caPath := filepath.Join(directory, "ca.crt")
	for path, raw := range map[string][]byte{certPath: clientCertPEM, keyPath: clientKeyPEM, caPath: wrongCAPEM} {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}
	client, err := NewRuntimeHTTPClient(RuntimeTLSConfig{
		CertificateFile: certPath, PrivateKeyFile: keyPath, CAFile: caPath, ServerName: "runtime.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Get(server.URL); err == nil || !strings.Contains(strings.ToLower(err.Error()), "unknown authority") {
		t.Fatalf("wrong CA handshake error = %v", err)
	}

	if err = os.WriteFile(caPath, caPEM, 0o600); err != nil {
		t.Fatal(err)
	}
	client, err = NewRuntimeHTTPClient(RuntimeTLSConfig{
		CertificateFile: certPath, PrivateKeyFile: keyPath, CAFile: caPath, ServerName: "wrong.runtime.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	if _, err = client.Get(server.URL); err == nil || !strings.Contains(strings.ToLower(err.Error()), "wrong.runtime.test") {
		t.Fatalf("server name handshake error = %v", err)
	}
}

func TestNativeRunnerExplicitHTTPClientOverridesMTLSFiles(t *testing.T) {
	clearRuntimeConfigEnv(t)
	store, err := OpenFileRuntimeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	explicitClient := &http.Client{Transport: http.DefaultTransport.(*http.Transport).Clone()}
	runner := Native(func(context.Context, NativeRun) (any, error) { return Success(nil), nil }).
		WithRuntimeBase("https://runtime.example.test").
		WithNodeID(testNodeID).
		WithAgentID(testAgentID).
		WithAgentToken("ol_agent_test").
		WithStore(store).
		WithHTTPClient(explicitClient)
	worker, err := runner.buildWorker(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	if worker.runtimeClient == nil || runner.httpClient != explicitClient {
		t.Fatalf("worker runtime=%#v explicit client=%#v", worker.runtimeClient, runner.httpClient)
	}
}
