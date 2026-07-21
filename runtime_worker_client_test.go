package openlinker

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"encoding/json"
	"encoding/pem"
	"io"
	"math/big"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeClientUsesTLS13MTLSAndExactInvocationProof(t *testing.T) {
	caPEM, serverCertificate, clientCertPEM, clientKeyPEM := runtimeTestPKI(t)
	caPool := x509.NewCertPool()
	if !caPool.AppendCertsFromPEM(caPEM) {
		t.Fatal("append test CA")
	}

	const (
		agentToken     = "ol_agent_long_lived_secret"
		invocation     = "ol_inv_v2.header.payload.signature"
		nodeEnvelope   = "ol_ctx_v2.header.payload.signature"
		idempotencyKey = "mtls-child-intent-1"
	)
	var sessionCalls atomic.Int32
	var delegatedCalls atomic.Int32
	handler := http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.TLS == nil || request.TLS.Version != tls.VersionTLS13 || len(request.TLS.PeerCertificates) != 1 || request.TLS.PeerCertificates[0].Subject.CommonName != "agent-node-test" {
			http.Error(w, "mTLS client identity mismatch", http.StatusUnauthorized)
			return
		}
		w.Header().Set("content-type", "application/json")
		switch request.URL.Path {
		case "/api/v1/agent-runtime/sessions":
			sessionCalls.Add(1)
			if request.Header.Get("authorization") != "Bearer "+agentToken {
				http.Error(w, "Agent Token mismatch", http.StatusUnauthorized)
				return
			}
			var hello RuntimeHelloPayload
			if err := json.NewDecoder(request.Body).Decode(&hello); err != nil || hello.NodeID != testNodeID {
				http.Error(w, "hello mismatch", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(RuntimeReadyPayload{
				CoreInstanceID:  testCoreInstanceID,
				AttachmentID:    testAttachmentID,
				Features:        RuntimeRequiredFeatures(),
				OfferTTLSeconds: 30,
				LeaseTTLSeconds: 60,
				DatabaseTime:    time.Now().UTC(),
			})
		case "/api/v1/agent-runtime/call-agent":
			delegatedCalls.Add(1)
			if request.Header.Get("authorization") != "Bearer "+invocation || request.Header.Get("authorization") == "Bearer "+agentToken {
				http.Error(w, "delegated authority mismatch", http.StatusUnauthorized)
				return
			}
			if request.Header.Get("idempotency-key") != idempotencyKey || request.Header.Get("openlinker-invocation-context") != nodeEnvelope {
				http.Error(w, "delegated headers mismatch", http.StatusBadRequest)
				return
			}
			body, err := io.ReadAll(request.Body)
			if err != nil {
				http.Error(w, "read body", http.StatusBadRequest)
				return
			}
			expectedProof, err := BuildRuntimeInvocationProof(invocation, RuntimeInvocationProofRequest{
				Method:         http.MethodPost,
				Path:           "/api/v1/agent-runtime/call-agent",
				IdempotencyKey: idempotencyKey,
				Context:        nodeEnvelope,
				Body:           body,
			})
			if err != nil || request.Header.Get("openlinker-invocation-proof") != expectedProof {
				http.Error(w, "invocation proof mismatch", http.StatusUnauthorized)
				return
			}
			var delegated RuntimeCallAgentRequest
			if err := json.Unmarshal(body, &delegated); err != nil || delegated.TargetAgentID != testTargetAgentID || delegated.Input["question"] != "proof" {
				http.Error(w, "delegated body mismatch", http.StatusBadRequest)
				return
			}
			_ = json.NewEncoder(w).Encode(RuntimeRunSummary{
				RunID:         "99999999-9999-4999-8999-999999999999",
				Status:        RuntimeRunRunning,
				DispatchState: RuntimeDispatchPending,
			})
		default:
			http.NotFound(w, request)
		}
	})
	server := httptest.NewUnstartedServer(handler)
	server.TLS = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		MaxVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{serverCertificate},
		ClientAuth:   tls.RequireAndVerifyClientCert,
		ClientCAs:    caPool,
	}
	server.StartTLS()
	defer server.Close()

	directory := t.TempDir()
	certPath := filepath.Join(directory, "client.crt")
	keyPath := filepath.Join(directory, "client.key")
	caPath := filepath.Join(directory, "ca.crt")
	for path, raw := range map[string][]byte{certPath: clientCertPEM, keyPath: clientKeyPEM, caPath: caPEM} {
		if err := os.WriteFile(path, raw, 0o600); err != nil {
			t.Fatal(err)
		}
	}

	client, httpClient, err := newRuntimeClient(server.URL, agentToken, runtimeTestHello().NodeID, RuntimeMTLSConfig{
		CertFile:   certPath,
		KeyFile:    keyPath,
		CAFile:     caPath,
		ServerName: "runtime.test",
	})
	if err != nil {
		t.Fatal(err)
	}
	defer httpClient.Transport.(*http.Transport).CloseIdleConnections()
	hello := RuntimeHelloPayload{
		NodeID:           testNodeID,
		AgentID:          testAgentID,
		WorkerID:         "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
		RuntimeSessionID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		SessionEpoch:     1,
		NodeVersion:      runtimeWorkerSDKAgent,
		Capacity:         1,
		Features:         RuntimeRequiredFeatures(),
		ContractDigest:   RuntimeContractDigest,
	}
	if _, err := client.CreateRuntimeSession(context.Background(), hello); err != nil {
		t.Fatalf("create mTLS runtime session: %v", err)
	}
	summary, err := client.CallRuntimeAgent(context.Background(), RuntimeCallAgentAuthorization{
		NodeEnvelope:         nodeEnvelope,
		AgentInvocationToken: invocation,
		IdempotencyKey:       idempotencyKey,
	}, RuntimeCallAgentRequest{
		TargetAgentID: testTargetAgentID,
		Input:         map[string]any{"question": "proof"},
	})
	if err != nil {
		t.Fatalf("delegated mTLS call: %v", err)
	}
	if summary.RunID != "99999999-9999-4999-8999-999999999999" || sessionCalls.Load() != 1 || delegatedCalls.Load() != 1 {
		t.Fatalf("summary=%#v sessionCalls=%d delegatedCalls=%d", summary, sessionCalls.Load(), delegatedCalls.Load())
	}
}

func TestRuntimeNodeHeaderTransportOverridesCallerValue(t *testing.T) {
	request, err := http.NewRequest(http.MethodPost, "https://runtime.example.test/api/v1/agent-runtime/sessions", nil)
	if err != nil {
		t.Fatal(err)
	}
	request.Header.Set(RuntimeNodeIDHeader, "ffffffff-ffff-4fff-8fff-ffffffffffff")
	transport := &runtimeNodeHeaderTransport{
		nodeID: testNodeID,
		base: sdkRoundTripper(func(outbound *http.Request) (*http.Response, error) {
			if got := outbound.Header.Get(RuntimeNodeIDHeader); got != testNodeID {
				t.Fatalf("Runtime Node header = %q, want %q", got, testNodeID)
			}
			return &http.Response{StatusCode: http.StatusNoContent, Body: http.NoBody, Header: make(http.Header)}, nil
		}),
	}

	if _, err = transport.RoundTrip(request); err != nil {
		t.Fatalf("round trip: %v", err)
	}
	if got := request.Header.Get(RuntimeNodeIDHeader); got != "ffffffff-ffff-4fff-8fff-ffffffffffff" {
		t.Fatalf("caller request was mutated: %q", got)
	}
}

func runtimeTestPKI(t *testing.T) ([]byte, tls.Certificate, []byte, []byte) {
	t.Helper()
	caPublic, caPrivate, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC()
	caTemplate := &x509.Certificate{
		SerialNumber:          big.NewInt(1),
		Subject:               pkix.Name{CommonName: "OpenLinker Runtime Test CA"},
		NotBefore:             now.Add(-time.Hour),
		NotAfter:              now.Add(time.Hour),
		IsCA:                  true,
		BasicConstraintsValid: true,
		KeyUsage:              x509.KeyUsageCertSign | x509.KeyUsageDigitalSignature,
	}
	caDER, err := x509.CreateCertificate(rand.Reader, caTemplate, caTemplate, caPublic, caPrivate)
	if err != nil {
		t.Fatal(err)
	}
	caCertificate, err := x509.ParseCertificate(caDER)
	if err != nil {
		t.Fatal(err)
	}
	caPEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: caDER})

	issue := func(serial int64, commonName string, usage x509.ExtKeyUsage, dnsNames []string) ([]byte, []byte, tls.Certificate) {
		publicKey, privateKey, generateErr := ed25519.GenerateKey(rand.Reader)
		if generateErr != nil {
			t.Fatal(generateErr)
		}
		template := &x509.Certificate{
			SerialNumber: big.NewInt(serial),
			Subject:      pkix.Name{CommonName: commonName},
			NotBefore:    now.Add(-time.Hour),
			NotAfter:     now.Add(time.Hour),
			KeyUsage:     x509.KeyUsageDigitalSignature,
			ExtKeyUsage:  []x509.ExtKeyUsage{usage},
			DNSNames:     dnsNames,
		}
		certificateDER, createErr := x509.CreateCertificate(rand.Reader, template, caCertificate, publicKey, caPrivate)
		if createErr != nil {
			t.Fatal(createErr)
		}
		privateDER, marshalErr := x509.MarshalPKCS8PrivateKey(privateKey)
		if marshalErr != nil {
			t.Fatal(marshalErr)
		}
		certificatePEM := pem.EncodeToMemory(&pem.Block{Type: "CERTIFICATE", Bytes: certificateDER})
		privatePEM := pem.EncodeToMemory(&pem.Block{Type: "PRIVATE KEY", Bytes: privateDER})
		pair, pairErr := tls.X509KeyPair(certificatePEM, privatePEM)
		if pairErr != nil {
			t.Fatal(pairErr)
		}
		return certificatePEM, privatePEM, pair
	}
	_, _, serverCertificate := issue(2, "runtime.test", x509.ExtKeyUsageServerAuth, []string{"runtime.test"})
	clientCertificatePEM, clientPrivatePEM, _ := issue(3, "agent-node-test", x509.ExtKeyUsageClientAuth, nil)
	return caPEM, serverCertificate, clientCertificatePEM, clientPrivatePEM
}
