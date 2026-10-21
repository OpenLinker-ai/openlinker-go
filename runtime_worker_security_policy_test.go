package openlinker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"sync/atomic"
	"testing"
)

func TestRuntimeIdentityPolicyPreservesDerivedNodeAndRejectsDowngrades(t *testing.T) {
	token := "ol_agent_transition_synthetic"
	derived := tokenScopedRuntimeNodeID(token)
	mode := "mtls"
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runtime-security-policy" || r.Method != http.MethodPost || r.Header.Get("Authorization") != "Bearer "+token {
			t.Error("unexpected credential destination")
			w.WriteHeader(401)
			return
		}
		var request struct {
			NodeID string `json:"node_id"`
		}
		if err := json.NewDecoder(r.Body).Decode(&request); err != nil || request.NodeID != derived {
			t.Errorf("node identity changed: %+v %v", request, err)
		}
		result := map[string]any{"node_id": derived, "agent_id": testAgentID, "authentication_mode": mode}
		if mode == "mtls" {
			result["runtime_url"] = "https://runtime.example.test:9443"
			result["credential_endpoint"] = server.URL + "/api/v1/runtime-credentials"
			result["operation_id"] = testRunID
		}
		_ = json.NewEncoder(w).Encode(result)
	}))
	defer server.Close()
	node := &RuntimeWorker{PlatformURL: server.URL, AgentToken: token, AgentID: testAgentID, DataDir: t.TempDir()}
	connection := runtimeConnectionInformation{RuntimeURL: "https://override.example.test", SecurityPolicyEndpoint: server.URL + "/api/v1/runtime-security-policy"}
	resolved, err := node.resolveRuntimeIdentityPolicy(context.Background(), connection)
	if err != nil {
		t.Fatal(err)
	}
	if node.NodeID != derived || !resolved.MTLSRequired || resolved.RuntimeURL != "https://runtime.example.test:9443" || resolved.CredentialOperationID != testRunID {
		t.Fatalf("wrong authoritative policy: %+v", resolved)
	}
	connection.SecurityPolicyEndpoint = "https://other.example.test/api/v1/runtime-security-policy"
	if _, err = node.resolveRuntimeIdentityPolicy(context.Background(), connection); err == nil {
		t.Fatal("foreign token destination accepted")
	}
	connection.SecurityPolicyEndpoint = server.URL + "/api/v1/runtime-security-policy"
	mode = "token_only"
	connection.MTLSRequired = true
	if _, err = node.resolveRuntimeIdentityPolicy(context.Background(), connection); err == nil {
		t.Fatal("global mTLS downgraded")
	}
	connection.MTLSRequired = false
	disk, err := loadOrCreateRuntimeCredential(node.DataDir, derived)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = node.resolveRuntimeIdentityPolicy(context.Background(), connection); err != nil {
		t.Fatalf("prepared key must not pin an unapproved transition: %v", err)
	}
	disk.MTLSRequired = true
	if err = persistRuntimeCredential(node.DataDir, disk); err != nil {
		t.Fatal(err)
	}
	for _, endpoint := range []string{connection.SecurityPolicyEndpoint, ""} {
		connection.SecurityPolicyEndpoint = endpoint
		if _, err = node.resolveRuntimeIdentityPolicy(context.Background(), connection); err == nil {
			t.Fatal("stored mTLS policy downgraded")
		}
	}
}

func TestRuntimeControlledCredentialUsesExactFeaturesAndPinsBeforeFailedIssuance(t *testing.T) {
	dir := t.TempDir()
	store := openRuntimeStoreForTest(t, dir)
	worker := store.Identity()
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	_, err := PrepareRuntimeCredential(RuntimeCredentialPreparation{DataDir: dir, NodeID: testNodeID, ConfiguredNodeID: testNodeID, AgentID: testAgentID, WorkerID: worker.WorkerID})
	if err != nil {
		t.Fatal(err)
	}
	features := []string{"mcp_bindings.v1", "skill_packages.v1"}
	called := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		var body runtimeCredentialIssueRequest
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		called = true
		if body.NodeID != testNodeID || body.OperationID != testRunID || !slices.Equal(body.Features, normalizeRuntimeHelloFeatures(features)) {
			t.Errorf("wrong controlled request: %+v", body)
		}
		w.WriteHeader(http.StatusConflict)
	}))
	defer server.Close()
	node := &RuntimeWorker{DataDir: dir, NodeID: testNodeID, AgentID: testAgentID, AgentToken: "ol_agent_test", NodeVersion: "test-node/v1", Capacity: 1, OptionalFeatures: features}
	connection := runtimeConnectionInformation{MTLSRequired: true, CredentialEndpoint: server.URL, SecurityPolicyEndpoint: server.URL + "/api/v1/runtime-security-policy", CredentialOperationID: testRunID}
	if err = node.configureRuntimeSecurity(context.Background(), connection); err == nil || !called {
		t.Fatalf("issuance result=%v called=%v", err, called)
	}
	disk, err := loadOrCreateRuntimeCredential(dir, testNodeID)
	if err != nil {
		t.Fatal(err)
	}
	if !disk.MTLSRequired || disk.CertificateChainPEM != "" {
		t.Fatal("authoritative migration policy must survive a failed certificate response")
	}
	if _, err = node.resolveRuntimeIdentityPolicy(context.Background(), runtimeConnectionInformation{}); err == nil {
		t.Fatal("restart downgraded a server-authorized migration")
	}
	info, err := os.Stat(filepath.Join(dir, runtimeCredentialFile))
	if err != nil {
		t.Fatal(err)
	}
	if !runtimeFileModeIsPrivate(info.Mode()) {
		t.Fatal("key is not private")
	}
}

func TestRuntimeIdentityPolicyFailsClosedWithoutRedirectingCredentials(t *testing.T) {
	var leaked atomic.Int64
	foreign := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) { leaked.Add(1); w.WriteHeader(500) }))
	defer foreign.Close()
	for _, scenario := range []string{"http_error", "redirect", "node_mismatch", "agent_mismatch", "unknown_mode", "unknown_field", "duplicate_field", "oversize", "foreign_credential", "invalid_operation"} {
		t.Run(scenario, func(t *testing.T) {
			var server *httptest.Server
			server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				body := map[string]any{"node_id": testNodeID, "agent_id": testAgentID, "authentication_mode": "mtls", "runtime_url": "https://runtime.example.test:9443", "credential_endpoint": server.URL + "/api/v1/runtime-credentials"}
				switch scenario {
				case "http_error":
					w.WriteHeader(503)
					return
				case "redirect":
					http.Redirect(w, r, foreign.URL, http.StatusTemporaryRedirect)
					return
				case "node_mismatch":
					body["node_id"] = testRunID
				case "agent_mismatch":
					body["agent_id"] = testRunID
				case "unknown_mode":
					body["authentication_mode"] = "optional"
				case "unknown_field":
					body["ignore_tls"] = true
				case "duplicate_field":
					_, _ = w.Write([]byte(`{"node_id":"` + testNodeID + `","node_id":"` + testNodeID + `"}`))
					return
				case "oversize":
					_, _ = w.Write([]byte(strings.Repeat(" ", openLinkerDiscoveryMaxBytes+1)))
					return
				case "foreign_credential":
					body["credential_endpoint"] = foreign.URL + "/api/v1/runtime-credentials"
				case "invalid_operation":
					body["operation_id"] = "not-a-uuid"
				}
				_ = json.NewEncoder(w).Encode(body)
			}))
			defer server.Close()
			node := &RuntimeWorker{PlatformURL: server.URL, AgentToken: "synthetic-private-token", NodeID: testNodeID, AgentID: testAgentID, DataDir: t.TempDir()}
			if _, err := node.resolveRuntimeIdentityPolicy(context.Background(), runtimeConnectionInformation{SecurityPolicyEndpoint: server.URL + "/api/v1/runtime-security-policy"}); err == nil {
				t.Fatal("invalid policy accepted")
			}
		})
	}
	if leaked.Load() != 0 {
		t.Fatal("credential-bearing policy request followed a redirect")
	}
}

func TestRuntimeWorkerStartupAppliesIdentityPolicyBeforeTransport(t *testing.T) {
	var transports atomic.Int64
	var server *httptest.Server
	server = httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch r.URL.Path {
		case "/.well-known/openlinker.json":
			_ = json.NewEncoder(w).Encode(map[string]any{"base_urls": map[string]any{"runtime": server.URL}, "runtime": map[string]any{"enabled": true, "mtls_required": false, "security_policy_endpoint": server.URL + "/api/v1/runtime-security-policy"}})
		case "/api/v1/runtime-security-policy":
			_ = json.NewEncoder(w).Encode(map[string]any{"node_id": testNodeID, "agent_id": testAgentID, "authentication_mode": "mtls", "runtime_url": "https://runtime.example.test:9443", "credential_endpoint": server.URL + "/api/v1/runtime-credentials", "operation_id": testRunID})
		case "/api/v1/runtime-credentials":
			w.WriteHeader(409)
		default:
			transports.Add(1)
			w.WriteHeader(500)
		}
	}))
	defer server.Close()
	dir := t.TempDir()
	worker, err := NewRuntimeWorker(RuntimeWorkerConfig{PlatformURL: server.URL, NodeID: testNodeID, AgentID: testAgentID, AgentToken: "ol_agent_synthetic", DataDir: dir, Capacity: 1,
		Handler: RuntimeHandlerFunc(func(context.Context, RuntimeContext) (RuntimeResult, error) {
			t.Error("handler started during failed migration")
			return RuntimeResult{}, nil
		})})
	if err != nil {
		t.Fatal(err)
	}
	if err = worker.Start(context.Background()); err == nil {
		t.Fatal("failed certificate issuance did not stop Worker startup")
	}
	if transports.Load() != 0 {
		t.Fatal("Worker fell back to token-only transport")
	}
	disk, err := loadOrCreateRuntimeCredential(dir, testNodeID)
	if err != nil || !disk.MTLSRequired {
		t.Fatalf("authorized policy not persisted: %v", err)
	}
}
