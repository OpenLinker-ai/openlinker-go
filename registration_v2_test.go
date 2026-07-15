package openlinker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"
)

const (
	testRegistrationTokenID        = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	testRegistrationRotatedTokenID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
)

func TestRegisterAgentViaTokenUsesBearerAndPrivateDefault(t *testing.T) {
	t.Parallel()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/agent-registration/agents" || request.Header.Get("Authorization") != "Bearer ol_agent_pending" {
			t.Fatalf("request = %s %s auth=%q", request.Method, request.URL.Path, request.Header.Get("Authorization"))
		}
		var body RegisterAgentViaTokenRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body.Visibility != "private" || body.Name != "Demo Agent" || body.ConnectionMode != "runtime" {
			t.Fatalf("body = %#v", body)
		}
		writeRuntimeTestJSON(t, w, RegisterAgentViaTokenResponse{
			Agent:      AgentResponse{ID: testAgentID, Slug: "demo-agent", Name: "Demo Agent"},
			AgentToken: AgentTokenResponse{ID: testRegistrationTokenID, Prefix: "ol_agent_demo", Status: "active_runtime"},
		})
	}))
	defer server.Close()
	registered, err := RegisterAgentViaToken(context.Background(), server.URL, "ol_agent_pending", RegisterAgentViaTokenRequest{Name: "Demo Agent"})
	if err != nil {
		t.Fatal(err)
	}
	if registered.Agent.ID != testAgentID {
		t.Fatalf("registered = %#v", registered)
	}
}

func TestNormalizeRegistrationConnectionMode(t *testing.T) {
	t.Parallel()
	for input, want := range map[string]string{
		"": "runtime", "runtime": "runtime", "runtime_ws": "runtime",
		"runtime_pull": "runtime", "agent_node": "runtime", "direct_http": "direct_http",
	} {
		if got := normalizeRegistrationConnectionMode(input); got != want {
			t.Fatalf("normalizeRegistrationConnectionMode(%q) = %q, want %q", input, got, want)
		}
	}
}

func TestEnsureAgentCreatesCreatorAgentAndRuntimeToken(t *testing.T) {
	t.Parallel()
	store := &memoryRegistrationStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer ol_user_creator" {
			t.Fatalf("auth = %q", request.Header.Get("Authorization"))
		}
		switch request.Method + " " + request.URL.Path {
		case "GET /api/v1/creator/agents/by-slug/demo-agent":
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"NOT_FOUND","message":"missing"}}`))
		case "POST /api/v1/creator/agents":
			writeRuntimeTestJSON(t, w, AgentResponse{ID: testAgentID, Slug: "demo-agent", Name: "Demo Agent"})
		case "POST /api/v1/creator/agent-tokens":
			writeRuntimeTestJSON(t, w, AgentTokenResponse{
				ID: testRegistrationTokenID, Prefix: "ol_agent_demo", Status: "active_runtime", PlaintextToken: "ol_agent_plaintext",
			})
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()
	registration, err := EnsureAgent(context.Background(), EnsureAgentRequest{
		APIBase: server.URL, UserToken: "ol_user_creator", Slug: "demo-agent", Name: "Demo Agent", Store: store,
	})
	if err != nil {
		t.Fatal(err)
	}
	if registration.AgentID != testAgentID || registration.AgentToken != "ol_agent_plaintext" || store.registration == nil {
		t.Fatalf("registration = %#v stored=%#v", registration, store.registration)
	}
}

func TestEnsureAgentReusesStoredRegistrationWithoutRedeemingTokenAgain(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	store := &memoryRegistrationStore{}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.URL.Path != "/api/v1/agent-registration/agents" || request.Header.Get("Authorization") != "Bearer ol_agent_pending" {
			t.Fatalf("request = %s %s auth=%q", request.Method, request.URL.Path, request.Header.Get("Authorization"))
		}
		writeRuntimeTestJSON(t, w, RegisterAgentViaTokenResponse{
			Agent: AgentResponse{ID: testAgentID, Slug: "demo-agent", Name: "Demo Agent"},
			AgentToken: AgentTokenResponse{
				ID: testRegistrationTokenID, Prefix: "ol_agent_demo", Status: "active_runtime",
			},
		})
	}))
	defer server.Close()
	spec := AgentSpec{Slug: "demo-agent", Name: "Demo Agent"}
	first, err := EnsureAgent(context.Background(), spec,
		WithRegistrationAPIBase(server.URL), WithRegistrationAgentToken("ol_agent_pending"), WithRegistrationStore(store))
	if err != nil {
		t.Fatal(err)
	}
	second, err := EnsureAgent(context.Background(), spec,
		WithRegistrationAPIBase(server.URL), WithRegistrationStore(store))
	if err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 1 || first.AgentID != second.AgentID || second.AgentToken != "ol_agent_pending" {
		t.Fatalf("calls=%d first=%#v second=%#v", calls.Load(), first, second)
	}
}

func TestEnsureAgentRegistrationPolicies(t *testing.T) {
	t.Parallel()
	var getAgentCalls atomic.Int32
	var listTokenCalls atomic.Int32
	var createAgentCalls atomic.Int32
	var createTokenCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer ol_user_creator" {
			t.Fatalf("auth = %q", request.Header.Get("Authorization"))
		}
		switch request.Method + " " + request.URL.Path {
		case "GET /api/v1/creator/agents/" + testAgentID:
			getAgentCalls.Add(1)
			writeRuntimeTestJSON(t, w, AgentResponse{ID: testAgentID, Slug: "demo-agent", Name: "Demo Agent"})
		case "GET /api/v1/creator/agent-tokens":
			listTokenCalls.Add(1)
			writeRuntimeTestJSON(t, w, AgentTokenListResponse{Items: []AgentTokenResponse{{
				ID: testRegistrationTokenID, AgentID: stringPointer(testAgentID), Status: "active_runtime",
			}}})
		case "POST /api/v1/creator/agents":
			createAgentCalls.Add(1)
			writeRuntimeTestJSON(t, w, AgentResponse{ID: testAgentID, Slug: "forced-agent", Name: "Forced Agent"})
		case "POST /api/v1/creator/agent-tokens":
			call := createTokenCalls.Add(1)
			writeRuntimeTestJSON(t, w, AgentTokenResponse{
				ID: testRegistrationRotatedTokenID, Prefix: "ol_agent_rotated", Status: "active_runtime",
				PlaintextToken: "ol_agent_rotated_" + string(rune('0'+call)),
			})
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()
	stored := func() *memoryRegistrationStore {
		return &memoryRegistrationStore{registration: &AgentRegistration{
			AgentID: testAgentID, AgentSlug: "demo-agent", AgentName: "Demo Agent",
			AgentToken: "ol_agent_active", TokenID: testRegistrationTokenID, APIBase: server.URL,
			RegisteredAt: time.Now().UTC(),
		}}
	}

	if _, err := EnsureAgent(context.Background(), AgentSpec{Name: "Demo Agent"},
		WithRegistrationPolicy(RegisterPolicyValidateOnly), WithRegistrationStore(stored())); err == nil {
		t.Fatal("validate_only without User Token succeeded")
	}
	validated, err := EnsureAgent(context.Background(), AgentSpec{Name: "Demo Agent"},
		WithRegistrationPolicy(RegisterPolicyValidateOnly), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(stored()))
	if err != nil || validated.AgentID != testAgentID {
		t.Fatalf("validated=%#v err=%v", validated, err)
	}
	rotated, err := EnsureAgent(context.Background(), AgentSpec{Name: "Demo Agent"},
		WithRegistrationPolicy(RegisterPolicyRotateToken), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(stored()))
	if err != nil || rotated.AgentToken == "ol_agent_active" {
		t.Fatalf("rotated=%#v err=%v", rotated, err)
	}
	forced, err := EnsureAgent(context.Background(), AgentSpec{Slug: "forced-agent", Name: "Forced Agent"},
		WithRegistrationPolicy(RegisterPolicyForceNew), WithRegistrationUserToken("ol_user_creator"), WithRegistrationStore(stored()))
	if err != nil || forced.AgentSlug != "forced-agent" {
		t.Fatalf("forced=%#v err=%v", forced, err)
	}
	if getAgentCalls.Load() != 2 || listTokenCalls.Load() != 1 || createAgentCalls.Load() != 1 || createTokenCalls.Load() != 2 {
		t.Fatalf("calls get=%d list=%d create-agent=%d create-token=%d",
			getAgentCalls.Load(), listTokenCalls.Load(), createAgentCalls.Load(), createTokenCalls.Load())
	}
}

func TestEnvRegistrationStorePreservesFileAndForcesPrivateMode(t *testing.T) {
	t.Parallel()
	path := filepath.Join(t.TempDir(), ".env")
	if err := os.WriteFile(path, []byte("UNRELATED=value\nOPENLINKER_AGENT_TOKEN=old\n"), 0o644); err != nil {
		t.Fatal(err)
	}
	store := NewEnvRegistrationStore(path)
	if err := store.SaveAgentRegistration(&AgentRegistration{
		AgentID: testAgentID, AgentSlug: "demo-agent", AgentName: "Demo Agent",
		AgentToken: "ol_agent_secret", APIBase: "https://api.example.test", UpdatedAt: time.Now().UTC(),
	}); err != nil {
		t.Fatal(err)
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("registration mode = %o", info.Mode().Perm())
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "UNRELATED=value") || strings.Contains(string(raw), "OPENLINKER_AGENT_TOKEN=old") {
		t.Fatalf("registration env = %s", raw)
	}
	loaded, err := store.LoadAgentRegistration()
	if err != nil || loaded.AgentToken != "ol_agent_secret" || loaded.AgentID != testAgentID {
		t.Fatalf("loaded=%#v err=%v", loaded, err)
	}
}

func stringPointer(value string) *string { return &value }

type memoryRegistrationStore struct{ registration *AgentRegistration }

func (s *memoryRegistrationStore) LoadAgentRegistration() (*AgentRegistration, error) {
	if s.registration == nil {
		return nil, nil
	}
	copyRegistration := *s.registration
	return &copyRegistration, nil
}

func (s *memoryRegistrationStore) SaveAgentRegistration(registration *AgentRegistration) error {
	copyRegistration := *registration
	s.registration = &copyRegistration
	return nil
}
