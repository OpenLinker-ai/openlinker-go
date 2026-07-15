package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func TestRunCreatesThenReusesStoredRegistration(t *testing.T) {
	t.Setenv("OPENLINKER_AGENT_TOKEN", "")
	t.Setenv("OPENLINKER_USER_TOKEN", "")
	const agentID = "22222222-2222-4222-8222-222222222222"
	const tokenID = "33333333-3333-4333-8333-333333333333"
	var calls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls.Add(1)
		if request.Header.Get("Authorization") != "Bearer ol_user_example" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		switch request.Method + " " + request.URL.Path {
		case "GET /api/v1/creator/agents/by-slug/demo-agent":
			w.WriteHeader(http.StatusNotFound)
			_, _ = w.Write([]byte(`{"error":{"code":"NOT_FOUND","message":"missing"}}`))
		case "POST /api/v1/creator/agents":
			var body openlinker.CreateAgentRequest
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.Slug != "demo-agent" || body.Visibility != "private" || body.ConnectionMode != "runtime_ws" {
				t.Fatalf("agent body = %#v", body)
			}
			_ = json.NewEncoder(w).Encode(openlinker.AgentResponse{ID: agentID, Slug: "demo-agent", Name: "Demo Agent"})
		case "POST /api/v1/creator/agent-tokens":
			var body openlinker.CreateAgentTokenRequest
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.AgentID != agentID || len(body.Scopes) != 2 {
				t.Fatalf("token body = %#v", body)
			}
			_ = json.NewEncoder(w).Encode(openlinker.AgentTokenResponse{
				ID: tokenID, Prefix: "ol_agent_demo", Status: "active_runtime", PlaintextToken: "ol_agent_plaintext",
			})
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	statePath := filepath.Join(t.TempDir(), "registration.env")
	cfg := config{
		APIBase: server.URL, UserToken: "ol_user_example", Slug: "demo-agent", Name: "Demo Agent",
		Tags: []string{"agent", "runtime"}, StatePath: statePath,
	}
	var firstOutput bytes.Buffer
	if err := run(context.Background(), cfg, &firstOutput); err != nil {
		t.Fatal(err)
	}
	var secondOutput bytes.Buffer
	reuseConfig := cfg
	reuseConfig.UserToken = ""
	if err := run(context.Background(), reuseConfig, &secondOutput); err != nil {
		t.Fatal(err)
	}
	if calls.Load() != 3 {
		t.Fatalf("HTTP calls = %d, want 3", calls.Load())
	}
	if strings.Contains(firstOutput.String(), "ol_agent_plaintext") || !strings.Contains(firstOutput.String(), `"token_prefix": "ol_agent_demo"`) {
		t.Fatalf("unsafe output = %s", firstOutput.String())
	}
	raw, err := os.ReadFile(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(string(raw), "ol_agent_plaintext") {
		t.Fatalf("registration state did not persist token: %s", raw)
	}
	info, err := os.Stat(statePath)
	if err != nil {
		t.Fatal(err)
	}
	if info.Mode().Perm() != 0o600 {
		t.Fatalf("state mode = %o", info.Mode().Perm())
	}
}
