package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func testEnv(values map[string]string) func(string) string {
	return func(key string) string { return values[key] }
}

func TestParseConfigDefaultsToReadOnlyAndGuardsWrites(t *testing.T) {
	env := testEnv(map[string]string{
		"OPENLINKER_API_BASE": "https://api.example.test", "OPENLINKER_USER_TOKEN": "ol_user_example", "OPENLINKER_AGENT_ID": "agent-1",
	})
	cfg, err := parseConfig(nil, env)
	if err != nil || cfg.Action != "list" || cfg.ConfirmWrite {
		t.Fatalf("default config = %#v, %v", cfg, err)
	}
	if _, err = parseConfig([]string{"--action=create"}, env); err == nil || !strings.Contains(err.Error(), "--confirm-write") {
		t.Fatalf("unguarded create error = %v", err)
	}
	if _, err = parseConfig([]string{"--action=revoke", "--token-id=token-1"}, env); err == nil || !strings.Contains(err.Error(), "--confirm-write") {
		t.Fatalf("unguarded revoke error = %v", err)
	}
}

func TestRunListsCreatesAndRevokesTokens(t *testing.T) {
	var listCalls atomic.Int32
	var createCalls atomic.Int32
	var revokeCalls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer ol_user_example" {
			t.Fatalf("Authorization = %q", request.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		switch request.Method + " " + request.URL.Path {
		case "GET /api/v1/creator/agent-tokens":
			listCalls.Add(1)
			if request.URL.Query().Get("agent_id") != "agent-1" {
				t.Fatalf("query = %s", request.URL.RawQuery)
			}
			agentID := "agent-1"
			_ = json.NewEncoder(w).Encode(openlinker.AgentTokenListResponse{Items: []openlinker.AgentTokenResponse{{
				ID: "token-list", AgentID: &agentID, Name: "existing", Prefix: "ol_agent_existing", Status: "active_runtime",
			}}})
		case "POST /api/v1/creator/agent-tokens":
			createCalls.Add(1)
			var body openlinker.CreateAgentTokenRequest
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if body.AgentID != "agent-1" || body.Name != "new runtime" || len(body.Scopes) != 2 {
				t.Fatalf("body = %#v", body)
			}
			_ = json.NewEncoder(w).Encode(openlinker.AgentTokenResponse{
				ID: "token-new", Name: body.Name, Prefix: "ol_agent_new", Status: "active_runtime", Scopes: body.Scopes,
				PlaintextToken: "ol_agent_plaintext_once",
			})
		case "DELETE /api/v1/creator/agent-tokens/token-new":
			revokeCalls.Add(1)
			w.WriteHeader(http.StatusNoContent)
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	base := config{APIBase: server.URL, UserToken: "ol_user_example", AgentID: "agent-1"}
	var listOutput bytes.Buffer
	list := base
	list.Action = "list"
	if err := run(context.Background(), list, &listOutput); err != nil {
		t.Fatal(err)
	}
	if strings.Contains(listOutput.String(), "plaintext_token") || !strings.Contains(listOutput.String(), "ol_agent_existing") {
		t.Fatalf("list output = %s", listOutput.String())
	}

	var createOutput bytes.Buffer
	create := base
	create.Action = "create"
	create.ConfirmWrite = true
	create.TokenName = "new runtime"
	create.Scopes = []string{"agent:pull", "agent:call"}
	if err := run(context.Background(), create, &createOutput); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(createOutput.String(), "ol_agent_plaintext_once") || !strings.Contains(createOutput.String(), "只显示一次") {
		t.Fatalf("create output = %s", createOutput.String())
	}

	var revokeOutput bytes.Buffer
	revoke := base
	revoke.Action = "revoke"
	revoke.ConfirmWrite = true
	revoke.TokenID = "token-new"
	if err := run(context.Background(), revoke, &revokeOutput); err != nil {
		t.Fatal(err)
	}
	if listCalls.Load() != 1 || createCalls.Load() != 1 || revokeCalls.Load() != 1 || !strings.Contains(revokeOutput.String(), `"status": "revoked"`) {
		t.Fatalf("calls list=%d create=%d revoke=%d output=%s", listCalls.Load(), createCalls.Load(), revokeCalls.Load(), revokeOutput.String())
	}
}
