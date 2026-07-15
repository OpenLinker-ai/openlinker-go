package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func TestRunUsesSyncEndpointAndExplicitIdempotencyKey(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/api/v1/run" {
			t.Fatalf("request = %s %s", request.Method, request.URL.Path)
		}
		if got := request.Header.Get("Idempotency-Key"); got != "example-sync-1" {
			t.Fatalf("Idempotency-Key = %q", got)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer ol_user_example" {
			t.Fatalf("Authorization = %q", got)
		}
		var body openlinker.RunAgentRequest
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		input := body.Input.(map[string]any)
		if body.AgentID != "agent-1" || input["text"] != "hello sync" {
			t.Fatalf("body = %#v", body)
		}
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		_ = json.NewEncoder(w).Encode(openlinker.RunResponse{RunID: "run-sync-1", Status: "success", Output: openlinker.JSON{"text": "done"}})
	}))
	defer server.Close()

	var output bytes.Buffer
	err := run(context.Background(), config{APIBase: server.URL, UserToken: "ol_user_example", AgentID: "agent-1", Input: "hello sync", IdempotencyKey: "example-sync-1"}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"run_id": "run-sync-1"`) || !strings.Contains(output.String(), `"status": "success"`) {
		t.Fatalf("output = %s", output.String())
	}
}
