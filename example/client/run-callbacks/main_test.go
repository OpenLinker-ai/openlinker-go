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

func TestRunUsesPlatformCallbacksWithoutExternalWebhook(t *testing.T) {
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		calls = append(calls, request.Method+" "+request.URL.Path)
		switch request.URL.Path {
		case "/api/v1/runs":
			if request.Header.Get("Idempotency-Key") != "example-callback-1" {
				t.Fatalf("Idempotency-Key = %q", request.Header.Get("Idempotency-Key"))
			}
			var body map[string]any
			if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			if _, exists := body["task_callback"]; exists {
				t.Fatalf("platform helper created task_callback: %#v", body)
			}
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(openlinker.RunResponse{RunID: "run-callback-1", Status: "running"})
		case "/api/v1/runs/run-callback-1/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("id: 1\nevent: run.message.delta\ndata: {\"text\":\"working\"}\n\n"))
			_, _ = w.Write([]byte("id: 2\nevent: run.completed\ndata: {\"status\":\"success\"}\n\n"))
		case "/api/v1/runs/run-callback-1":
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(openlinker.RunResponse{RunID: "run-callback-1", Status: "success"})
		default:
			t.Fatalf("unexpected request %s", request.URL.Path)
		}
	}))
	defer server.Close()

	var output bytes.Buffer
	err := run(context.Background(), config{
		APIBase: server.URL, UserToken: "ol_user_example", AgentID: "agent-1", Input: "hello callback",
		IdempotencyKey: "example-callback-1",
	}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if len(calls) != 3 || !strings.Contains(output.String(), "message delta") || !strings.Contains(output.String(), "terminal callback: run.completed") || !strings.Contains(output.String(), "callback stream closed") {
		t.Fatalf("calls=%#v output=%s", calls, output.String())
	}
}
