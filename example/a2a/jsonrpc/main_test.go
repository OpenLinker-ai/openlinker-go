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

func TestRunSendsCurrentJSONRPCMessage(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Header.Get("Authorization") != "Bearer a2a-token" {
			t.Fatalf("Authorization=%q", request.Header.Get("Authorization"))
		}
		var rpc openlinker.A2AJSONRPCRequest
		if err := json.NewDecoder(request.Body).Decode(&rpc); err != nil {
			t.Fatal(err)
		}
		if rpc.Method != openlinker.A2AMethodMessageSend {
			t.Fatalf("method=%q", rpc.Method)
		}
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(openlinker.A2AJSONRPCResponse{JSONRPC: "2.0", ID: rpc.ID, Result: json.RawMessage(`{"id":"task-jsonrpc","status":{"state":"completed"}}`)})
	}))
	defer server.Close()
	var output bytes.Buffer
	if err := run(context.Background(), config{Endpoint: server.URL, Token: "a2a-token", Input: "hello", MessageID: "message-1"}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), "task-jsonrpc") || !strings.Contains(output.String(), "completed") {
		t.Fatalf("output=%s", output.String())
	}
}
