package openlinker

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestNewWebhookRunCallbackPassesExternalURLAndSecret(t *testing.T) {
	callback, err := NewWebhookRunCallback(" https://caller.example.com/openlinker/events ", WebhookRunCallbackOptions{
		Token:      "caller-token",
		EventTypes: []string{"run.completed", "run.failed"},
		Metadata:   JSON{"client": "go-sdk"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if callback.URL != "https://caller.example.com/openlinker/events" {
		t.Fatalf("callback URL = %q", callback.URL)
	}
	if len(callback.Secret) != 64 {
		t.Fatalf("callback secret length = %d", len(callback.Secret))
	}

	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		writeJSON(t, w, RunResponse{RunID: "run-webhook", Status: "running"})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.StartAgentRun(context.Background(), RunAgentRequest{
		AgentID:      "00000000-0000-0000-0000-000000000001",
		Input:        JSON{"query": "hello"},
		TaskCallback: callback,
	})
	if err != nil {
		t.Fatal(err)
	}

	taskCallback, ok := got["task_callback"].(map[string]any)
	if !ok {
		t.Fatalf("task_callback = %#v", got["task_callback"])
	}
	if taskCallback["url"] != callback.URL || taskCallback["token"] != "caller-token" {
		t.Fatalf("task callback = %#v", taskCallback)
	}
	if taskCallback["secret"] != callback.Secret {
		t.Fatalf("task callback secret = %#v", taskCallback["secret"])
	}
	events, ok := taskCallback["event_types"].([]any)
	if !ok || len(events) != 2 || events[0] != "run.completed" {
		t.Fatalf("task callback events = %#v", taskCallback["event_types"])
	}
}

func TestTaskCallbackSignatureHelpersVerifyWebhookPayloads(t *testing.T) {
	secret, err := GenerateTaskCallbackSecret()
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte(`{"event_type":"run.completed","run_id":"run-1"}`)
	signature := SignTaskCallbackPayload(payload, secret)
	if !VerifyTaskCallbackSignature(payload, secret, "sha256="+signature) {
		t.Fatal("expected signature to verify")
	}
	if VerifyTaskCallbackSignature(append(payload, '\n'), secret, "sha256="+signature) {
		t.Fatal("expected tampered payload to fail verification")
	}

	req := httptest.NewRequest(http.MethodPost, "/openlinker/events", strings.NewReader(string(payload)))
	req.Header.Set("X-OpenLinker-Signature", "sha256="+signature)
	body, ok, err := VerifyTaskCallbackRequest(req, secret, 1024)
	if err != nil {
		t.Fatal(err)
	}
	if !ok {
		t.Fatal("expected request signature to verify")
	}
	if string(body) != string(payload) {
		t.Fatalf("body = %s", body)
	}
	restored, err := io.ReadAll(req.Body)
	if err != nil {
		t.Fatal(err)
	}
	if string(restored) != string(payload) {
		t.Fatalf("restored body = %s", restored)
	}
}
