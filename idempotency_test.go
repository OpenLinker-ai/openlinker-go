package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestValidateIdempotencyKeyBoundaries(t *testing.T) {
	for _, key := range []string{"x", " ", strings.Repeat("~", maxIdempotencyKeyBytes)} {
		if err := validateIdempotencyKey(key); err != nil {
			t.Fatalf("valid key of length %d rejected: %v", len(key), err)
		}
	}
	for _, key := range []string{
		"",
		strings.Repeat("x", maxIdempotencyKeyBytes+1),
		"private\x1fkey",
		"private\x7fkey",
		"private\x80key",
	} {
		if !errors.Is(validateIdempotencyKey(key), errInvalidIdempotencyKey) {
			t.Fatalf("unsafe key of length %d was accepted", len(key))
		}
	}
}

func TestRunCreationSendsExplicitIdempotencyKeyAndAcceptsContractStatuses(t *testing.T) {
	tests := []struct {
		name         string
		path         string
		status       int
		bodyReplay   bool
		headerReplay string
		wantReplay   bool
	}{
		{name: "created sync run", path: "/api/v1/run", status: http.StatusCreated},
		{name: "completed replay", path: "/api/v1/run", status: http.StatusOK, bodyReplay: true, wantReplay: true},
		{name: "running replay", path: "/api/v1/runs", status: http.StatusAccepted, headerReplay: "true", wantReplay: true},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			const key = "retry-safe-run-intent"
			server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				if r.Method != http.MethodPost || r.URL.Path != tt.path {
					t.Fatalf("request = %s %s", r.Method, r.URL.Path)
				}
				if got := r.Header.Get("Idempotency-Key"); got != key {
					t.Fatalf("Idempotency-Key = %q", got)
				}
				var body map[string]any
				if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
					t.Fatal(err)
				}
				if _, ok := body["idempotency_key"]; ok {
					t.Fatalf("idempotency key leaked into JSON body: %#v", body)
				}
				w.Header().Set("Content-Type", "application/json")
				if tt.headerReplay != "" {
					w.Header().Set("Idempotency-Replayed", tt.headerReplay)
				}
				w.WriteHeader(tt.status)
				if err := json.NewEncoder(w).Encode(RunResponse{
					RunID:    "run-1",
					Status:   "running",
					Replayed: tt.bodyReplay,
				}); err != nil {
					t.Fatal(err)
				}
			}))
			defer server.Close()

			client, err := NewClient(server.URL, WithHeader("Idempotency-Key", "client-default-must-not-win"))
			if err != nil {
				t.Fatal(err)
			}
			req := RunAgentRequest{AgentID: "agent-1", Input: JSON{"query": "hello"}, IdempotencyKey: key}
			var got *RunResponse
			if tt.path == "/api/v1/run" {
				got, err = client.RunAgent(context.Background(), req)
			} else {
				got, err = client.StartAgentRun(context.Background(), req)
			}
			if err != nil {
				t.Fatal(err)
			}
			if got.RunID != "run-1" || got.Replayed != tt.wantReplay {
				t.Fatalf("response = %+v", got)
			}
		})
	}
}

func TestRunCreationGeneratesFreshPrintableKeyForEachInvocation(t *testing.T) {
	var keys []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		key := r.Header.Get("Idempotency-Key")
		if err := validateIdempotencyKey(key); err != nil {
			t.Fatalf("generated Idempotency-Key is invalid: %v", err)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if _, ok := body["idempotency_key"]; ok {
			t.Fatalf("generated key leaked into JSON body: %#v", body)
		}
		keys = append(keys, key)
		w.Header().Set("Content-Type", "application/json")
		w.WriteHeader(http.StatusCreated)
		if err := json.NewEncoder(w).Encode(RunResponse{RunID: "run-generated", Status: "running"}); err != nil {
			t.Fatal(err)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	req := RunAgentRequest{AgentID: "agent-1", Input: JSON{"query": "hello"}}
	if _, err := client.RunAgent(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if _, err := client.StartAgentRun(context.Background(), req); err != nil {
		t.Fatal(err)
	}
	if len(keys) != 2 || keys[0] == keys[1] {
		t.Fatalf("generated keys = %#v", keys)
	}
}

func TestRunCreationRejectsUnsafeExplicitKeyWithoutSendingOrEchoingIt(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		w.WriteHeader(http.StatusInternalServerError)
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	keys := []string{
		"private\nkey",
		"private-密钥",
		strings.Repeat("private-key-material", 20),
	}
	for index, key := range keys {
		req := RunAgentRequest{AgentID: "agent-1", Input: JSON{"query": "hello"}, IdempotencyKey: key}
		var callErr error
		if index%2 == 0 {
			_, callErr = client.RunAgent(context.Background(), req)
		} else {
			_, callErr = client.StartAgentRun(context.Background(), req)
		}
		if !errors.Is(callErr, errInvalidIdempotencyKey) {
			t.Fatalf("key %d error = %v", index, callErr)
		}
		if strings.Contains(callErr.Error(), key) {
			t.Fatalf("key %d leaked in error: %q", index, callErr)
		}
	}
	if requests != 0 {
		t.Fatalf("server requests = %d, want 0", requests)
	}
}
