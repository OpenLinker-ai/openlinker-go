package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestNativeRunnerCompletesPullRun(t *testing.T) {
	resultCh := make(chan map[string]any, 1)
	claimed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer ol_agent_native" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/heartbeat":
			writeJSON(t, w, AgentHeartbeatResponse{AgentID: "agent-native"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agent-runtime/runs/claim":
			if claimed {
				w.WriteHeader(http.StatusNoContent)
				return
			}
			claimed = true
			writeJSON(t, w, RuntimePullRunResponse{
				RunID:          "run-native",
				AgentID:        "agent-native",
				Input:          JSON{"text": "hello"},
				Source:         "api",
				ResultEndpoint: "/api/v1/agent-runtime/runs/run-native/result",
				ResultMethod:   http.MethodPost,
				ResultRequired: true,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/runs/run-native/result":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			resultCh <- body
			writeJSON(t, w, RunResponse{RunID: "run-native", Status: "success"})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithRuntimeToken("ol_agent_native"))
	if err != nil {
		t.Fatal(err)
	}
	err = Native(func(ctx context.Context, run NativeRun) (any, error) {
		if got := run.Text(); got != "hello" {
			t.Fatalf("Text() = %q", got)
		}
		return JSON{"answer": "ok"}, nil
	}).
		WithClient(client).
		WithConnector(RuntimeConnectorPull).
		WithPullWait(time.Second).
		WithMaxRuns(1).
		Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-resultCh:
		if result["status"] != "success" {
			t.Fatalf("status = %v", result["status"])
		}
		output := result["output"].(map[string]any)
		if output["answer"] != "ok" {
			t.Fatalf("output = %#v", output)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

func TestNativeRunnerCompletesPanickedRun(t *testing.T) {
	resultCh := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/heartbeat":
			writeJSON(t, w, AgentHeartbeatResponse{AgentID: "agent-native"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agent-runtime/runs/claim":
			writeJSON(t, w, RuntimePullRunResponse{
				RunID:          "run-panic",
				AgentID:        "agent-native",
				Input:          "hello",
				Source:         "api",
				ResultEndpoint: "/api/v1/agent-runtime/runs/run-panic/result",
				ResultMethod:   http.MethodPost,
				ResultRequired: true,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/runs/run-panic/result":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			resultCh <- body
			writeJSON(t, w, RunResponse{RunID: "run-panic", Status: "failed"})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithRuntimeToken("ol_agent_native"))
	if err != nil {
		t.Fatal(err)
	}
	err = Native(func(ctx context.Context, run NativeRun) (any, error) {
		panic("boom")
	}).
		WithClient(client).
		WithPullWait(time.Second).
		WithMaxRuns(1).
		Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-resultCh:
		if result["status"] != "failed" {
			t.Fatalf("status = %v", result["status"])
		}
		agentErr := result["error"].(map[string]any)
		if agentErr["code"] != "AGENT_RUNTIME_PANIC" {
			t.Fatalf("error code = %v", agentErr["code"])
		}
		if message := agentErr["message"].(string); !strings.Contains(message, "boom") {
			t.Fatalf("error message = %q", message)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

func TestNativeRunnerCompletesFailedRun(t *testing.T) {
	resultCh := make(chan map[string]any, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/heartbeat":
			writeJSON(t, w, AgentHeartbeatResponse{AgentID: "agent-native"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agent-runtime/runs/claim":
			writeJSON(t, w, RuntimePullRunResponse{
				RunID:          "run-failed",
				AgentID:        "agent-native",
				Input:          "hello",
				Source:         "api",
				ResultEndpoint: "/api/v1/agent-runtime/runs/run-failed/result",
				ResultMethod:   http.MethodPost,
				ResultRequired: true,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/runs/run-failed/result":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			resultCh <- body
			writeJSON(t, w, RunResponse{RunID: "run-failed", Status: "failed"})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithRuntimeToken("ol_agent_native"))
	if err != nil {
		t.Fatal(err)
	}
	err = Native(func(ctx context.Context, run NativeRun) (any, error) {
		return nil, errors.New("boom")
	}).
		WithClient(client).
		WithPullWait(time.Second).
		WithMaxRuns(1).
		Run(context.Background())
	if err != nil {
		t.Fatal(err)
	}

	select {
	case result := <-resultCh:
		if result["status"] != "failed" {
			t.Fatalf("status = %v", result["status"])
		}
		agentErr := result["error"].(map[string]any)
		if agentErr["code"] != "AGENT_RUNTIME_ERROR" || agentErr["message"] != "boom" {
			t.Fatalf("error = %#v", agentErr)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}
