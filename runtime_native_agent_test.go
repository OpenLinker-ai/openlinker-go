package openlinker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func TestNativeAgentRunnerCompletesPullRun(t *testing.T) {
	resultCh := make(chan map[string]any, 1)
	claimed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
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
				RunID:          "run-native-agent",
				AgentID:        "agent-native",
				Input:          JSON{"task": "world"},
				Source:         "api",
				ResultEndpoint: "/api/v1/agent-runtime/runs/run-native-agent/result",
				ResultMethod:   http.MethodPost,
				ResultRequired: true,
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/runs/run-native-agent/result":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			resultCh <- body
			writeJSON(t, w, RunResponse{RunID: "run-native-agent", Status: "success"})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithRuntimeToken("ol_agent_native"))
	if err != nil {
		t.Fatal(err)
	}
	err = WithFunc(func(ctx context.Context, input string) (string, error) {
		return "hello " + input, nil
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
		if result["status"] != "success" {
			t.Fatalf("status = %v", result["status"])
		}
		output := result["output"].(map[string]any)
		if output["text"] != "hello world" {
			t.Fatalf("output = %#v", output)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

func TestNativeAgentRunnerInjectsNativeRunContext(t *testing.T) {
	var seenRun NativeRun
	var seen bool
	runner := WithFunc(func(ctx context.Context, input string) (string, error) {
		seenRun, seen = NativeRunFromContext(ctx)
		return "ok", nil
	})

	out, err := runner.handleRun(context.Background(), NativeRun{
		Assignment: RuntimeAssignment{
			RunID:   "run-context",
			AgentID: "agent-context",
			Input:   JSON{"text": "hello"},
			A2A: &AgentA2AContext{
				TraceID:           "trace-context",
				ProtocolContextID: "ctx-context",
			},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if !seen {
		t.Fatal("NativeRunFromContext() ok = false, want true")
	}
	if seenRun.Assignment.RunID != "run-context" {
		t.Fatalf("run_id = %q", seenRun.Assignment.RunID)
	}
	if seenRun.Assignment.AgentID != "agent-context" {
		t.Fatalf("agent_id = %q", seenRun.Assignment.AgentID)
	}
	if seenRun.Assignment.A2A == nil || seenRun.Assignment.A2A.TraceID != "trace-context" {
		t.Fatalf("a2a = %#v", seenRun.Assignment.A2A)
	}

	result := out.(NativeResult)
	if result.Status != "success" {
		t.Fatalf("status = %q", result.Status)
	}
}
