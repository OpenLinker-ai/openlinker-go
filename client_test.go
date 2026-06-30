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

func TestListAgentsBuildsCoreURLAndAuthHeader(t *testing.T) {
	var gotPath string
	var gotQuery string
	var gotAuth string
	var gotSDK string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		gotPath = r.URL.Path
		gotQuery = r.URL.RawQuery
		gotAuth = r.Header.Get("Authorization")
		gotSDK = r.Header.Get("X-OpenLinker-SDK")
		writeJSON(t, w, MarketListResponse{Total: 0, Page: 2, Size: 5})
	}))
	defer server.Close()

	client, err := NewClient(server.URL+"/api/v1", WithAccessToken("ol_live_test"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.ListAgents(context.Background(), ListAgentsParams{
		Query:        "data",
		Tags:         []string{"sql", "finance"},
		Page:         2,
		Size:         5,
		CallableOnly: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Page != 2 || resp.Size != 5 {
		t.Fatalf("response page/size = %d/%d", resp.Page, resp.Size)
	}
	if gotPath != "/api/v1/agents" {
		t.Fatalf("path = %s", gotPath)
	}
	for _, want := range []string{"q=data", "page=2", "size=5", "callable_only=true", "tags=sql%2Cfinance"} {
		if !strings.Contains(gotQuery, want) {
			t.Fatalf("query %q missing %q", gotQuery, want)
		}
	}
	if gotAuth != "Bearer ol_live_test" {
		t.Fatalf("auth = %q", gotAuth)
	}
	if gotSDK != defaultSDKAgent {
		t.Fatalf("sdk agent = %q", gotSDK)
	}
}

func TestRunAgentEncodesRequestBody(t *testing.T) {
	var got map[string]any
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/run" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		writeJSON(t, w, RunResponse{RunID: "run-1", Status: "success", CostCents: 0, DurationMS: 12})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.RunAgent(context.Background(), RunAgentRequest{
		AgentID:  "00000000-0000-0000-0000-000000000001",
		Input:    JSON{"query": "hello"},
		Metadata: JSON{"trace_id": "trace-1"},
		TaskCallback: &TaskCallbackConfig{
			URL:        "https://caller.example.com/a2a/events",
			Token:      "caller-token",
			Secret:     "caller-secret",
			EventTypes: []AgentEventType{AgentEventTypeRunCompleted, AgentEventTypeRunFailed},
			Metadata:   JSON{"client": "go-sdk"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.RunID != "run-1" {
		t.Fatalf("run id = %q", resp.RunID)
	}
	if got["agent_id"] != "00000000-0000-0000-0000-000000000001" {
		t.Fatalf("agent_id = %#v", got["agent_id"])
	}
	if got["input"].(map[string]any)["query"] != "hello" {
		t.Fatalf("input = %#v", got["input"])
	}
	push, ok := got["task_callback"].(map[string]any)
	if !ok || push["url"] != "https://caller.example.com/a2a/events" || push["token"] != "caller-token" {
		t.Fatalf("task callback = %#v", got["task_callback"])
	}
	if push["secret"] != "caller-secret" {
		t.Fatalf("task callback secret = %#v", push["secret"])
	}
	events, ok := push["event_types"].([]any)
	if !ok || len(events) != 2 || events[0] != "run.completed" {
		t.Fatalf("task callback events = %#v", push["event_types"])
	}
}

func TestRunAgentWithCallbacksUsesPlatformStream(t *testing.T) {
	var calls []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		calls = append(calls, r.Method+" "+r.URL.String())
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/runs":
			var got map[string]any
			if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
				t.Fatal(err)
			}
			if _, ok := got["task_callback"]; ok {
				t.Fatalf("platform callback should not create external task_callback: %#v", got)
			}
			writeJSON(t, w, RunResponse{RunID: "run-platform", Status: "running"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/runs/run-platform/stream":
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("id: 1\nevent: run.message.delta\ndata: {\"text\":\"working\"}\n\n"))
			_, _ = w.Write([]byte("id: 2\nevent: run.completed\ndata: {\"status\":\"success\"}\n\n"))
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/runs/run-platform":
			writeJSON(t, w, RunResponse{RunID: "run-platform", Status: "success", Output: JSON{"ok": true}})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.String())
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	var events []StreamRunEvent
	var terminal []StreamRunEvent
	resp, err := client.RunAgentWithCallbacks(context.Background(), RunAgentRequest{
		AgentID: "00000000-0000-0000-0000-000000000001",
		Input:   JSON{"query": "hello"},
	}, PlatformCallbackOptions{
		EventTypes: []AgentEventType{AgentEventTypeRunMessageDelta},
		OnEvent: func(event StreamRunEvent) error {
			events = append(events, event)
			return nil
		},
		OnTerminal: func(event StreamRunEvent) error {
			terminal = append(terminal, event)
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.Status != "success" {
		t.Fatalf("response = %+v", resp)
	}
	if len(events) != 1 || events[0].Event != "run.message.delta" {
		t.Fatalf("events = %+v", events)
	}
	if len(terminal) != 1 || terminal[0].Event != "run.completed" {
		t.Fatalf("terminal = %+v", terminal)
	}
	if len(calls) != 3 || calls[0] != "POST /api/v1/runs" || calls[1] != "GET /api/v1/runs/run-platform/stream" || calls[2] != "GET /api/v1/runs/run-platform" {
		t.Fatalf("calls = %#v", calls)
	}
}

func TestStandardErrorsBecomeOpenLinkerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		w.Header().Set("X-Request-Id", "req-1")
		w.WriteHeader(http.StatusForbidden)
		_, _ = w.Write([]byte(`{"error":{"code":"FORBIDDEN","message":"missing scope","details":{"scope":"agents:run"}}}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.GetRun(context.Background(), "run-1")
	var openlinkerErr *Error
	if !errors.As(err, &openlinkerErr) {
		t.Fatalf("err = %T %v", err, err)
	}
	if openlinkerErr.StatusCode != http.StatusForbidden || openlinkerErr.Code != "FORBIDDEN" {
		t.Fatalf("openlinker error = %+v", openlinkerErr)
	}
	if openlinkerErr.Message != "missing scope" || openlinkerErr.RequestID != "req-1" {
		t.Fatalf("openlinker error = %+v", openlinkerErr)
	}
}

func TestStreamRunEventsParsesSSE(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/runs/run-1/stream" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if r.URL.Query().Get("after_sequence") != "6" {
			t.Fatalf("after_sequence = %q", r.URL.Query().Get("after_sequence"))
		}
		if r.Header.Get("Accept") != "text/event-stream" {
			t.Fatalf("accept = %q", r.Header.Get("Accept"))
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("id: 7\nevent: run.completed\ndata: {\"ok\":true}\n\n"))
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	var events []StreamRunEvent
	err = client.StreamRunEvents(context.Background(), "run-1", StreamRunEventsOptions{AfterSequence: 6}, func(event StreamRunEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 {
		t.Fatalf("events len = %d", len(events))
	}
	if events[0].ID != "7" || events[0].Event != "run.completed" || string(events[0].Data) != `{"ok":true}` {
		t.Fatalf("event = %+v data=%s", events[0], events[0].Data)
	}
}

func TestRuntimeMethodsUseRuntimeTokenAndProtocolEndpoints(t *testing.T) {
	var calls []struct {
		Path   string
		Query  string
		Auth   string
		Method string
		Body   map[string]any
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		call := struct {
			Path   string
			Query  string
			Auth   string
			Method string
			Body   map[string]any
		}{
			Path:   r.URL.Path,
			Query:  r.URL.RawQuery,
			Auth:   r.Header.Get("Authorization"),
			Method: r.Method,
		}
		if r.Body != nil && r.ContentLength != 0 {
			_ = json.NewDecoder(r.Body).Decode(&call.Body)
		}
		calls = append(calls, call)

		switch r.URL.Path {
		case "/api/v1/agent-runtime/heartbeat":
			writeJSON(t, w, AgentHeartbeatResponse{
				AgentID:                          "agent-1",
				AvailabilityStatus:               "healthy",
				ConsecutiveFailures:              0,
				PendingRunCount:                  1,
				ClaimNow:                         true,
				RecommendedHeartbeatAfterSeconds: 60,
				MaxClaimWaitSeconds:              30,
			})
		case "/api/v1/agent-runtime/runs/claim":
			writeJSON(t, w, RuntimePullRunResponse{
				RunID:          "run-1",
				AgentID:        "agent-1",
				Input:          JSON{"query": "hello"},
				Source:         "api",
				ResultEndpoint: "/api/v1/agent-runtime/runs/run-1/result",
				ResultMethod:   http.MethodPost,
				ResultRequired: true,
			})
		case "/api/v1/agent-runtime/runs/run-1/result":
			writeJSON(t, w, RunResponse{RunID: "run-1", Status: "success", CostCents: 0, DurationMS: 10})
		case "/api/v1/agent-runtime/call-agent":
			writeJSON(t, w, RunResponse{RunID: "child-1", Status: "success", CostCents: 0, DurationMS: 20})
		default:
			t.Fatalf("unexpected path %s", r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithAccessToken("ol_live_user"), WithRuntimeToken("ol_live_runtime"))
	if err != nil {
		t.Fatal(err)
	}
	heartbeat, err := client.HeartbeatAgent(context.Background())
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := client.ClaimRuntimeRun(context.Background(), ClaimRuntimeRunParams{WaitSeconds: 25})
	if err != nil {
		t.Fatal(err)
	}
	completed, err := client.CompleteRuntimeRun(context.Background(), "run-1", RuntimePullResultRequest{
		Status:     "success",
		Output:     JSON{"ok": true},
		Events:     []AgentEvent{{EventType: AgentEventTypeRunMessageDelta, Payload: JSON{"text": "done"}}},
		DurationMS: 10,
	})
	if err != nil {
		t.Fatal(err)
	}
	child, err := client.CallAgent(context.Background(), CallAgentRequest{
		CurrentRunID:  "run-1",
		TargetAgentID: "target-agent",
		Reason:        "delegate",
		Input:         JSON{"query": "child"},
		TaskCallback: &TaskCallbackConfig{
			URL:        "https://caller.example.com/a2a/events",
			Token:      "caller-token",
			Secret:     "caller-secret",
			EventTypes: []AgentEventType{AgentEventTypeRunCompleted, AgentEventTypeRunFailed, AgentEventTypeRunCanceled},
			Metadata:   JSON{"client": "go-sdk"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	childAt, err := client.CallAgentAt(context.Background(), "/api/v1/agent-runtime/call-agent", CallAgentRequest{
		CurrentRunID:  "run-1",
		TargetAgentID: "target-agent",
		Input:         "child",
		PushNotificationConfig: &TaskCallbackConfig{
			URL:   "https://caller.example.com/a2a/protocol-events",
			Token: "protocol-token",
		},
	})
	if err != nil {
		t.Fatal(err)
	}

	if heartbeat.AgentID != "agent-1" || claimed.RunID != "run-1" || completed.RunID != "run-1" || child.RunID != "child-1" || childAt.RunID != "child-1" {
		t.Fatalf("unexpected responses: heartbeat=%+v claimed=%+v completed=%+v child=%+v childAt=%+v", heartbeat, claimed, completed, child, childAt)
	}
	if len(calls) != 5 {
		t.Fatalf("calls len = %d", len(calls))
	}
	for _, call := range calls {
		if call.Auth != "Bearer ol_live_runtime" {
			t.Fatalf("runtime auth = %q", call.Auth)
		}
	}
	if calls[1].Path != "/api/v1/agent-runtime/runs/claim" || calls[1].Query != "wait=25" {
		t.Fatalf("claim path/query = %s?%s", calls[1].Path, calls[1].Query)
	}
	if calls[2].Body["status"] != "success" {
		t.Fatalf("result body = %#v", calls[2].Body)
	}
	if calls[3].Body["current_run_id"] != "run-1" || calls[3].Body["target_agent_id"] != "target-agent" {
		t.Fatalf("call agent body = %#v", calls[3].Body)
	}
	push, ok := calls[3].Body["task_callback"].(map[string]any)
	if !ok || push["url"] != "https://caller.example.com/a2a/events" || push["token"] != "caller-token" {
		t.Fatalf("call agent task callback = %#v", calls[3].Body["task_callback"])
	}
	if push["secret"] != "caller-secret" {
		t.Fatalf("call agent task callback secret = %#v", push["secret"])
	}
	events, ok := push["event_types"].([]any)
	if !ok || len(events) != 3 || events[0] != "run.completed" {
		t.Fatalf("call agent push events = %#v", push["event_types"])
	}
	if calls[4].Path != "/api/v1/agent-runtime/call-agent" || calls[4].Body["input"] != "child" {
		t.Fatalf("call agent at body = %#v path=%s", calls[4].Body, calls[4].Path)
	}
	pushConfig, ok := calls[4].Body["pushNotificationConfig"].(map[string]any)
	if !ok || pushConfig["url"] != "https://caller.example.com/a2a/protocol-events" || pushConfig["token"] != "protocol-token" {
		t.Fatalf("call agent pushNotificationConfig = %#v", calls[4].Body["pushNotificationConfig"])
	}
}

func TestClaimRuntimeRunReturnsNilOnNoContent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Retry-After", "3")
		w.Header().Set("X-OpenLinker-Max-Claim-Wait-Seconds", "30")
		w.WriteHeader(http.StatusNoContent)
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithRuntimeToken("ol_live_runtime"))
	if err != nil {
		t.Fatal(err)
	}
	claimed, err := client.ClaimRuntimeRun(context.Background(), ClaimRuntimeRunParams{})
	if err != nil {
		t.Fatal(err)
	}
	if claimed != nil {
		t.Fatalf("claimed = %+v, want nil", claimed)
	}
	detailed, err := client.ClaimRuntimeRunDetailed(context.Background(), ClaimRuntimeRunParams{})
	if err != nil {
		t.Fatal(err)
	}
	if detailed.Run != nil || detailed.RetryAfter != 3*time.Second || detailed.MaxClaimWaitSeconds != 30 {
		t.Fatalf("detailed = %+v", detailed)
	}
}

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}
