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

	client, err := NewClient(server.URL+"/api/v1", WithUserToken("ol_user_test"))
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
	if gotAuth != "Bearer ol_user_test" {
		t.Fatalf("auth = %q", gotAuth)
	}
	if gotSDK != defaultSDKAgent {
		t.Fatalf("sdk agent = %q", gotSDK)
	}
}

func TestClientRejectsOversizedResponseBody(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"items":[`))
		_, _ = w.Write([]byte(strings.Repeat(`"x",`, int(maxResponseBodyBytes/4))))
		_, _ = w.Write([]byte(`"tail"]}`))
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	_, err = client.ListAgents(context.Background(), ListAgentsParams{})
	if err == nil || !strings.Contains(err.Error(), "response body exceeds") {
		t.Fatalf("err = %v", err)
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
		writeJSON(t, w, RunResponse{
			RunID: "run-1", AgentID: "00000000-0000-0000-0000-000000000001",
			AgentSlug: "runtime-agent", AgentName: "Runtime Agent", AgentConnectionMode: "runtime",
			Status: "success", CostCents: 0, DurationMS: 12,
			StartedAt: "2026-07-18T00:00:00Z", FinishedAt: "2026-07-18T00:00:01Z", Source: "api",
			RuntimeContractID: "openlinker.runtime.v2", RuntimeTransport: "websocket",
			RuntimeTransportReason: "recovery", RuntimeTransportChangedAt: "2026-07-18T00:00:00Z",
			DispatchState: "terminal", AttemptCount: 1, MaxAttempts: 3, LatestAttemptID: "attempt-1",
		})
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
			EventTypes: []string{"run.completed", "run.failed"},
			Metadata:   JSON{"client": "go-sdk"},
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	if resp.RunID != "run-1" {
		t.Fatalf("run id = %q", resp.RunID)
	}
	if resp.AgentConnectionMode != "runtime" || resp.RuntimeTransport != "websocket" ||
		resp.RuntimeTransportReason != "recovery" || resp.DispatchState != "terminal" || resp.AttemptCount != 1 {
		t.Fatalf("run execution evidence = %#v", resp)
	}
	if got["agent_id"] != "00000000-0000-0000-0000-000000000001" {
		t.Fatalf("agent_id = %#v", got["agent_id"])
	}
	if _, ok := got["agent_connection_mode"]; ok {
		t.Fatalf("run request must not select Agent connection mode: %#v", got)
	}
	if _, ok := got["runtime_transport"]; ok {
		t.Fatalf("run request must not select Runtime transport: %#v", got)
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

func TestRecommendTaskEncodesRequestAndResponse(t *testing.T) {
	var got RecommendTaskRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/tasks/recommend" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if auth := r.Header.Get("Authorization"); auth != "Bearer ol_user_test" {
			t.Fatalf("authorization = %q", auth)
		}
		if err := json.NewDecoder(r.Body).Decode(&got); err != nil {
			t.Fatal(err)
		}
		writeJSON(t, w, RecommendTaskResponse{
			TaskID:       "task-1",
			Visibility:   "private",
			ParsedSkills: []string{"summary"},
			Recommendations: []TaskRecommendation{{
				Agent:      TaskAgentSummary{ID: "agent-1", Slug: "writer", Name: "Writer"},
				MatchScore: 0.95,
				Why:        "matches summary",
			}},
		})
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithUserToken("ol_user_test"))
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.RecommendTask(context.Background(), RecommendTaskRequest{
		Query:      "summarize a document",
		TemplateID: "summary",
		SkillIDs:   []string{"summary"},
		MCPTools:   []string{"search_agents"},
		AgentSlugs: []string{"writer"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if got.Query != "summarize a document" || got.TemplateID != "summary" || len(got.SkillIDs) != 1 || got.AgentSlugs[0] != "writer" {
		t.Fatalf("request = %#v", got)
	}
	if resp.TaskID != "task-1" || len(resp.Recommendations) != 1 || resp.Recommendations[0].Agent.Slug != "writer" {
		t.Fatalf("response = %#v", resp)
	}
}

func TestCancelRunPostsOwnedRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/runs/run-1/cancel" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if r.Body != nil && r.ContentLength > 0 {
			t.Fatalf("cancel body length = %d", r.ContentLength)
		}
		writeJSON(t, w, RunResponse{RunID: "run-1", Status: "running", CancelState: "requested"})
	}))
	defer server.Close()

	client, err := NewClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	resp, err := client.CancelRun(context.Background(), "run-1")
	if err != nil {
		t.Fatal(err)
	}
	if resp.RunID != "run-1" || resp.CancelState != "requested" {
		t.Fatalf("response = %#v", resp)
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
		EventTypes: []string{"run.message.delta"},
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

func writeJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}
