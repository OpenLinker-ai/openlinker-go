package openlinker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestRuntimePullConnectorAssignsAndCompletesRun(t *testing.T) {
	assignedCh := make(chan RuntimeAssignment, 1)
	resultCh := make(chan map[string]any, 1)
	claimed := false
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if got := r.Header.Get("Authorization"); got != "Bearer ol_live_runtime" {
			t.Fatalf("authorization = %q", got)
		}
		switch {
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/heartbeat":
			writeJSON(t, w, AgentHeartbeatResponse{AgentID: "agent-1"})
		case r.Method == http.MethodGet && r.URL.Path == "/api/v1/agent-runtime/runs/claim":
			if r.URL.Query().Get("wait") != "1" {
				t.Fatalf("wait = %q", r.URL.Query().Get("wait"))
			}
			if claimed {
				w.Header().Set("Retry-After", "1")
				w.WriteHeader(http.StatusNoContent)
				return
			}
			claimed = true
			writeJSON(t, w, RuntimePullRunResponse{
				RunID:          "run-1",
				AgentID:        "agent-1",
				Input:          "hello",
				Metadata:       JSON{"source": "test"},
				Source:         "api",
				ResultEndpoint: "/api/v1/agent-runtime/runs/run-1/result",
				ResultMethod:   http.MethodPost,
				ResultRequired: true,
				A2A:            &AgentA2AContext{CurrentRunID: "run-1"},
			})
		case r.Method == http.MethodPost && r.URL.Path == "/api/v1/agent-runtime/runs/run-1/result":
			var body map[string]any
			if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
				t.Fatal(err)
			}
			resultCh <- body
			writeJSON(t, w, RunResponse{RunID: "run-1", Status: "success"})
		default:
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithRuntimeToken("ol_live_runtime"))
	if err != nil {
		t.Fatal(err)
	}
	connector := NewRuntimePullConnector(client)
	connector.Wait = time.Second
	connector.Heartbeat = time.Millisecond
	connector.EmptyRetry = time.Millisecond
	connector.MaxRuns = 1
	if err := connector.Start(context.Background(), RuntimeHandlers{
		OnAssigned: func(assignment RuntimeAssignment) {
			assignedCh <- assignment
		},
		OnError: func(err error) {
			t.Errorf("unexpected connector error: %v", err)
		},
	}); err != nil {
		t.Fatal(err)
	}
	defer connector.Stop(context.Background())

	select {
	case assignment := <-assignedCh:
		if assignment.RunID != "run-1" || assignment.Input != "hello" || assignment.A2A.CurrentRunID != "run-1" {
			t.Fatalf("assignment = %+v", assignment)
		}
		if err := connector.CompleteRun(context.Background(), assignment.RunID, RuntimePullResultRequest{
			Status: "success",
			Output: JSON{"answer": "ok"},
			Events: []AgentEvent{{EventType: "run.message.delta", Payload: "done"}},
		}); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for assignment")
	}

	select {
	case result := <-resultCh:
		if result["status"] != "success" || result["output"].(map[string]any)["answer"] != "ok" {
			t.Fatalf("result = %#v", result)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for result")
	}
}

func TestRuntimeWSConnectorHandlesAssignmentsAndSendsMessages(t *testing.T) {
	upgrader := websocket.Upgrader{}
	readyCh := make(chan RuntimeWSServerMessage, 1)
	assignedCh := make(chan RuntimeAssignment, 1)
	messagesCh := make(chan []RuntimeWSClientMessage, 1)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.URL.Path != "/api/v1/agent-runtime/ws" {
			t.Fatalf("path = %s", r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer ol_live_ws" {
			t.Fatalf("authorization = %q", got)
		}
		if got := r.Header.Get("X-OpenLinker-SDK"); got != defaultSDKAgent {
			t.Fatalf("sdk header = %q", got)
		}
		conn, err := upgrader.Upgrade(w, r, nil)
		if err != nil {
			t.Fatal(err)
		}
		defer conn.Close()
		if err := conn.WriteJSON(RuntimeWSServerMessage{Type: "runtime.ready", AgentID: "agent-1"}); err != nil {
			t.Error(err)
			return
		}
		if err := conn.WriteJSON(RuntimeWSServerMessage{
			Type:           "run.assigned",
			RunID:          "run-ws",
			AgentID:        "agent-1",
			Input:          JSON{"task": "ws"},
			Source:         "api",
			ResultRequired: true,
			A2A:            &AgentA2AContext{CurrentRunID: "run-ws"},
		}); err != nil {
			t.Error(err)
			return
		}
		var messages []RuntimeWSClientMessage
		for len(messages) < 2 {
			var message RuntimeWSClientMessage
			if err := conn.ReadJSON(&message); err != nil {
				t.Error(err)
				return
			}
			messages = append(messages, message)
		}
		messagesCh <- messages
	}))
	defer server.Close()

	client, err := NewClient(server.URL, WithRuntimeToken("ol_live_ws"))
	if err != nil {
		t.Fatal(err)
	}
	connector := NewRuntimeWSConnector(client)
	connector.Reconnect = false
	if err := connector.Start(context.Background(), RuntimeHandlers{
		OnReady: func(message RuntimeWSServerMessage) {
			readyCh <- message
		},
		OnAssigned: func(assignment RuntimeAssignment) {
			assignedCh <- assignment
		},
		OnError: func(err error) {
			t.Errorf("unexpected connector error: %v", err)
		},
	}); err != nil {
		t.Fatal(err)
	}
	defer connector.Stop(context.Background())

	select {
	case ready := <-readyCh:
		if ready.AgentID != "agent-1" {
			t.Fatalf("ready = %+v", ready)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for ready")
	}

	select {
	case assignment := <-assignedCh:
		if assignment.RunID != "run-ws" || assignment.A2A.CurrentRunID != "run-ws" {
			t.Fatalf("assignment = %+v", assignment)
		}
		if err := connector.SendRunEvent(context.Background(), assignment.RunID, AgentEvent{EventType: "run.message.delta", Payload: "hi"}); err != nil {
			t.Fatal(err)
		}
		if err := connector.CompleteRun(context.Background(), assignment.RunID, RuntimePullResultRequest{
			Status:     "success",
			Output:     JSON{"answer": "ok"},
			DurationMS: 12,
		}); err != nil {
			t.Fatal(err)
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for assignment")
	}

	select {
	case messages := <-messagesCh:
		if messages[0].Type != "run.event" || messages[0].EventType != "run.message.delta" || messages[0].RunID != "run-ws" {
			t.Fatalf("event message = %+v", messages[0])
		}
		if messages[1].Type != "run.result" || messages[1].Status != "success" || messages[1].DurationMS != 12 {
			t.Fatalf("result message = %+v", messages[1])
		}
	case <-time.After(2 * time.Second):
		t.Fatal("timed out waiting for websocket messages")
	}
}
