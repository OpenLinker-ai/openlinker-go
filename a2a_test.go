package openlinker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestA2AClientJSONRPCMethods(t *testing.T) {
	var received []A2AJSONRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Header.Get("authorization") != "Bearer ol_public" || r.Header.Get("a2a-version") != "1.0" {
			t.Fatalf("headers = %#v", r.Header)
		}
		var req A2AJSONRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		received = append(received, req)
		switch req.Method {
		case A2AMethodMessageSend, A2AMethodTasksGet, A2AMethodTasksCancel:
			writeJSON(t, w, A2AJSONRPCResponse{
				JSONRPC: "2.0",
				ID:      req.ID,
				Result: mustRawJSON(t, A2ATask{
					Kind: "task",
					ID:   "task-a2a",
					Status: A2ATaskStatus{
						State: "TASK_STATE_COMPLETED",
						Message: &A2AMessage{Parts: []map[string]any{{
							"kind": "text",
							"text": "done",
						}}},
					},
				}),
			})
		case A2AMethodTasksList:
			writeJSON(t, w, A2AJSONRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: mustRawJSON(t, A2ATaskListResponse{Tasks: []A2ATask{{ID: "task-a2a"}}})})
		case A2AMethodTaskPushNotificationSet, A2AMethodTaskPushNotificationGet:
			writeJSON(t, w, A2AJSONRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: mustRawJSON(t, A2ATaskPushNotificationConfig{
				TaskID: "task-a2a",
				PushNotificationConfig: A2APushNotificationConfig{
					ID:  "cfg-1",
					URL: "https://caller.example/a2a/events",
				},
			})})
		case A2AMethodTaskPushNotificationList:
			writeJSON(t, w, A2AJSONRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: mustRawJSON(t, A2ATaskPushConfigList{Items: []A2ATaskPushNotificationConfig{{TaskID: "task-a2a"}}})})
		case A2AMethodTaskPushNotificationDelete:
			writeJSON(t, w, A2AJSONRPCResponse{JSONRPC: "2.0", ID: req.ID, Result: mustRawJSON(t, nil)})
		default:
			writeJSON(t, w, A2AJSONRPCResponse{JSONRPC: "2.0", ID: req.ID, Error: &A2AJSONRPCError{Code: -32601, Message: "missing"}})
		}
	}))
	defer server.Close()

	client, err := NewA2AClient(server.URL, WithA2AToken("ol_public"))
	if err != nil {
		t.Fatal(err)
	}
	task, err := client.SendMessage(context.Background(), NewA2ATextMessageParams("msg-1", "hello", nil))
	if err != nil {
		t.Fatal(err)
	}
	if task.ID != "task-a2a" || A2ATaskStateRunStatus(task.Status.State) != "success" || ExtractA2AText(task) != "done" {
		t.Fatalf("task = %#v", task)
	}
	if _, err := client.GetTask(context.Background(), A2ATaskQueryParams{ID: "task-a2a"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTasks(context.Background(), A2ATaskListParams{Status: "completed"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.CancelTask(context.Background(), A2ATaskQueryParams{ID: "task-a2a"}); err != nil {
		t.Fatal(err)
	}
	push := A2ATaskPushConfigParams{ID: "task-a2a", PushNotificationConfig: A2APushNotificationConfig{URL: "https://caller.example/a2a/events"}}
	if _, err := client.SetTaskPushNotificationConfig(context.Background(), push); err != nil {
		t.Fatal(err)
	}
	if _, err := client.GetTaskPushNotificationConfig(context.Background(), A2ATaskPushConfigParams{ID: "task-a2a", PushNotificationConfigID: "cfg-1"}); err != nil {
		t.Fatal(err)
	}
	if _, err := client.ListTaskPushNotificationConfigs(context.Background(), A2ATaskPushConfigParams{ID: "task-a2a"}); err != nil {
		t.Fatal(err)
	}
	if err := client.DeleteTaskPushNotificationConfig(context.Background(), A2ATaskPushConfigParams{ID: "task-a2a", PushNotificationConfigID: "cfg-1"}); err != nil {
		t.Fatal(err)
	}

	if len(received) != 8 {
		t.Fatalf("received %d requests", len(received))
	}
	if received[0].Method != A2AMethodMessageSend {
		t.Fatalf("method = %s", received[0].Method)
	}
	params, ok := received[0].Params.(map[string]any)
	if !ok {
		encoded, err := json.Marshal(received[0].Params)
		if err != nil {
			t.Fatal(err)
		}
		if err := json.Unmarshal(encoded, &params); err != nil {
			t.Fatal(err)
		}
	}
	message := params["message"].(map[string]any)
	if _, ok := message["kind"]; ok {
		t.Fatalf("current A2A message should not include kind: %#v", message)
	}
	part := message["parts"].([]any)[0].(map[string]any)
	if part["text"] != "hello" {
		t.Fatalf("current A2A part = %#v", part)
	}
	if _, ok := part["kind"]; ok {
		t.Fatalf("current A2A part should not include kind: %#v", part)
	}
	config := params["configuration"].(map[string]any)
	if config["returnImmediately"] != false {
		t.Fatalf("current A2A config = %#v", config)
	}
	if _, ok := config["blocking"]; ok {
		t.Fatalf("current A2A config should not include blocking: %#v", config)
	}
}

func TestA2AClientLegacyDialect(t *testing.T) {
	var received A2AJSONRPCRequest
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if err := json.NewDecoder(r.Body).Decode(&received); err != nil {
			t.Fatal(err)
		}
		writeJSON(t, w, A2AJSONRPCResponse{JSONRPC: "2.0", ID: received.ID, Result: mustRawJSON(t, A2ATask{
			ID:     "task-legacy",
			Status: A2ATaskStatus{State: "completed"},
		})})
	}))
	defer server.Close()

	client, err := NewA2AClient(server.URL, WithA2ADialect(A2ADialectLegacy))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := client.SendMessage(context.Background(), NewA2ATextMessageParams("msg-legacy", "legacy", nil)); err != nil {
		t.Fatal(err)
	}
	if received.Method != A2ALegacyMethodMessageSend {
		t.Fatalf("legacy method = %s", received.Method)
	}
	params := received.Params.(map[string]any)
	message := params["message"].(map[string]any)
	if message["kind"] != "message" {
		t.Fatalf("legacy message = %#v", message)
	}
	part := message["parts"].([]any)[0].(map[string]any)
	if part["kind"] != "text" || part["text"] != "legacy" {
		t.Fatalf("legacy part = %#v", part)
	}
	config := params["configuration"].(map[string]any)
	if config["blocking"] != true {
		t.Fatalf("legacy config = %#v", config)
	}
	if _, ok := config["returnImmediately"]; ok {
		t.Fatalf("legacy config should not include returnImmediately: %#v", config)
	}
}

func TestA2AClientStreamAndErrors(t *testing.T) {
	requests := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		requests++
		var req A2AJSONRPCRequest
		if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
			t.Fatal(err)
		}
		if requests == 1 {
			w.Header().Set("content-type", "text/event-stream")
			_, _ = w.Write([]byte("id: 1\n"))
			_, _ = w.Write([]byte("event: task.status\n"))
			_, _ = w.Write([]byte(`data: {"jsonrpc":"2.0","result":{"statusUpdate":{"status":{"state":"TASK_STATE_WORKING"}}}}` + "\n\n"))
			return
		}
		writeJSON(t, w, A2AJSONRPCResponse{JSONRPC: "2.0", ID: req.ID, Error: &A2AJSONRPCError{Code: -32603, Message: "boom"}})
	}))
	defer server.Close()

	client, err := NewA2AClient(server.URL)
	if err != nil {
		t.Fatal(err)
	}
	var events []A2AStreamEvent
	err = client.StreamMessage(context.Background(), NewA2ATextMessageParams("msg-1", "stream", nil), func(event A2AStreamEvent) error {
		events = append(events, event)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(events) != 1 || events[0].Result.StatusUpdate == nil || NormalizeA2ATaskState(events[0].Result.StatusUpdate.Status.State) != "working" {
		t.Fatalf("events = %#v", events)
	}
	if _, err := client.GetTask(context.Background(), A2ATaskQueryParams{ID: "bad"}); err == nil || !strings.Contains(err.Error(), "boom") {
		t.Fatalf("expected json-rpc error, got %v", err)
	}
}

func TestA2ACompatibilityHelpers(t *testing.T) {
	if NormalizeA2AJSONRPCMethod("SendMessage") != A2AMethodMessageSend {
		t.Fatal("SendMessage alias did not normalize")
	}
	if NormalizeA2AJSONRPCMethodForDialect("SendMessage", A2ADialectLegacy) != A2ALegacyMethodMessageSend {
		t.Fatal("SendMessage legacy alias did not normalize")
	}
	if NormalizeA2AJSONRPCMethod("ListTaskPushNotificationConfigs") != A2AMethodTaskPushNotificationList {
		t.Fatal("push notification alias did not normalize")
	}
	if NormalizeA2ADialect("0.3") != A2ADialectLegacy || NormalizeA2ADialect("1.0") != A2ADialectCurrent {
		t.Fatal("A2A dialect did not normalize")
	}
	if NormalizeA2ATaskState("TASK_STATE_CANCELLED") != "canceled" {
		t.Fatal("TASK_STATE_CANCELLED did not normalize")
	}
	for _, state := range []string{"failed", "TASK_STATE_FAILED", "TASK_STATE_REJECTED", "auth_required", "input-required"} {
		if !A2ATaskStateFailed(state) {
			t.Fatalf("%s should be failed", state)
		}
	}
}

func mustRawJSON(t *testing.T, value any) json.RawMessage {
	t.Helper()
	encoded, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return encoded
}
