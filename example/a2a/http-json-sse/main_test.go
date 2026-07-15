package main

import (
	"bytes"
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
)

func TestRunUsesHTTPJSONAndSSEBinding(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.Method != http.MethodPost || request.URL.Path != "/message:stream" {
			t.Fatalf("request=%s %s", request.Method, request.URL.Path)
		}
		if request.Header.Get("Accept") != "text/event-stream" || request.Header.Get("Content-Type") != "application/a2a+json" {
			t.Fatalf("headers=%v", request.Header)
		}
		var body map[string]any
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("id: 1\nevent: task-status-update\ndata: {\"statusUpdate\":{\"taskId\":\"task-sse\",\"status\":{\"state\":\"working\"}}}\n\n"))
		_, _ = w.Write([]byte("id: 2\nevent: task-status-update\ndata: {\"statusUpdate\":{\"taskId\":\"task-sse\",\"status\":{\"state\":\"completed\"},\"final\":true}}\n\n"))
	}))
	defer server.Close()
	var output bytes.Buffer
	if err := run(context.Background(), config{Endpoint: server.URL, Input: "hello", MessageID: "message-1"}, &output); err != nil {
		t.Fatal(err)
	}
	if strings.Count(output.String(), `"event": "task-status-update"`) != 2 || !strings.Contains(output.String(), "completed") {
		t.Fatalf("output=%s", output.String())
	}
}
