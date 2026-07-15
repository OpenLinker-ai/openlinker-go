package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func TestRunStreamsUntilTerminalEvent(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		switch request.URL.Path {
		case "/api/v1/runs":
			if request.Method != http.MethodPost || request.Header.Get("Idempotency-Key") != "example-stream-1" {
				t.Fatalf("start request = %s key=%q", request.Method, request.Header.Get("Idempotency-Key"))
			}
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(openlinker.RunResponse{RunID: "run-stream-1", Status: "running"})
		case "/api/v1/runs/run-stream-1/stream":
			if request.Header.Get("Accept") != "text/event-stream" {
				t.Fatalf("Accept = %q", request.Header.Get("Accept"))
			}
			w.Header().Set("Content-Type", "text/event-stream")
			_, _ = w.Write([]byte("id: 1\nevent: run.message.delta\ndata: {\"text\":\"working\"}\n\n"))
			_, _ = w.Write([]byte("id: 2\nevent: run.completed\ndata: {\"status\":\"success\"}\n\n"))
		default:
			t.Fatalf("unexpected request %s", request.URL.Path)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var output bytes.Buffer
	err := run(ctx, config{
		APIBase: server.URL, UserToken: "ol_user_example", AgentID: "agent-1", Input: "hello stream",
		IdempotencyKey: "example-stream-1",
	}, &output)
	if err != nil {
		t.Fatal(err)
	}
	for _, want := range []string{"开始读取 Run run-stream-1", "run.message.delta", "run.completed"} {
		if !strings.Contains(output.String(), want) {
			t.Fatalf("output %q missing %q", output.String(), want)
		}
	}
}

func TestRunRejectsStreamClosedBeforeTerminal(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api/v1/runs" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(openlinker.RunResponse{RunID: "run-broken", Status: "running"})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		_, _ = w.Write([]byte("id: 1\nevent: run.message.delta\ndata: {}\n\n"))
	}))
	defer server.Close()

	err := run(context.Background(), config{APIBase: server.URL, AgentID: "agent-1", IdempotencyKey: "example-stream-broken"}, &bytes.Buffer{})
	if err == nil || !strings.Contains(err.Error(), "终态事件前断开") {
		t.Fatalf("error = %v", err)
	}
}

func TestRunStopsStreamingWhenContextExpires(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api/v1/runs" {
			w.Header().Set("Content-Type", "application/json")
			_ = json.NewEncoder(w).Encode(openlinker.RunResponse{RunID: "run-timeout", Status: "running"})
			return
		}
		w.Header().Set("Content-Type", "text/event-stream")
		w.WriteHeader(http.StatusOK)
		if flusher, ok := w.(http.Flusher); ok {
			flusher.Flush()
		}
		<-request.Context().Done()
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Millisecond)
	defer cancel()
	err := run(ctx, config{APIBase: server.URL, AgentID: "agent-1", IdempotencyKey: "example-stream-timeout"}, &bytes.Buffer{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}
}
