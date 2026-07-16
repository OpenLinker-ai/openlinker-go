package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func TestRunStartsAndPollsUntilTerminal(t *testing.T) {
	var polls atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case request.Method == http.MethodPost && request.URL.Path == "/api/v1/runs":
			if request.Header.Get("Idempotency-Key") != "example-async-1" {
				t.Fatalf("Idempotency-Key = %q", request.Header.Get("Idempotency-Key"))
			}
			w.WriteHeader(http.StatusAccepted)
			_ = json.NewEncoder(w).Encode(openlinker.RunResponse{RunID: "run-async-1", Status: "queued"})
		case request.Method == http.MethodGet && request.URL.Path == "/api/v1/runs/run-async-1":
			status := "running"
			if polls.Add(1) == 2 {
				status = "success"
			}
			_ = json.NewEncoder(w).Encode(openlinker.RunResponse{RunID: "run-async-1", Status: status})
		default:
			t.Fatalf("unexpected request %s %s", request.Method, request.URL.Path)
		}
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	var output bytes.Buffer
	err := run(ctx, config{
		APIBase: server.URL, UserToken: "ol_user_example", AgentID: "agent-1", Input: "hello async",
		IdempotencyKey: "example-async-1", PollInterval: time.Millisecond,
	}, &output)
	if err != nil {
		t.Fatal(err)
	}
	if polls.Load() != 2 || !strings.Contains(output.String(), "已提交 Run run-async-1") || !strings.Contains(output.String(), `"status": "success"`) {
		t.Fatalf("polls=%d output=%s", polls.Load(), output.String())
	}
}

func TestRunStopsPollingWhenContextExpires(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		if request.URL.Path == "/api/v1/runs" {
			_ = json.NewEncoder(w).Encode(openlinker.RunResponse{RunID: "run-timeout", Status: "queued"})
			return
		}
		_ = json.NewEncoder(w).Encode(openlinker.RunResponse{RunID: "run-timeout", Status: "running"})
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
	defer cancel()
	err := run(ctx, config{
		APIBase: server.URL, AgentID: "agent-1", IdempotencyKey: "example-async-timeout",
		PollInterval: time.Second,
	}, &bytes.Buffer{})
	if !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("error = %v, want context deadline", err)
	}
}
