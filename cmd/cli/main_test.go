package main

import (
	"bytes"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func TestContextCommandRedactsTokens(t *testing.T) {
	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runCLI([]string{"context"}, strings.NewReader(""), stdout, stderr, testEnv(map[string]string{
		"OPENLINKER_API_BASE":      "http://core.test",
		"OPENLINKER_RUN_ID":        "run-1",
		"OPENLINKER_AGENT_ID":      "agent-1",
		"OPENLINKER_TRACE_ID":      "trace-1",
		"OPENLINKER_TOKEN":         "user-secret",
		"OPENLINKER_RUNTIME_TOKEN": "runtime-secret",
	}))
	if code != 0 {
		t.Fatalf("runCLI() code = %d stderr=%s", code, stderr.String())
	}
	out := stdout.String()
	for _, secret := range []string{"user-secret", "runtime-secret"} {
		if strings.Contains(out, secret) {
			t.Fatalf("context output leaked %q: %s", secret, out)
		}
	}
	if !strings.Contains(out, `"run_id": "run-1"`) || !strings.Contains(out, `"trace_id": "trace-1"`) {
		t.Fatalf("context output missing run context: %s", out)
	}
}

func TestRunCommandSendsUserTokenAndInput(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/run" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer user-token" {
			t.Fatalf("Authorization = %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["agent_id"] != "agent-target" {
			t.Fatalf("agent_id = %v", body["agent_id"])
		}
		input := body["input"].(map[string]any)
		if input["task"] != "hello" {
			t.Fatalf("input = %#v", input)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id": "run-started",
			"status": "success",
			"output": map[string]any{"ok": true},
		})
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runCLI([]string{
		"--api", server.URL,
		"--token", "user-token",
		"run",
		"--agent", "agent-target",
		"--input", `{"task":"hello"}`,
	}, strings.NewReader(""), stdout, stderr, testEnv(nil))
	if code != 0 {
		t.Fatalf("runCLI() code = %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"run_id": "run-started"`) {
		t.Fatalf("stdout = %s", stdout.String())
	}
}

func TestDelegateCommandUsesRuntimeTokenAndEnvParentRun(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodPost || r.URL.Path != "/api/v1/agent-runtime/call-agent" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer runtime-token" {
			t.Fatalf("Authorization = %q", got)
		}
		var body map[string]any
		if err := json.NewDecoder(r.Body).Decode(&body); err != nil {
			t.Fatal(err)
		}
		if body["parent_run_id"] != "parent-run" {
			t.Fatalf("parent_run_id = %v", body["parent_run_id"])
		}
		if body["target_agent_id"] != "agent-child" {
			t.Fatalf("target_agent_id = %v", body["target_agent_id"])
		}
		if body["trace_id"] != "trace-parent" {
			t.Fatalf("trace_id = %v", body["trace_id"])
		}
		input := body["input"].(map[string]any)
		if input["task"] != "delegate this" {
			t.Fatalf("input = %#v", input)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"run_id":        "child-run",
			"status":        "success",
			"parent_run_id": "parent-run",
		})
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runCLI([]string{
		"--api", server.URL,
		"delegate",
		"--agent", "agent-child",
		"--reason", "test delegation",
		"--input", `{"task":"delegate this"}`,
	}, strings.NewReader(""), stdout, stderr, testEnv(map[string]string{
		"OPENLINKER_RUNTIME_TOKEN": "runtime-token",
		"OPENLINKER_RUN_ID":        "parent-run",
		"OPENLINKER_TRACE_ID":      "trace-parent",
	}))
	if code != 0 {
		t.Fatalf("runCLI() code = %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"run_id": "child-run"`) {
		t.Fatalf("stdout = %s", stdout.String())
	}
}

func TestRunsChildrenCommand(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if r.Method != http.MethodGet || r.URL.Path != "/api/v1/runs/parent-run/children" {
			t.Fatalf("unexpected request %s %s", r.Method, r.URL.Path)
		}
		if got := r.Header.Get("Authorization"); got != "Bearer user-token" {
			t.Fatalf("Authorization = %q", got)
		}
		_ = json.NewEncoder(w).Encode(map[string]any{
			"items": []map[string]any{
				{"child_run_id": "child-1", "status": "success"},
			},
		})
	}))
	defer server.Close()

	stdout := &bytes.Buffer{}
	stderr := &bytes.Buffer{}
	code := runCLI([]string{
		"--api", server.URL,
		"--token", "user-token",
		"runs",
		"children",
		"--id", "parent-run",
	}, strings.NewReader(""), stdout, stderr, testEnv(nil))
	if code != 0 {
		t.Fatalf("runCLI() code = %d stderr=%s", code, stderr.String())
	}
	if !strings.Contains(stdout.String(), `"child_run_id": "child-1"`) {
		t.Fatalf("stdout = %s", stdout.String())
	}
}

func TestPayloadTreatsPlainTextAsTextInput(t *testing.T) {
	c := &cli{stdin: strings.NewReader(""), getenv: testEnv(nil)}
	payload, err := c.payload("hello world", "", "")
	if err != nil {
		t.Fatal(err)
	}
	got := payload.(openlinker.JSON)
	if got["text"] != "hello world" {
		t.Fatalf("payload = %#v", payload)
	}
}

func testEnv(values map[string]string) func(string) string {
	return func(key string) string {
		if values == nil {
			return ""
		}
		return values[key]
	}
}
