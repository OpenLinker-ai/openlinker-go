package openlinker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"testing"
	"time"
)

func delegatedTestToken() string {
	return "ol_inv_v2.current." + base64.RawURLEncoding.EncodeToString([]byte(`{"audience":"openlinker.runtime.v2/delegation"}`)) + ".signature"
}

func TestReadDelegatedRunSignsOnlyAttemptAuthority(t *testing.T) {
	authorization := RuntimeCallAgentAuthorization{NodeEnvelope: "ol_ctx_v2.current.payload.signature", AgentInvocationToken: delegatedTestToken(), IdempotencyKey: "read-child"}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		body, _ := io.ReadAll(req.Body)
		if req.Method != "POST" || req.URL.Path != runtimeDelegatedRunReadPath || req.Header.Get("Authorization") != "Bearer "+authorization.AgentInvocationToken {
			t.Error("incorrect delegated read identity")
		}
		proof, _ := BuildRuntimeInvocationProof(authorization.AgentInvocationToken, RuntimeInvocationProofRequest{Method: req.Method, Path: req.URL.Path, Body: body, Context: req.Header.Get(runtimeInvocationHeader), IdempotencyKey: req.Header.Get("Idempotency-Key")})
		if req.Header.Get(runtimeInvocationProofHeader) != proof {
			t.Error("proof does not bind exact request")
		}
		var request map[string]any
		json.Unmarshal(body, &request)
		if len(request) != 1 || request["run_id"] != runtimeTestRunID {
			t.Error("unexpected read payload")
		}
		json.NewEncoder(w).Encode(RuntimeDelegatedRun{RuntimeRunSummary: RuntimeRunSummary{RunID: runtimeTestRunID, Status: RuntimeRunSuccess, DispatchState: RuntimeDispatchTerminal}, Output: map[string]any{"summary": "done"}})
	}))
	defer server.Close()
	client, err := NewRuntime(server.URL, WithAgentToken("long-lived-must-not-be-used"), WithHeader("Authorization", "Bearer must-not-win"))
	if err != nil {
		t.Fatal(err)
	}
	result, err := client.ReadRuntimeDelegatedRun(context.Background(), authorization, runtimeTestRunID)
	if err != nil || result.Output["summary"] != "done" {
		t.Fatalf("result=%v err=%v", result, err)
	}
	old := authorization
	old.AgentInvocationToken = "ol_inv_v2.current.payload.signature"
	if _, err := client.ReadRuntimeDelegatedRun(context.Background(), old, runtimeTestRunID); !errors.Is(err, ErrRuntimeDelegationUnsupported) {
		t.Fatal(err)
	}
}

func TestReadDelegatedRunRejectsAnotherRunResponse(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		json.NewEncoder(w).Encode(RuntimeDelegatedRun{RuntimeRunSummary: RuntimeRunSummary{RunID: runtimeTestTargetAgentID, Status: RuntimeRunSuccess, DispatchState: RuntimeDispatchTerminal}})
	}))
	defer server.Close()
	client, _ := NewRuntime(server.URL, WithAgentToken("agent-token"))
	_, err := client.ReadRuntimeDelegatedRun(context.Background(), RuntimeCallAgentAuthorization{NodeEnvelope: "ol_ctx_v2.current.payload.signature", AgentInvocationToken: delegatedTestToken(), IdempotencyKey: "read-child"}, runtimeTestRunID)
	if !errors.Is(err, ErrRuntimeProtocolMismatch) {
		t.Fatal(err)
	}
}

type delegatedWorkerFake struct {
	*fakeRuntimeClient
	started chan struct{}
}

func (fake *delegatedWorkerFake) ReadRuntimeDelegatedRun(ctx context.Context, auth RuntimeCallAgentAuthorization, runID string) (*RuntimeDelegatedRun, error) {
	close(fake.started)
	<-ctx.Done()
	return nil, ctx.Err()
}

func TestDelegatedReadStopsWithAttemptAndRejectsFinishedAttempt(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	fake := &delegatedWorkerFake{newFakeRuntimeClient(), make(chan struct{})}
	worker := &RuntimeWorker{runtimeClient: fake}
	attempt := &activeRuntimeAttempt{ctx: ctx}
	done := make(chan error, 1)
	go func() {
		_, err := worker.readDelegatedRunForAttempt(context.Background(), attempt, runtimeTestRunID)
		done <- err
	}()
	<-fake.started
	cancel()
	select {
	case err := <-done:
		if !errors.Is(err, context.Canceled) {
			t.Fatal(err)
		}
	case <-time.After(time.Second):
		t.Fatal("read ignored cancellation")
	}
	attempt.finished.Store(true)
	if _, err := worker.readDelegatedRunForAttempt(context.Background(), attempt, runtimeTestRunID); !errors.Is(err, context.Canceled) {
		t.Fatal(err)
	}
	if (RuntimeContext{}).CanReadDelegatedRuns() {
		t.Fatal("empty context advertises delegation")
	}
}
