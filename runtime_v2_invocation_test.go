package openlinker

import (
	"context"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
)

const runtimeV2TestTargetAgentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

func TestBuildRuntimeV2InvocationProofMatchesCoreVector(t *testing.T) {
	t.Parallel()

	request := RuntimeV2InvocationProofRequest{
		Method:         http.MethodPost,
		Path:           runtimeV2CallAgentPath,
		IdempotencyKey: "delegation-42<&",
		Context:        "ol_ctx_v2.current.payload.signature",
		Body:           []byte(`{"target_agent_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","input":{"q":"hello"},"reason":"need data"}`),
	}
	proof, err := BuildRuntimeV2InvocationProof("ol_inv_v2.current.payload.signature", request)
	if err != nil {
		t.Fatal(err)
	}
	// Generated once with Core's BuildRuntimeInvocationProof. This SDK test
	// intentionally does not import Core.
	const want = "NPUA_HnpwGbee56_RoGEAUZl-A8j1ASRsSJU2fBaJk0"
	if proof != want {
		t.Fatalf("proof = %q, want %q", proof, want)
	}

	mutations := []func(*RuntimeV2InvocationProofRequest){
		func(value *RuntimeV2InvocationProofRequest) { value.Body = append(value.Body, ' ') },
		func(value *RuntimeV2InvocationProofRequest) { value.IdempotencyKey += "-other" },
		func(value *RuntimeV2InvocationProofRequest) { value.Context += "x" },
		func(value *RuntimeV2InvocationProofRequest) { value.Path += "/other" },
	}
	for index, mutate := range mutations {
		changed := request
		changed.Body = append([]byte(nil), request.Body...)
		mutate(&changed)
		got, buildErr := BuildRuntimeV2InvocationProof("ol_inv_v2.current.payload.signature", changed)
		if buildErr != nil {
			t.Fatalf("mutation %d: %v", index, buildErr)
		}
		if got == proof {
			t.Fatalf("mutation %d did not change proof", index)
		}
	}
	otherTokenProof, err := BuildRuntimeV2InvocationProof("ol_inv_v2.other.payload.signature", request)
	if err != nil {
		t.Fatal(err)
	}
	if otherTokenProof == proof {
		t.Fatal("invocation token did not change proof key")
	}
}

func TestCallRuntimeV2AgentSignsAndSendsTheSameBody(t *testing.T) {
	t.Parallel()

	authorization := RuntimeV2CallAgentAuthorization{
		NodeEnvelope:         "ol_ctx_v2.current.payload.signature",
		AgentInvocationToken: "ol_inv_v2.current.payload.signature",
		IdempotencyKey:       "delegation-42",
	}
	changingValue := &runtimeV2ChangingJSON{}
	request := RuntimeV2CallAgentRequest{
		TargetAgentID: runtimeV2TestTargetAgentID,
		Input:         map[string]any{"q": "hello", "nonce": changingValue},
		Metadata:      map[string]any{"trace": "sdk"},
		Reason:        "need data",
	}
	expectedBody := []byte(`{"target_agent_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","input":{"nonce":{"value":1},"q":"hello"},"metadata":{"trace":"sdk"},"reason":"need data"}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.URL.EscapedPath() != runtimeV2CallAgentPath {
			t.Errorf("request = %s %s", req.Method, req.URL.EscapedPath())
		}
		if got := req.Header.Values("Authorization"); len(got) != 1 || got[0] != "Bearer "+authorization.AgentInvocationToken {
			t.Errorf("Authorization = %#v", got)
		}
		if got := req.Header.Get("Idempotency-Key"); got != authorization.IdempotencyKey {
			t.Errorf("Idempotency-Key = %q", got)
		}
		if got := req.Header.Get(runtimeV2InvocationHeader); got != authorization.NodeEnvelope {
			t.Errorf("context = %q", got)
		}
		body, readErr := io.ReadAll(req.Body)
		if readErr != nil {
			t.Fatal(readErr)
		}
		if string(body) != string(expectedBody) {
			t.Errorf("body = %s, want %s", body, expectedBody)
		}
		if calls := changingValue.calls.Load(); calls != 1 {
			t.Errorf("request body marshaled %d times", calls)
		}
		wantProof, buildErr := BuildRuntimeV2InvocationProof(authorization.AgentInvocationToken, RuntimeV2InvocationProofRequest{
			Method: req.Method, Path: req.URL.EscapedPath(), IdempotencyKey: req.Header.Get("Idempotency-Key"),
			Context: req.Header.Get(runtimeV2InvocationHeader), Body: body,
		})
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		if got := req.Header.Get(runtimeV2InvocationProofHeader); got != wantProof {
			t.Errorf("proof = %q, want %q", got, wantProof)
		}
		w.Header().Set("Content-Type", "application/json")
		writeRuntimeV2TestJSON(t, w, RuntimeV2RunSummary{
			RunID: runtimeV2TestRunID, Status: RuntimeV2RunRunning, DispatchState: RuntimeV2DispatchPending,
		})
	}))
	defer server.Close()

	runtimeClient, err := NewRuntime(
		server.URL,
		WithRuntimeToken("long-lived-agent-token"),
		WithHeader("Authorization", "Bearer caller-default-must-not-win"),
		WithHeader("Idempotency-Key", "caller-default-must-not-win"),
		WithHeader(runtimeV2InvocationHeader, "caller-default-must-not-win"),
	)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := runtimeClient.CallRuntimeV2Agent(context.Background(), authorization, request)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RunID != runtimeV2TestRunID || summary.Status != RuntimeV2RunRunning || summary.DispatchState != RuntimeV2DispatchPending {
		t.Fatalf("summary = %#v", summary)
	}
}

type runtimeV2ChangingJSON struct {
	calls atomic.Int32
}

func (value *runtimeV2ChangingJSON) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"value":%d}`, value.calls.Add(1))), nil
}

func TestCallRuntimeV2AgentRejectsInvalidAuthorityAndSummary(t *testing.T) {
	t.Parallel()

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		writeRuntimeV2TestJSON(t, w, RuntimeV2RunSummary{
			RunID: runtimeV2TestRunID, Status: RuntimeV2RunSuccess, DispatchState: RuntimeV2DispatchPending,
		})
	}))
	defer server.Close()
	runtimeClient, err := NewRuntime(server.URL, WithRuntimeToken("long-lived-agent-token"))
	if err != nil {
		t.Fatal(err)
	}
	request := RuntimeV2CallAgentRequest{TargetAgentID: runtimeV2TestTargetAgentID, Input: map[string]any{}}
	authorization := RuntimeV2CallAgentAuthorization{
		NodeEnvelope:         "ol_ctx_v2.current.payload.signature",
		AgentInvocationToken: "ol_inv_v2.current.payload.signature",
		IdempotencyKey:       "delegation-42",
	}
	invalid := authorization
	invalid.IdempotencyKey = "\n"
	if _, err = runtimeClient.CallRuntimeV2Agent(context.Background(), invalid, request); err == nil {
		t.Fatal("invalid authority reached transport")
	}
	invalid.IdempotencyKey = " delegation-42 "
	if _, err = runtimeClient.CallRuntimeV2Agent(context.Background(), invalid, request); err == nil {
		t.Fatal("header-normalized idempotency key reached transport")
	}
	if calls != 0 {
		t.Fatalf("invalid authority calls = %d", calls)
	}
	if _, err = runtimeClient.CallRuntimeV2Agent(context.Background(), authorization, request); err == nil {
		t.Fatal("inconsistent run summary must fail")
	}
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
}
