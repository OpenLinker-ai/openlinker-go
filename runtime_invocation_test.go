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

const runtimeTestTargetAgentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"

func TestBuildRuntimeInvocationProofMatchesCoreVector(t *testing.T) {
	t.Parallel()

	request := RuntimeInvocationProofRequest{
		Method:         http.MethodPost,
		Path:           runtimeCallAgentPath,
		IdempotencyKey: "delegation-42<&",
		Context:        "ol_ctx_v2.current.payload.signature",
		Body:           []byte(`{"target_agent_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","input":{"q":"hello"},"reason":"need data"}`),
	}
	proof, err := BuildRuntimeInvocationProof("ol_inv_v2.current.payload.signature", request)
	if err != nil {
		t.Fatal(err)
	}
	// Generated once with Core's BuildRuntimeInvocationProof. This SDK test
	// intentionally does not import Core.
	const want = "lBuoEqAJKl9ujEr72b0oR3cuuoqJPqCs1vkABcw6zA0"
	if proof != want {
		t.Fatalf("proof = %q, want %q", proof, want)
	}

	mutations := []func(*RuntimeInvocationProofRequest){
		func(value *RuntimeInvocationProofRequest) { value.Body = append(value.Body, ' ') },
		func(value *RuntimeInvocationProofRequest) { value.IdempotencyKey += "-other" },
		func(value *RuntimeInvocationProofRequest) { value.Context += "x" },
		func(value *RuntimeInvocationProofRequest) { value.Path += "/other" },
	}
	for index, mutate := range mutations {
		changed := request
		changed.Body = append([]byte(nil), request.Body...)
		mutate(&changed)
		got, buildErr := BuildRuntimeInvocationProof("ol_inv_v2.current.payload.signature", changed)
		if buildErr != nil {
			t.Fatalf("mutation %d: %v", index, buildErr)
		}
		if got == proof {
			t.Fatalf("mutation %d did not change proof", index)
		}
	}
	otherTokenProof, err := BuildRuntimeInvocationProof("ol_inv_v2.other.payload.signature", request)
	if err != nil {
		t.Fatal(err)
	}
	if otherTokenProof == proof {
		t.Fatal("invocation token did not change proof key")
	}
}

func TestCallRuntimeAgentSignsAndSendsTheSameBody(t *testing.T) {
	t.Parallel()

	authorization := RuntimeCallAgentAuthorization{
		NodeEnvelope:         "ol_ctx_v2.current.payload.signature",
		AgentInvocationToken: "ol_inv_v2.current.payload.signature",
		IdempotencyKey:       "delegation-42",
	}
	changingValue := &runtimeChangingJSON{}
	request := RuntimeCallAgentRequest{
		TargetAgentID: runtimeTestTargetAgentID,
		Input:         map[string]any{"q": "hello", "nonce": changingValue},
		Metadata:      map[string]any{"trace": "sdk"},
		Reason:        "need data",
	}
	expectedBody := []byte(`{"target_agent_id":"aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa","input":{"nonce":{"value":1},"q":"hello"},"metadata":{"trace":"sdk"},"reason":"need data"}`)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodPost || req.URL.EscapedPath() != runtimeCallAgentPath {
			t.Errorf("request = %s %s", req.Method, req.URL.EscapedPath())
		}
		if got := req.Header.Values("Authorization"); len(got) != 1 || got[0] != "Bearer "+authorization.AgentInvocationToken {
			t.Errorf("Authorization = %#v", got)
		}
		if got := req.Header.Get("Idempotency-Key"); got != authorization.IdempotencyKey {
			t.Errorf("Idempotency-Key = %q", got)
		}
		if got := req.Header.Get(runtimeInvocationHeader); got != authorization.NodeEnvelope {
			t.Errorf("context = %q", got)
		}
		if got := req.Header.Get(RuntimeAttachmentHeader); got != "" {
			t.Errorf("call-agent attachment header = %q", got)
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
		wantProof, buildErr := BuildRuntimeInvocationProof(authorization.AgentInvocationToken, RuntimeInvocationProofRequest{
			Method: req.Method, Path: req.URL.EscapedPath(), IdempotencyKey: req.Header.Get("Idempotency-Key"),
			Context: req.Header.Get(runtimeInvocationHeader), Body: body,
		})
		if buildErr != nil {
			t.Fatal(buildErr)
		}
		if got := req.Header.Get(runtimeInvocationProofHeader); got != wantProof {
			t.Errorf("proof = %q, want %q", got, wantProof)
		}
		w.Header().Set("Content-Type", "application/json")
		writeRuntimeTestJSON(t, w, RuntimeRunSummary{
			RunID: runtimeTestRunID, Status: RuntimeRunRunning, DispatchState: RuntimeDispatchPending,
		})
	}))
	defer server.Close()

	runtimeClient, err := NewRuntime(
		server.URL,
		WithAgentToken("long-lived-agent-token"),
		WithHeader("Authorization", "Bearer caller-default-must-not-win"),
		WithHeader("Idempotency-Key", "caller-default-must-not-win"),
		WithHeader(runtimeInvocationHeader, "caller-default-must-not-win"),
	)
	if err != nil {
		t.Fatal(err)
	}
	summary, err := runtimeClient.CallRuntimeAgent(context.Background(), authorization, request)
	if err != nil {
		t.Fatal(err)
	}
	if summary.RunID != runtimeTestRunID || summary.Status != RuntimeRunRunning || summary.DispatchState != RuntimeDispatchPending {
		t.Fatalf("summary = %#v", summary)
	}
}

type runtimeChangingJSON struct {
	calls atomic.Int32
}

func (value *runtimeChangingJSON) MarshalJSON() ([]byte, error) {
	return []byte(fmt.Sprintf(`{"value":%d}`, value.calls.Add(1))), nil
}

func TestCallRuntimeAgentRejectsInvalidAuthorityAndSummary(t *testing.T) {
	t.Parallel()

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		writeRuntimeTestJSON(t, w, RuntimeRunSummary{
			RunID: runtimeTestRunID, Status: RuntimeRunSuccess, DispatchState: RuntimeDispatchPending,
		})
	}))
	defer server.Close()
	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("long-lived-agent-token"))
	if err != nil {
		t.Fatal(err)
	}
	request := RuntimeCallAgentRequest{TargetAgentID: runtimeTestTargetAgentID, Input: map[string]any{}}
	authorization := RuntimeCallAgentAuthorization{
		NodeEnvelope:         "ol_ctx_v2.current.payload.signature",
		AgentInvocationToken: "ol_inv_v2.current.payload.signature",
		IdempotencyKey:       "delegation-42",
	}
	invalid := authorization
	invalid.IdempotencyKey = "\n"
	if _, err = runtimeClient.CallRuntimeAgent(context.Background(), invalid, request); err == nil {
		t.Fatal("invalid authority reached transport")
	}
	invalid.IdempotencyKey = " delegation-42 "
	if _, err = runtimeClient.CallRuntimeAgent(context.Background(), invalid, request); err == nil {
		t.Fatal("header-normalized idempotency key reached transport")
	}
	if calls != 0 {
		t.Fatalf("invalid authority calls = %d", calls)
	}
	if _, err = runtimeClient.CallRuntimeAgent(context.Background(), authorization, request); err == nil {
		t.Fatal("inconsistent run summary must fail")
	}
	if calls != 1 {
		t.Fatalf("calls = %d", calls)
	}
}
