package openlinker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"
)

const (
	runtimeV2TestNodeID    = "11111111-1111-4111-8111-111111111111"
	runtimeV2TestAgentID   = "22222222-2222-4222-8222-222222222222"
	runtimeV2TestSessionID = "33333333-3333-4333-8333-333333333333"
	runtimeV2TestRunID     = "44444444-4444-4444-8444-444444444444"
	runtimeV2TestAttemptID = "55555555-5555-4555-8555-555555555555"
	runtimeV2TestLeaseID   = "66666666-6666-4666-8666-666666666666"
	runtimeV2TestEventID   = "77777777-7777-4777-8777-777777777777"
	runtimeV2TestResultID  = "88888888-8888-4888-8888-888888888888"
	runtimeV2TestCoreID    = "99999999-9999-4999-8999-999999999999"
)

func TestRuntimeV2HTTPFlowRequiresExplicitAssignmentConfirmation(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	identity := runtimeV2TestIdentity()
	var mu sync.Mutex
	steps := make([]string, 0, 5)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer ol_agent_v2" {
			t.Errorf("authorization = %q", req.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		steps = append(steps, req.URL.Path)
		mu.Unlock()
		switch req.URL.Path {
		case "/api/v1/agent-runtime/v2/sessions":
			var hello RuntimeV2HelloPayload
			decodeRuntimeV2TestBody(t, req, &hello)
			if hello.RuntimeSessionID != runtimeV2TestSessionID {
				t.Errorf("hello = %#v", hello)
			}
			writeRuntimeV2TestJSON(t, w, RuntimeV2ReadyPayload{
				CoreInstanceID: runtimeV2TestCoreID, Features: RuntimeRequiredFeatures(),
				OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
			})
		case "/api/v1/agent-runtime/v2/runs/claim":
			if req.URL.Query().Get("wait") != "12" {
				t.Errorf("wait = %q", req.URL.Query().Get("wait"))
			}
			writeRuntimeV2TestJSON(t, w, RuntimeV2RunAssignedPayload{
				AttemptIdentity: identity, OfferNo: 1, OfferExpiresAt: now.Add(30 * time.Second),
				AttemptDeadlineAt: now.Add(time.Minute), RunDeadlineAt: now.Add(2 * time.Minute),
				Input: map[string]any{"q": "hello"}, NodeEnvelope: "ol_ctx_v2.current.payload.signature",
				AgentInvocationToken: "ol_inv_v2.current.payload.signature",
			})
		case "/api/v1/agent-runtime/v2/runs/" + runtimeV2TestRunID + "/assignment-ack":
			var ack RuntimeV2AssignmentAckPayload
			decodeRuntimeV2TestBody(t, req, &ack)
			writeRuntimeV2TestJSON(t, w, RuntimeV2AssignmentConfirmedPayload{
				AttemptIdentity: ack.AttemptIdentity, AttemptNo: 1, LeaseExpiresAt: now.Add(time.Minute),
			})
		case "/api/v1/agent-runtime/v2/runs/" + runtimeV2TestRunID + "/events":
			var event RuntimeV2RunEventPayload
			decodeRuntimeV2TestBody(t, req, &event)
			writeRuntimeV2TestJSON(t, w, RuntimeV2RunEventAckPayload{
				ClientEventID: event.ClientEventID, ClientEventSeq: event.ClientEventSeq, Sequence: 4,
			})
		case "/api/v1/agent-runtime/v2/runs/" + runtimeV2TestRunID + "/result":
			var result RuntimeV2RunResultPayload
			decodeRuntimeV2TestBody(t, req, &result)
			writeRuntimeV2TestJSON(t, w, RuntimeV2RunResultAckPayload{
				ResultID: result.ResultID, Classification: "success", RunStatus: "success", DispatchState: "terminal",
			})
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	hello := runtimeV2TestHello()
	if _, err = runtimeClient.CreateRuntimeV2Session(context.Background(), hello); err != nil {
		t.Fatal(err)
	}
	assigned, err := runtimeClient.ClaimRuntimeV2Run(context.Background(), 12, RuntimeV2ClaimRequest{
		RuntimeSessionID: hello.RuntimeSessionID, Capacity: 1, Inflight: 0,
	})
	if err != nil || assigned == nil {
		t.Fatalf("claim = %#v, %v", assigned, err)
	}
	// Claim returns only an offer. The SDK exposes a distinct ACK call and does
	// not invoke user code or synthesize execution permission here.
	confirmed, err := runtimeClient.AckRuntimeV2Assignment(context.Background(), RuntimeV2AssignmentAckPayload{
		AttemptIdentity: assigned.AttemptIdentity,
	})
	if err != nil || confirmed.AttemptNo != 1 {
		t.Fatalf("confirm = %#v, %v", confirmed, err)
	}
	eventAck, err := runtimeClient.AppendRuntimeV2Event(context.Background(), RuntimeV2RunEventPayload{
		AttemptIdentity: identity, ClientEventID: runtimeV2TestEventID, ClientEventSeq: 1,
		EventType: "run.progress", Payload: map[string]any{"percent": 50},
	})
	if err != nil || eventAck.Sequence != 4 {
		t.Fatalf("event ACK = %#v, %v", eventAck, err)
	}
	resultAck, err := runtimeClient.FinalizeRuntimeV2Result(context.Background(), RuntimeV2RunResultPayload{
		AttemptIdentity: identity, ResultID: runtimeV2TestResultID, Status: "success",
		Output: map[string]any{"answer": "ok"}, DurationMS: 10, FinalClientEventSeq: 1,
	})
	if err != nil || resultAck.ResultID != runtimeV2TestResultID {
		t.Fatalf("result ACK = %#v, %v", resultAck, err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"/api/v1/agent-runtime/v2/sessions",
		"/api/v1/agent-runtime/v2/runs/claim",
		"/api/v1/agent-runtime/v2/runs/" + runtimeV2TestRunID + "/assignment-ack",
		"/api/v1/agent-runtime/v2/runs/" + runtimeV2TestRunID + "/events",
		"/api/v1/agent-runtime/v2/runs/" + runtimeV2TestRunID + "/result",
	}
	if len(steps) != len(want) {
		t.Fatalf("steps = %#v", steps)
	}
	for index := range want {
		if steps[index] != want[index] {
			t.Fatalf("steps = %#v, want %#v", steps, want)
		}
	}
}

func TestRuntimeV2HTTPRejectsUnknownResponseAndUnstableIDs(t *testing.T) {
	t.Parallel()

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"core_instance_id":"` + runtimeV2TestCoreID + `","features":["lease_fence","assignment_confirm","renew","resume","event_ack","result_ack","cancel","persistent_spool"],"offer_ttl_seconds":30,"lease_ttl_seconds":60,"database_time":"2026-07-11T00:00:00Z","unexpected":true}`))
	}))
	defer server.Close()
	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runtimeClient.CreateRuntimeV2Session(context.Background(), runtimeV2TestHello()); err == nil {
		t.Fatal("unknown response field must fail")
	}
	bad := RuntimeV2RunEventPayload{
		AttemptIdentity: runtimeV2TestIdentity(), ClientEventID: "event-from-clock", ClientEventSeq: 1,
		EventType: "run.progress", Payload: map[string]any{},
	}
	if _, err = runtimeClient.AppendRuntimeV2Event(context.Background(), bad); err == nil {
		t.Fatal("caller must supply a stable UUID event ID")
	}
	if calls != 1 {
		t.Fatalf("invalid Event reached server; calls=%d", calls)
	}
}

func runtimeV2TestHello() RuntimeV2HelloPayload {
	return RuntimeV2HelloPayload{
		NodeID: runtimeV2TestNodeID, AgentID: runtimeV2TestAgentID, WorkerID: "worker-a",
		RuntimeSessionID: runtimeV2TestSessionID, SessionEpoch: 1, NodeVersion: "0.2.0",
		Capacity: 1, Features: RuntimeRequiredFeatures(), ContractDigest: RuntimeContractDigest,
	}
}

func runtimeV2TestIdentity() RuntimeV2AttemptIdentity {
	return RuntimeV2AttemptIdentity{
		RunID: runtimeV2TestRunID, AttemptID: runtimeV2TestAttemptID, LeaseID: runtimeV2TestLeaseID,
		FencingToken: 1, NodeID: runtimeV2TestNodeID, AgentID: runtimeV2TestAgentID,
		WorkerID: "worker-a", RuntimeSessionID: runtimeV2TestSessionID,
	}
}

func decodeRuntimeV2TestBody(t *testing.T, req *http.Request, out any) {
	t.Helper()
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		t.Fatal(err)
	}
}

func writeRuntimeV2TestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}
