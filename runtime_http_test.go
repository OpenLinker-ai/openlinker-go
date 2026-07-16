package openlinker

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"reflect"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const (
	runtimeTestNodeID       = "11111111-1111-4111-8111-111111111111"
	runtimeTestAgentID      = "22222222-2222-4222-8222-222222222222"
	runtimeTestSessionID    = "33333333-3333-4333-8333-333333333333"
	runtimeTestRunID        = "44444444-4444-4444-8444-444444444444"
	runtimeTestAttemptID    = "55555555-5555-4555-8555-555555555555"
	runtimeTestLeaseID      = "66666666-6666-4666-8666-666666666666"
	runtimeTestEventID      = "77777777-7777-4777-8777-777777777777"
	runtimeTestResultID     = "88888888-8888-4888-8888-888888888888"
	runtimeTestCoreID       = "99999999-9999-4999-8999-999999999999"
	runtimeTestAttachmentID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
)

func TestRuntimeFallbackReasonHeaderIsBoundedAndCreateOnly(t *testing.T) {
	var mu sync.Mutex
	var observed []string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		observed = append(observed, request.URL.Path+"="+request.Header.Get(RuntimeFallbackReasonHeader))
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		writeRuntimeTestJSON(t, w, RuntimeReadyPayload{
			CoreInstanceID: runtimeTestCoreID, AttachmentID: runtimeTestAttachmentID,
			Features: RuntimeRequiredFeatures(), OfferTTLSeconds: 30, LeaseTTLSeconds: 60,
			DatabaseTime: time.Now().UTC(),
		})
	}))
	defer server.Close()
	runtimeClient, err := NewRuntime(
		server.URL,
		WithAgentToken("ol_agent_v2"),
		WithHeader(RuntimeFallbackReasonHeader, "private_network_error"),
	)
	if err != nil {
		t.Fatal(err)
	}
	for _, reason := range []runtimeFallbackReason{
		runtimeFallbackExplicit,
		runtimeFallbackWebSocketUnavailable,
		runtimeFallbackPolicyForced,
		runtimeFallbackRecovery,
	} {
		ctx := withRuntimeFallbackReason(context.Background(), reason)
		if _, err = runtimeClient.CreateRuntimeSession(ctx, runtimeTestHello()); err != nil {
			t.Fatal(err)
		}
	}
	invalidContext := context.WithValue(context.Background(), runtimeFallbackReasonContextKey{}, runtimeFallbackReason("private_network_error"))
	if _, err = runtimeClient.CreateRuntimeSession(invalidContext, runtimeTestHello()); err != nil {
		t.Fatal(err)
	}
	if _, err = runtimeClient.HeartbeatRuntimeSession(withRuntimeFallbackReason(context.Background(), runtimeFallbackRecovery), runtimeTestHello()); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"/api/v1/agent-runtime/sessions=explicit",
		"/api/v1/agent-runtime/sessions=websocket_unavailable",
		"/api/v1/agent-runtime/sessions=policy_forced",
		"/api/v1/agent-runtime/sessions=recovery",
		"/api/v1/agent-runtime/sessions=",
		"/api/v1/agent-runtime/sessions/" + runtimeTestSessionID + "/heartbeat=",
	}
	if !reflect.DeepEqual(observed, want) {
		t.Fatalf("fallback reason headers = %#v, want %#v", observed, want)
	}
}

func TestRuntimeHTTPDrainUsesAttachmentAndReturnsCorePersistedState(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch request.URL.Path {
		case "/api/v1/agent-runtime/sessions":
			writeRuntimeTestJSON(t, w, RuntimeReadyPayload{
				CoreInstanceID: runtimeTestCoreID, AttachmentID: runtimeTestAttachmentID,
				Features: RuntimeRequiredFeatures(), OfferTTLSeconds: 30, LeaseTTLSeconds: 60,
				DatabaseTime: now,
			})
		case "/api/v1/agent-runtime/sessions/" + runtimeTestSessionID + "/drain":
			if got := request.Header.Get(RuntimeAttachmentHeader); got != runtimeTestAttachmentID {
				t.Errorf("drain attachment = %q", got)
			}
			var drain RuntimeDrainPayload
			decodeRuntimeTestBody(t, request, &drain)
			if drain.Capacity != 0 || drain.Inflight != 2 || drain.ReasonCode != "DEPLOYMENT" {
				t.Errorf("drain request = %#v", drain)
			}
			writeRuntimeTestJSON(t, w, RuntimeDrainPayload{
				DeadlineAt: drain.DeadlineAt.Add(-time.Second),
				ReasonCode: "FIRST_WRITER_REASON",
				Capacity:   0,
				Inflight:   3,
			})
		default:
			http.NotFound(w, request)
		}
	}))
	defer server.Close()
	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runtimeClient.CreateRuntimeSession(context.Background(), runtimeTestHello()); err != nil {
		t.Fatal(err)
	}
	response, err := runtimeClient.DrainRuntimeSession(context.Background(), runtimeTestSessionID, RuntimeDrainPayload{
		DeadlineAt: now.Add(time.Minute), ReasonCode: "DEPLOYMENT", Capacity: 0, Inflight: 2,
	})
	if err != nil {
		t.Fatal(err)
	}
	if response.ReasonCode != "FIRST_WRITER_REASON" || response.Inflight != 3 || response.Capacity != 0 {
		t.Fatalf("authoritative drain = %#v", response)
	}
}

func TestRuntimeHTTPFlowRequiresExplicitAssignmentConfirmation(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	identity := runtimeTestIdentity()
	var mu sync.Mutex
	steps := make([]string, 0, 7)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer ol_agent_v2" {
			t.Errorf("authorization = %q", req.Header.Get("Authorization"))
		}
		w.Header().Set("Content-Type", "application/json")
		mu.Lock()
		steps = append(steps, req.URL.Path)
		mu.Unlock()
		switch req.URL.Path {
		case "/api/v1/agent-runtime/sessions":
			if got := req.Header.Get(RuntimeAttachmentHeader); got != "" {
				t.Errorf("create attachment header = %q", got)
			}
			var hello RuntimeHelloPayload
			decodeRuntimeTestBody(t, req, &hello)
			if hello.RuntimeSessionID != runtimeTestSessionID {
				t.Errorf("hello = %#v", hello)
			}
			writeRuntimeTestJSON(t, w, RuntimeReadyPayload{
				CoreInstanceID: runtimeTestCoreID, AttachmentID: runtimeTestAttachmentID, Features: RuntimeRequiredFeatures(),
				OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
			})
		case "/api/v1/agent-runtime/sessions/" + runtimeTestSessionID + "/heartbeat":
			if got := req.Header.Get(RuntimeAttachmentHeader); got != runtimeTestAttachmentID {
				t.Errorf("heartbeat attachment header = %q", got)
			}
			var heartbeat RuntimeHelloPayload
			decodeRuntimeTestBody(t, req, &heartbeat)
			if !reflect.DeepEqual(heartbeat, runtimeTestHello()) {
				t.Errorf("heartbeat = %#v", heartbeat)
			}
			writeRuntimeTestJSON(t, w, RuntimeReadyPayload{
				CoreInstanceID: runtimeTestCoreID, AttachmentID: runtimeTestAttachmentID, Features: RuntimeRequiredFeatures(),
				OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
			})
		case "/api/v1/agent-runtime/sessions/" + runtimeTestSessionID + "/close":
			var closeRequest RuntimeSessionCloseRequest
			decodeRuntimeTestBody(t, req, &closeRequest)
			if closeRequest.RuntimeSessionID != runtimeTestSessionID || closeRequest.Status != "offline" || closeRequest.Reason != "process restart" {
				t.Errorf("close request = %#v", closeRequest)
			}
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/agent-runtime/runs/claim":
			if req.URL.Query().Get("wait") != "12" {
				t.Errorf("wait = %q", req.URL.Query().Get("wait"))
			}
			writeRuntimeTestJSON(t, w, RuntimeRunAssignedPayload{
				AttemptIdentity: identity, OfferNo: 1, OfferExpiresAt: now.Add(30 * time.Second),
				AttemptDeadlineAt: now.Add(time.Minute), RunDeadlineAt: now.Add(2 * time.Minute),
				Input: map[string]any{"q": "hello"}, NodeEnvelope: "ol_ctx_v2.current.payload.signature",
				AgentInvocationToken: "ol_inv_v2.current.payload.signature",
			})
		case "/api/v1/agent-runtime/runs/" + runtimeTestRunID + "/assignment-ack":
			var ack RuntimeAssignmentAckPayload
			decodeRuntimeTestBody(t, req, &ack)
			writeRuntimeTestJSON(t, w, RuntimeAssignmentConfirmedPayload{
				AttemptIdentity: ack.AttemptIdentity, AttemptNo: 1, LeaseExpiresAt: now.Add(time.Minute),
			})
		case "/api/v1/agent-runtime/runs/" + runtimeTestRunID + "/events":
			var event RuntimeRunEventPayload
			decodeRuntimeTestBody(t, req, &event)
			writeRuntimeTestJSON(t, w, RuntimeRunEventAckPayload{
				ClientEventID: event.ClientEventID, ClientEventSeq: event.ClientEventSeq, Sequence: 4,
			})
		case "/api/v1/agent-runtime/runs/" + runtimeTestRunID + "/result":
			var result RuntimeRunResultPayload
			decodeRuntimeTestBody(t, req, &result)
			writeRuntimeTestJSON(t, w, RuntimeRunResultAckPayload{
				ResultID: result.ResultID, Classification: "success", RunStatus: "success", DispatchState: "terminal",
			})
		default:
			http.NotFound(w, req)
		}
		if req.URL.Path != "/api/v1/agent-runtime/sessions" && req.Header.Get(RuntimeAttachmentHeader) != runtimeTestAttachmentID {
			t.Errorf("%s attachment header = %q", req.URL.Path, req.Header.Get(RuntimeAttachmentHeader))
		}
	}))
	defer server.Close()

	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	hello := runtimeTestHello()
	if _, err = runtimeClient.CreateRuntimeSession(context.Background(), hello); err != nil {
		t.Fatal(err)
	}
	if _, err = runtimeClient.HeartbeatRuntimeSession(context.Background(), hello); err != nil {
		t.Fatal(err)
	}
	assigned, err := runtimeClient.ClaimRuntimeRun(context.Background(), 12, RuntimeClaimRequest{
		RuntimeSessionID: hello.RuntimeSessionID, Capacity: 1, Inflight: 0,
	})
	if err != nil || assigned == nil {
		t.Fatalf("claim = %#v, %v", assigned, err)
	}
	// Claim returns only an offer. The SDK exposes a distinct ACK call and does
	// not invoke user code or synthesize execution permission here.
	confirmed, err := runtimeClient.AckRuntimeAssignment(context.Background(), RuntimeAssignmentAckPayload{
		AttemptIdentity: assigned.AttemptIdentity,
	})
	if err != nil || confirmed.AttemptNo != 1 {
		t.Fatalf("confirm = %#v, %v", confirmed, err)
	}
	eventAck, err := runtimeClient.AppendRuntimeEvent(context.Background(), RuntimeRunEventPayload{
		AttemptIdentity: identity, ClientEventID: runtimeTestEventID, ClientEventSeq: 1,
		EventType: "run.progress", Payload: map[string]any{"percent": 50},
	})
	if err != nil || eventAck.Sequence != 4 {
		t.Fatalf("event ACK = %#v, %v", eventAck, err)
	}
	resultAck, err := runtimeClient.FinalizeRuntimeResult(context.Background(), RuntimeRunResultPayload{
		AttemptIdentity: identity, ResultID: runtimeTestResultID, Status: "success",
		Output: map[string]any{"answer": "ok"}, DurationMS: 10, FinalClientEventSeq: 1,
	})
	if err != nil || resultAck.ResultID != runtimeTestResultID {
		t.Fatalf("result ACK = %#v, %v", resultAck, err)
	}
	if err = runtimeClient.CloseRuntimeSession(context.Background(), RuntimeSessionCloseRequest{
		NodeID: runtimeTestNodeID, AgentID: runtimeTestAgentID, WorkerID: "worker-a",
		RuntimeSessionID: runtimeTestSessionID, SessionEpoch: 1,
		Status: "offline", Reason: "process restart",
	}); err != nil {
		t.Fatal(err)
	}

	mu.Lock()
	defer mu.Unlock()
	want := []string{
		"/api/v1/agent-runtime/sessions",
		"/api/v1/agent-runtime/sessions/" + runtimeTestSessionID + "/heartbeat",
		"/api/v1/agent-runtime/runs/claim",
		"/api/v1/agent-runtime/runs/" + runtimeTestRunID + "/assignment-ack",
		"/api/v1/agent-runtime/runs/" + runtimeTestRunID + "/events",
		"/api/v1/agent-runtime/runs/" + runtimeTestRunID + "/result",
		"/api/v1/agent-runtime/sessions/" + runtimeTestSessionID + "/close",
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

func TestRuntimeHTTPCloseRequiresNoContent(t *testing.T) {
	t.Parallel()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
	}))
	defer server.Close()
	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient.attachmentMu.Lock()
	runtimeClient.attachmentID = runtimeTestAttachmentID
	runtimeClient.attachmentMu.Unlock()
	err = runtimeClient.CloseRuntimeSession(context.Background(), RuntimeSessionCloseRequest{
		NodeID: runtimeTestNodeID, AgentID: runtimeTestAgentID, WorkerID: "worker-a",
		RuntimeSessionID: runtimeTestSessionID, SessionEpoch: 1,
		Status: "closed", Reason: "operator shutdown",
	})
	if err == nil {
		t.Fatal("runtime session close accepted a non-204 response")
	}
}

func TestRuntimeHTTPHeartbeatRejectsAttachmentRotation(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	rotatedAttachmentID := "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/api/v1/agent-runtime/sessions":
			writeRuntimeTestJSON(t, w, RuntimeReadyPayload{
				CoreInstanceID: runtimeTestCoreID, AttachmentID: runtimeTestAttachmentID,
				Features: RuntimeRequiredFeatures(), OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
			})
		case "/api/v1/agent-runtime/sessions/" + runtimeTestSessionID + "/heartbeat":
			if got := req.Header.Get(RuntimeAttachmentHeader); got != runtimeTestAttachmentID {
				t.Errorf("heartbeat attachment header = %q", got)
			}
			writeRuntimeTestJSON(t, w, RuntimeReadyPayload{
				CoreInstanceID: runtimeTestCoreID, AttachmentID: rotatedAttachmentID,
				Features: RuntimeRequiredFeatures(), OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
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
	if _, err = runtimeClient.CreateRuntimeSession(context.Background(), runtimeTestHello()); err != nil {
		t.Fatal(err)
	}
	if _, err = runtimeClient.HeartbeatRuntimeSession(context.Background(), runtimeTestHello()); err == nil {
		t.Fatal("heartbeat accepted a rotated attachment generation")
	}
	runtimeClient.attachmentMu.RLock()
	got := runtimeClient.attachmentID
	runtimeClient.attachmentMu.RUnlock()
	if got != runtimeTestAttachmentID {
		t.Fatalf("attachment after rejected heartbeat = %q", got)
	}
}

func TestRuntimeHTTPSerializesConcurrentSessionCreation(t *testing.T) {
	firstStarted := make(chan struct{})
	secondStarted := make(chan struct{})
	releaseFirst := make(chan struct{})
	var calls atomic.Int32
	now := time.Now().UTC().Truncate(time.Millisecond)
	secondAttachmentID := "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Path != "/api/v1/agent-runtime/sessions" {
			http.NotFound(w, req)
			return
		}
		call := calls.Add(1)
		attachmentID := runtimeTestAttachmentID
		if call == 1 {
			close(firstStarted)
			<-releaseFirst
		} else {
			attachmentID = secondAttachmentID
			if call == 2 {
				close(secondStarted)
			}
		}
		w.Header().Set("Content-Type", "application/json")
		writeRuntimeTestJSON(t, w, RuntimeReadyPayload{
			CoreInstanceID: runtimeTestCoreID, AttachmentID: attachmentID,
			Features: RuntimeRequiredFeatures(), OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
		})
	}))
	defer server.Close()

	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	firstDone := make(chan error, 1)
	secondDone := make(chan error, 1)
	go func() {
		_, createErr := runtimeClient.CreateRuntimeSession(context.Background(), runtimeTestHello())
		firstDone <- createErr
	}()
	<-firstStarted
	go func() {
		_, createErr := runtimeClient.CreateRuntimeSession(context.Background(), runtimeTestHello())
		secondDone <- createErr
	}()
	select {
	case <-secondStarted:
		t.Fatal("second create crossed the attachment lifecycle gate")
	case <-time.After(100 * time.Millisecond):
	}
	close(releaseFirst)
	if err = <-firstDone; err != nil {
		t.Fatal(err)
	}
	if err = <-secondDone; err != nil {
		t.Fatal(err)
	}
	runtimeClient.attachmentMu.RLock()
	got := runtimeClient.attachmentID
	runtimeClient.attachmentMu.RUnlock()
	if got != secondAttachmentID {
		t.Fatalf("final attachment = %q, want %q", got, secondAttachmentID)
	}
}

func TestRuntimeHTTPHeartbeatSharesPullGenerationWhileLifecycleRotationWaits(t *testing.T) {
	claimStarted := make(chan struct{})
	commandsStarted := make(chan struct{})
	heartbeatReached := make(chan struct{})
	closeReached := make(chan struct{})
	releasePulls := make(chan struct{})
	var releaseOnce sync.Once
	t.Cleanup(func() { releaseOnce.Do(func() { close(releasePulls) }) })
	now := time.Now().UTC().Truncate(time.Millisecond)

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get(RuntimeAttachmentHeader) != runtimeTestAttachmentID {
			t.Errorf("%s attachment header = %q", req.URL.Path, req.Header.Get(RuntimeAttachmentHeader))
		}
		switch req.URL.Path {
		case "/api/v1/agent-runtime/runs/claim":
			close(claimStarted)
			<-releasePulls
			w.WriteHeader(http.StatusNoContent)
		case "/api/v1/agent-runtime/commands":
			close(commandsStarted)
			<-releasePulls
			w.Header().Set("Content-Type", "application/json")
			writeRuntimeTestJSON(t, w, RuntimeCommandsResponse{Commands: []RuntimePendingCommand{}, DatabaseTime: now})
		case "/api/v1/agent-runtime/sessions/" + runtimeTestSessionID + "/heartbeat":
			close(heartbeatReached)
			w.Header().Set("Content-Type", "application/json")
			writeRuntimeTestJSON(t, w, RuntimeReadyPayload{
				CoreInstanceID: runtimeTestCoreID, AttachmentID: runtimeTestAttachmentID,
				Features: RuntimeRequiredFeatures(), OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
			})
		case "/api/v1/agent-runtime/sessions/" + runtimeTestSessionID + "/close":
			close(closeReached)
			w.WriteHeader(http.StatusNoContent)
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient.attachmentMu.Lock()
	runtimeClient.attachmentID = runtimeTestAttachmentID
	runtimeClient.attachmentMu.Unlock()

	claimDone := make(chan error, 1)
	go func() {
		_, claimErr := runtimeClient.ClaimRuntimeRun(context.Background(), 25, RuntimeClaimRequest{
			RuntimeSessionID: runtimeTestSessionID, Capacity: 1, Inflight: 0,
		})
		claimDone <- claimErr
	}()
	commandsDone := make(chan error, 1)
	go func() {
		_, commandsErr := runtimeClient.PollRuntimeCommands(context.Background(), runtimeTestSessionID, 25)
		commandsDone <- commandsErr
	}()
	waitForTestSignal(t, claimStarted, time.Second, "long claim request")
	waitForTestSignal(t, commandsStarted, time.Second, "long command poll")

	heartbeatDone := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		defer cancel()
		_, heartbeatErr := runtimeClient.HeartbeatRuntimeSession(ctx, runtimeTestHello())
		heartbeatDone <- heartbeatErr
	}()
	waitForTestSignal(t, heartbeatReached, time.Second, "heartbeat alongside long Pull requests")
	if err = <-heartbeatDone; err != nil {
		t.Fatalf("heartbeat alongside long Pull requests: %v", err)
	}

	closeDone := make(chan error, 1)
	go func() {
		closeDone <- runtimeClient.CloseRuntimeSession(context.Background(), RuntimeSessionCloseRequest{
			NodeID: runtimeTestNodeID, AgentID: runtimeTestAgentID, WorkerID: "worker-a",
			RuntimeSessionID: runtimeTestSessionID, SessionEpoch: 1,
			Status: "closed", Reason: "transport rotation",
		})
	}()
	select {
	case <-closeReached:
		t.Fatal("attachment lifecycle rotation crossed in-flight Pull requests")
	case <-time.After(100 * time.Millisecond):
	}

	releaseOnce.Do(func() { close(releasePulls) })
	if err = <-claimDone; err != nil {
		t.Fatalf("claim request: %v", err)
	}
	if err = <-commandsDone; err != nil {
		t.Fatalf("command poll: %v", err)
	}
	waitForTestSignal(t, closeReached, time.Second, "lifecycle rotation after Pull requests")
	if err = <-closeDone; err != nil {
		t.Fatalf("close Runtime session: %v", err)
	}
}

func TestRuntimeHTTPRejectsUnknownResponseAndUnstableIDs(t *testing.T) {
	t.Parallel()

	calls := 0
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		calls++
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"core_instance_id":"` + runtimeTestCoreID + `","features":["lease_fence","assignment_confirm","renew","resume","event_ack","result_ack","cancel","persistent_spool"],"offer_ttl_seconds":30,"lease_ttl_seconds":60,"database_time":"2026-07-11T00:00:00Z","unexpected":true}`))
	}))
	defer server.Close()
	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runtimeClient.CreateRuntimeSession(context.Background(), runtimeTestHello()); err == nil {
		t.Fatal("unknown response field must fail")
	}
	bad := RuntimeRunEventPayload{
		AttemptIdentity: runtimeTestIdentity(), ClientEventID: "event-from-clock", ClientEventSeq: 1,
		EventType: "run.progress", Payload: map[string]any{},
	}
	if _, err = runtimeClient.AppendRuntimeEvent(context.Background(), bad); err == nil {
		t.Fatal("caller must supply a stable UUID event ID")
	}
	if calls != 1 {
		t.Fatalf("invalid Event reached server; calls=%d", calls)
	}
}

func runtimeTestHello() RuntimeHelloPayload {
	return RuntimeHelloPayload{
		NodeID: runtimeTestNodeID, AgentID: runtimeTestAgentID, WorkerID: "worker-a",
		RuntimeSessionID: runtimeTestSessionID, SessionEpoch: 1, NodeVersion: "0.2.0",
		Capacity: 1, Features: RuntimeRequiredFeatures(), ContractDigest: RuntimeContractDigest,
	}
}

func runtimeTestIdentity() RuntimeAttemptIdentity {
	return RuntimeAttemptIdentity{
		RunID: runtimeTestRunID, AttemptID: runtimeTestAttemptID, LeaseID: runtimeTestLeaseID,
		FencingToken: 1, NodeID: runtimeTestNodeID, AgentID: runtimeTestAgentID,
		WorkerID: "worker-a", RuntimeSessionID: runtimeTestSessionID,
	}
}

func decodeRuntimeTestBody(t *testing.T, req *http.Request, out any) {
	t.Helper()
	decoder := json.NewDecoder(req.Body)
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		t.Fatal(err)
	}
}

func writeRuntimeTestJSON(t *testing.T, w http.ResponseWriter, value any) {
	t.Helper()
	if err := json.NewEncoder(w).Encode(value); err != nil {
		t.Fatal(err)
	}
}
