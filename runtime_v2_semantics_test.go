package openlinker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"io"
	"math"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

const (
	runtimeV2TestCancellationID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	runtimeV2OtherSessionID     = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

func TestRuntimeV2CommandsAndCancelAck(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	identity := runtimeV2TestIdentity()
	cancel := RuntimeV2RunCancelPayload{
		CancellationID: runtimeV2TestCancellationID, AttemptIdentity: identity,
		ReasonCode: "OWNER_REQUEST", DeadlineAt: now.Add(30 * time.Second),
	}
	cancelPayload, err := json.Marshal(cancel)
	if err != nil {
		t.Fatal(err)
	}
	steps := make(chan string, 2)
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Header.Get("Authorization") != "Bearer ol_agent_v2" {
			t.Errorf("authorization = %q", req.Header.Get("Authorization"))
		}
		steps <- req.URL.RequestURI()
		w.Header().Set("Content-Type", "application/json")
		switch req.URL.Path {
		case "/api/v1/agent-runtime/v2/commands":
			if req.Method != http.MethodGet || req.URL.Query().Get("wait") != "17" ||
				req.URL.Query().Get("runtime_session_id") != runtimeV2TestSessionID {
				t.Errorf("command request = %s %s", req.Method, req.URL.RequestURI())
			}
			writeRuntimeV2TestJSON(t, w, RuntimeV2CommandsResponse{
				Commands:     []RuntimeV2PendingCommand{{Type: RuntimeV2RunCancel, Payload: cancelPayload}},
				DatabaseTime: now,
			})
		case "/api/v1/agent-runtime/v2/runs/" + runtimeV2TestRunID + "/cancel-ack":
			var ack RuntimeV2RunCancelAckPayload
			decodeRuntimeV2TestBody(t, req, &ack)
			if ack.CancellationID != runtimeV2TestCancellationID || ack.CancelState != RuntimeV2CancelUnsupported ||
				ack.ErrorCode != "CANCEL_NOT_SUPPORTED" {
				t.Errorf("cancel ACK = %#v", ack)
			}
			writeRuntimeV2TestJSON(t, w, RuntimeV2RunCancellationState{
				CancellationID: runtimeV2TestCancellationID, CancelState: RuntimeV2CancelUnsupported,
				UpdatedAt: now, ErrorCode: "CANCEL_NOT_SUPPORTED",
			})
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()

	runtimeClient, err := NewRuntime(server.URL, WithRuntimeToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runtimeClient.PollRuntimeV2Commands(context.Background(), "not-a-uuid", 17); err == nil {
		t.Fatal("invalid runtime_session_id reached command transport")
	}
	if _, err = runtimeClient.PollRuntimeV2Commands(context.Background(), runtimeV2TestSessionID, RuntimeV2MaxPullWaitSeconds+1); err == nil {
		t.Fatal("invalid wait reached command transport")
	}
	commands, err := runtimeClient.PollRuntimeV2Commands(context.Background(), runtimeV2TestSessionID, 17)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands.Commands) != 1 || !commands.DatabaseTime.Equal(now) {
		t.Fatalf("commands = %#v", commands)
	}
	decoded, err := commands.Commands[0].Decode()
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Cancel == nil || *decoded.Cancel != cancel || decoded.Drain != nil || decoded.Revoke != nil {
		t.Fatalf("decoded command = %#v", decoded)
	}
	state, err := runtimeClient.AckRuntimeV2Cancel(context.Background(), RuntimeV2RunCancelAckPayload{
		CancellationID: runtimeV2TestCancellationID, AttemptIdentity: identity,
		CancelState: RuntimeV2CancelUnsupported, ErrorCode: "CANCEL_NOT_SUPPORTED",
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.CancelState != RuntimeV2CancelUnsupported || state.ErrorCode != "CANCEL_NOT_SUPPORTED" {
		t.Fatalf("state = %#v", state)
	}
	close(steps)
	var got []string
	for step := range steps {
		got = append(got, step)
	}
	want := []string{
		"/api/v1/agent-runtime/v2/commands?runtime_session_id=" + runtimeV2TestSessionID + "&wait=17",
		"/api/v1/agent-runtime/v2/runs/" + runtimeV2TestRunID + "/cancel-ack",
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("steps = %#v, want %#v", got, want)
	}
}

func TestDecodeRuntimeV2PendingCommandIsStrictAndTyped(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	identity := runtimeV2TestIdentity()
	validRevoke := runtimeV2MarshalTest(t, RuntimeV2RunLeaseRevokedPayload{
		AttemptIdentity: identity, ReasonCode: "LEASE_LOST",
		RunStatus: RuntimeV2RunCanceled, DispatchState: RuntimeV2DispatchTerminal,
	})
	decoded, err := DecodeRuntimeV2PendingCommand(RuntimeV2PendingCommand{Type: RuntimeV2LeaseRevoked, Payload: validRevoke})
	if err != nil || decoded.Revoke == nil || decoded.Type != RuntimeV2LeaseRevoked {
		t.Fatalf("revoke = %#v, %v", decoded, err)
	}
	validDrain := runtimeV2MarshalTest(t, RuntimeV2DrainPayload{
		DeadlineAt: now.Add(time.Minute), ReasonCode: "DEPLOY", Capacity: 3, Inflight: 2,
	})
	decoded, err = DecodeRuntimeV2PendingCommand(RuntimeV2PendingCommand{Type: RuntimeV2Drain, Payload: validDrain})
	if err != nil || decoded.Drain == nil || decoded.Type != RuntimeV2Drain {
		t.Fatalf("drain = %#v, %v", decoded, err)
	}

	cases := []RuntimeV2PendingCommand{
		{Type: RuntimeV2MessageType("runtime.unknown"), Payload: []byte(`{}`)},
		{Type: RuntimeV2Drain, Payload: runtimeV2MarshalTest(t, map[string]any{
			"deadline_at": now, "reason_code": "DRAIN", "capacity": -1, "inflight": 0,
		})},
		{Type: RuntimeV2RunCancel, Payload: runtimeV2MarshalTest(t, map[string]any{
			"cancellation_id": runtimeV2TestCancellationID, "attempt_identity": identity,
			"reason_code": "OWNER_REQUEST", "deadline_at": now, "unexpected": true,
		})},
	}
	for index, command := range cases {
		if _, err := DecodeRuntimeV2PendingCommand(command); err == nil {
			t.Fatalf("case %d must fail", index)
		}
	}
}

func TestRuntimeV2ResponseIdentityAndCommandValidation(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	identity := runtimeV2TestIdentity()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(req.URL.Path, "/runs/claim"):
			wrongIdentity := identity
			wrongIdentity.RuntimeSessionID = runtimeV2OtherSessionID
			writeRuntimeV2TestJSON(t, w, RuntimeV2RunAssignedPayload{
				AttemptIdentity: wrongIdentity, OfferNo: 1, OfferExpiresAt: now.Add(time.Minute),
				AttemptDeadlineAt: now.Add(time.Minute), RunDeadlineAt: now.Add(time.Hour), Input: map[string]any{},
				NodeEnvelope:         "ol_ctx_v2.current.payload.signature",
				AgentInvocationToken: "ol_inv_v2.current.payload.signature",
			})
		case strings.HasSuffix(req.URL.Path, "/assignment-reject"):
			writeRuntimeV2TestJSON(t, w, RuntimeV2AssignmentRejectedPayload{
				AttemptIdentity: identity, Outcome: RuntimeV2AssignmentRejectOutcome("unknown"),
				DispatchState: RuntimeV2DispatchPending,
			})
		case strings.HasSuffix(req.URL.Path, "/lease-renew"):
			writeRuntimeV2TestJSON(t, w, RuntimeV2LeaseRenewedPayload{
				AttemptIdentity: identity, LeaseExpiresAt: now.Add(time.Minute),
				PendingCommand: &RuntimeV2PendingCommand{Type: RuntimeV2RunCancel, Payload: []byte(`{}`)},
			})
		default:
			http.NotFound(w, req)
		}
	}))
	defer server.Close()
	runtimeClient, err := NewRuntime(server.URL, WithRuntimeToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	if _, err = runtimeClient.ClaimRuntimeV2Run(context.Background(), 0, RuntimeV2ClaimRequest{
		RuntimeSessionID: runtimeV2TestSessionID, Capacity: 1,
	}); err == nil {
		t.Fatal("claim session mismatch must fail")
	}
	if _, err = runtimeClient.RejectRuntimeV2Assignment(context.Background(), RuntimeV2AssignmentRejectPayload{
		AttemptIdentity: identity, ReasonCode: RuntimeV2RejectNodeAtCapacity, Capacity: 1, Inflight: 1,
	}); err == nil {
		t.Fatal("unknown rejection outcome must fail")
	}
	if _, err = runtimeClient.RenewRuntimeV2Lease(context.Background(), RuntimeV2LeaseRenewPayload{
		AttemptIdentity: identity, Capacity: 1, Inflight: 1,
	}); err == nil {
		t.Fatal("invalid renewal command must fail")
	}
}

func TestRuntimeV2ResultResumeEventAndCancellationSemantics(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	validRetry := RuntimeV2RunResultAckPayload{
		ResultID: runtimeV2TestResultID, Classification: RuntimeV2ResultRetryableFailure,
		RunStatus: RuntimeV2RunRunning, DispatchState: RuntimeV2DispatchRetryWait, NextAttemptAt: &now,
	}
	if err := validateRuntimeV2ResultAck(validRetry); err != nil {
		t.Fatal(err)
	}
	invalidResults := []RuntimeV2RunResultAckPayload{
		{ResultID: runtimeV2TestResultID, Classification: RuntimeV2ResultClassification("other"), RunStatus: RuntimeV2RunRunning, DispatchState: RuntimeV2DispatchPending},
		{ResultID: runtimeV2TestResultID, Classification: RuntimeV2ResultSuccess, RunStatus: RuntimeV2RunRunning, DispatchState: RuntimeV2DispatchTerminal},
		{ResultID: runtimeV2TestResultID, Classification: RuntimeV2ResultRetryableFailure, RunStatus: RuntimeV2RunRunning, DispatchState: RuntimeV2DispatchRetryWait},
		{ResultID: runtimeV2TestResultID, Classification: RuntimeV2ResultDeadLetter, RunStatus: RuntimeV2RunFailed, DispatchState: RuntimeV2DispatchDeadLetter, NextAttemptAt: &now},
	}
	for index, ack := range invalidResults {
		if err := validateRuntimeV2ResultAck(ack); err == nil {
			t.Fatalf("invalid result ACK %d passed", index)
		}
	}

	identity := runtimeV2TestIdentity()
	leaseExpiry := now.Add(time.Minute)
	validResume := RuntimeV2ResumeAcceptedPayload{
		AttemptIdentity: identity, Decision: RuntimeV2ResumeContinue, LeaseExpiresAt: &leaseExpiry,
		AllowedActions: []RuntimeV2ResumeAction{
			RuntimeV2ActionContinueExecution, RuntimeV2ActionUploadEvents, RuntimeV2ActionUploadResult,
		},
	}
	if err := validateRuntimeV2ResumeDecision(validResume); err != nil {
		t.Fatal(err)
	}
	invalidResume := validResume
	invalidResume.AllowedActions = append(invalidResume.AllowedActions, RuntimeV2ActionClearSpool)
	if err := validateRuntimeV2ResumeDecision(invalidResume); err == nil {
		t.Fatal("inconsistent resume actions passed")
	}
	invalidResume = RuntimeV2ResumeAcceptedPayload{
		AttemptIdentity: identity, Decision: RuntimeV2ResumeUploadSpool,
		AllowedActions: []RuntimeV2ResumeAction{RuntimeV2ActionUploadEvents, RuntimeV2ActionUploadEvents},
	}
	if err := validateRuntimeV2ResumeDecision(invalidResume); err == nil {
		t.Fatal("duplicate resume actions passed")
	}

	event := RuntimeV2RunEventPayload{
		AttemptIdentity: identity, ClientEventID: runtimeV2TestEventID, ClientEventSeq: 1,
		EventType: "run.progress", Payload: map[string]any{},
	}
	if err := validateRuntimeV2Event(event); err != nil {
		t.Fatal(err)
	}
	for _, eventType := range []string{"progress", "Run.progress", "run.completed"} {
		event.EventType = eventType
		if err := validateRuntimeV2Event(event); err == nil {
			t.Fatalf("event type %q passed", eventType)
		}
	}
	result := RuntimeV2RunResultPayload{
		AttemptIdentity: identity, ResultID: runtimeV2TestResultID, Status: "success",
		Output: map[string]any{}, DurationMS: math.MaxInt32 + 1,
	}
	if err := validateRuntimeV2Result(result); err == nil {
		t.Fatal("oversized duration passed")
	}

	validCancelAck := RuntimeV2RunCancelAckPayload{
		CancellationID: runtimeV2TestCancellationID, AttemptIdentity: identity,
		CancelState: RuntimeV2CancelFailed, ErrorCode: "STOP_FAILED",
	}
	if err := validateRuntimeV2CancelAck(validCancelAck); err != nil {
		t.Fatal(err)
	}
	validCancelAck.ErrorCode = ""
	if err := validateRuntimeV2CancelAck(validCancelAck); err == nil {
		t.Fatal("failed cancellation ACK without error passed")
	}
	validCancelAck.CancelState = RuntimeV2CancelRequested
	if err := validateRuntimeV2CancelAck(validCancelAck); err == nil {
		t.Fatal("requested is not a legal cancellation ACK")
	}
}

func TestRuntimeV2ErrorEnvelopeIsBoundedAndStrict(t *testing.T) {
	t.Parallel()

	valid := runtimeV2HTTPResponse(http.StatusConflict, `{"error":{"code":"STALE_LEASE","message":"Lease is stale","retryable":false,"current_run_status":"running","current_dispatch_state":"executing"}}`)
	err := parseRuntimeV2Error(valid)
	var sdkError *Error
	if !errors.As(err, &sdkError) || sdkError.Code != "STALE_LEASE" || sdkError.StatusCode != http.StatusConflict {
		t.Fatalf("valid error = %#v", err)
	}
	body, ok := sdkError.Details.(RuntimeV2ErrorBody)
	if !ok || body.CurrentRunStatus != RuntimeV2RunRunning || body.CurrentDispatchState != RuntimeV2DispatchExecuting {
		t.Fatalf("details = %#v", sdkError.Details)
	}

	unknown := runtimeV2HTTPResponse(http.StatusBadRequest, `{"error":{"code":"BAD_REQUEST","message":"bad","retryable":false,"unknown":true}}`)
	if err := parseRuntimeV2Error(unknown); err == nil || errors.As(err, &sdkError) {
		t.Fatalf("unknown error field = %#v", err)
	}

	oversized := &http.Response{
		StatusCode: http.StatusBadRequest,
		Status:     "400 Bad Request",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), int(RuntimeV2MaxMessageBytes)+1))),
	}
	if err := parseRuntimeV2Error(oversized); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("oversized error = %v", err)
	}
}

func runtimeV2MarshalTest(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func runtimeV2HTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
