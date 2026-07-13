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
	runtimeTestCancellationID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	runtimeOtherSessionID     = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
)

func TestRuntimeCommandsAndCancelAck(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC().Truncate(time.Millisecond)
	identity := runtimeTestIdentity()
	cancel := RuntimeRunCancelPayload{
		CancellationID: runtimeTestCancellationID, AttemptIdentity: identity,
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
		case "/api/v1/agent-runtime/commands":
			if req.Method != http.MethodGet || req.URL.Query().Get("wait") != "17" ||
				req.URL.Query().Get("runtime_session_id") != runtimeTestSessionID {
				t.Errorf("command request = %s %s", req.Method, req.URL.RequestURI())
			}
			writeRuntimeTestJSON(t, w, RuntimeCommandsResponse{
				Commands:     []RuntimePendingCommand{{Type: RuntimeRunCancel, Payload: cancelPayload}},
				DatabaseTime: now,
			})
		case "/api/v1/agent-runtime/runs/" + runtimeTestRunID + "/cancel-ack":
			var ack RuntimeRunCancelAckPayload
			decodeRuntimeTestBody(t, req, &ack)
			if ack.CancellationID != runtimeTestCancellationID || ack.CancelState != RuntimeCancelUnsupported ||
				ack.ErrorCode != "CANCEL_NOT_SUPPORTED" {
				t.Errorf("cancel ACK = %#v", ack)
			}
			writeRuntimeTestJSON(t, w, RuntimeRunCancellationState{
				CancellationID: runtimeTestCancellationID, CancelState: RuntimeCancelUnsupported,
				UpdatedAt: now, ErrorCode: "CANCEL_NOT_SUPPORTED",
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
	if _, err = runtimeClient.PollRuntimeCommands(context.Background(), "not-a-uuid", 17); err == nil {
		t.Fatal("invalid runtime_session_id reached command transport")
	}
	if _, err = runtimeClient.PollRuntimeCommands(context.Background(), runtimeTestSessionID, RuntimeMaxPullWaitSeconds+1); err == nil {
		t.Fatal("invalid wait reached command transport")
	}
	commands, err := runtimeClient.PollRuntimeCommands(context.Background(), runtimeTestSessionID, 17)
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
	state, err := runtimeClient.AckRuntimeCancel(context.Background(), RuntimeRunCancelAckPayload{
		CancellationID: runtimeTestCancellationID, AttemptIdentity: identity,
		CancelState: RuntimeCancelUnsupported, ErrorCode: "CANCEL_NOT_SUPPORTED",
	})
	if err != nil {
		t.Fatal(err)
	}
	if state.CancelState != RuntimeCancelUnsupported || state.ErrorCode != "CANCEL_NOT_SUPPORTED" {
		t.Fatalf("state = %#v", state)
	}
	close(steps)
	var got []string
	for step := range steps {
		got = append(got, step)
	}
	want := []string{
		"/api/v1/agent-runtime/commands?runtime_session_id=" + runtimeTestSessionID + "&wait=17",
		"/api/v1/agent-runtime/runs/" + runtimeTestRunID + "/cancel-ack",
	}
	if len(got) != len(want) || got[0] != want[0] || got[1] != want[1] {
		t.Fatalf("steps = %#v, want %#v", got, want)
	}
}

func TestDecodeRuntimePendingCommandIsStrictAndTyped(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	identity := runtimeTestIdentity()
	validRevoke := runtimeMarshalTest(t, RuntimeRunLeaseRevokedPayload{
		AttemptIdentity: identity, ReasonCode: "LEASE_LOST",
		RunStatus: RuntimeRunCanceled, DispatchState: RuntimeDispatchTerminal,
	})
	decoded, err := DecodeRuntimePendingCommand(RuntimePendingCommand{Type: RuntimeLeaseRevoked, Payload: validRevoke})
	if err != nil || decoded.Revoke == nil || decoded.Type != RuntimeLeaseRevoked {
		t.Fatalf("revoke = %#v, %v", decoded, err)
	}
	validDrain := runtimeMarshalTest(t, RuntimeDrainPayload{
		DeadlineAt: now.Add(time.Minute), ReasonCode: "DEPLOY", Capacity: 3, Inflight: 2,
	})
	decoded, err = DecodeRuntimePendingCommand(RuntimePendingCommand{Type: RuntimeDrain, Payload: validDrain})
	if err != nil || decoded.Drain == nil || decoded.Type != RuntimeDrain {
		t.Fatalf("drain = %#v, %v", decoded, err)
	}

	cases := []RuntimePendingCommand{
		{Type: RuntimeMessageType("runtime.unknown"), Payload: []byte(`{}`)},
		{Type: RuntimeDrain, Payload: runtimeMarshalTest(t, map[string]any{
			"deadline_at": now, "reason_code": "DRAIN", "capacity": -1, "inflight": 0,
		})},
		{Type: RuntimeRunCancel, Payload: runtimeMarshalTest(t, map[string]any{
			"cancellation_id": runtimeTestCancellationID, "attempt_identity": identity,
			"reason_code": "OWNER_REQUEST", "deadline_at": now, "unexpected": true,
		})},
	}
	for index, command := range cases {
		if _, err := DecodeRuntimePendingCommand(command); err == nil {
			t.Fatalf("case %d must fail", index)
		}
	}
}

func TestRuntimeResponseIdentityAndCommandValidation(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	identity := runtimeTestIdentity()
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case strings.HasSuffix(req.URL.Path, "/runs/claim"):
			wrongIdentity := identity
			wrongIdentity.RuntimeSessionID = runtimeOtherSessionID
			writeRuntimeTestJSON(t, w, RuntimeRunAssignedPayload{
				AttemptIdentity: wrongIdentity, OfferNo: 1, OfferExpiresAt: now.Add(time.Minute),
				AttemptDeadlineAt: now.Add(time.Minute), RunDeadlineAt: now.Add(time.Hour), Input: map[string]any{},
				NodeEnvelope:         "ol_ctx_v2.current.payload.signature",
				AgentInvocationToken: "ol_inv_v2.current.payload.signature",
			})
		case strings.HasSuffix(req.URL.Path, "/assignment-reject"):
			writeRuntimeTestJSON(t, w, RuntimeAssignmentRejectedPayload{
				AttemptIdentity: identity, Outcome: RuntimeAssignmentRejectOutcome("unknown"),
				DispatchState: RuntimeDispatchPending,
			})
		case strings.HasSuffix(req.URL.Path, "/lease-renew"):
			writeRuntimeTestJSON(t, w, RuntimeLeaseRenewedPayload{
				AttemptIdentity: identity, LeaseExpiresAt: now.Add(time.Minute),
				PendingCommand: &RuntimePendingCommand{Type: RuntimeRunCancel, Payload: []byte(`{}`)},
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
	if _, err = runtimeClient.ClaimRuntimeRun(context.Background(), 0, RuntimeClaimRequest{
		RuntimeSessionID: runtimeTestSessionID, Capacity: 1,
	}); err == nil {
		t.Fatal("claim session mismatch must fail")
	}
	if _, err = runtimeClient.RejectRuntimeAssignment(context.Background(), RuntimeAssignmentRejectPayload{
		AttemptIdentity: identity, ReasonCode: RuntimeRejectNodeAtCapacity, Capacity: 1, Inflight: 1,
	}); err == nil {
		t.Fatal("unknown rejection outcome must fail")
	}
	if _, err = runtimeClient.RenewRuntimeLease(context.Background(), RuntimeLeaseRenewPayload{
		AttemptIdentity: identity, Capacity: 1, Inflight: 1,
	}); err == nil {
		t.Fatal("invalid renewal command must fail")
	}
}

func TestRuntimeResultResumeEventAndCancellationSemantics(t *testing.T) {
	t.Parallel()

	now := time.Now().UTC()
	validRetry := RuntimeRunResultAckPayload{
		ResultID: runtimeTestResultID, Classification: RuntimeResultRetryableFailure,
		RunStatus: RuntimeRunRunning, DispatchState: RuntimeDispatchRetryWait, NextAttemptAt: &now,
	}
	if err := validateRuntimeResultAck(validRetry); err != nil {
		t.Fatal(err)
	}
	invalidResults := []RuntimeRunResultAckPayload{
		{ResultID: runtimeTestResultID, Classification: RuntimeResultClassification("other"), RunStatus: RuntimeRunRunning, DispatchState: RuntimeDispatchPending},
		{ResultID: runtimeTestResultID, Classification: RuntimeResultSuccess, RunStatus: RuntimeRunRunning, DispatchState: RuntimeDispatchTerminal},
		{ResultID: runtimeTestResultID, Classification: RuntimeResultRetryableFailure, RunStatus: RuntimeRunRunning, DispatchState: RuntimeDispatchRetryWait},
		{ResultID: runtimeTestResultID, Classification: RuntimeResultDeadLetter, RunStatus: RuntimeRunFailed, DispatchState: RuntimeDispatchDeadLetter, NextAttemptAt: &now},
	}
	for index, ack := range invalidResults {
		if err := validateRuntimeResultAck(ack); err == nil {
			t.Fatalf("invalid result ACK %d passed", index)
		}
	}

	identity := runtimeTestIdentity()
	leaseExpiry := now.Add(time.Minute)
	validResume := RuntimeResumeAcceptedPayload{
		AttemptIdentity: identity, Decision: RuntimeResumeContinue, LeaseExpiresAt: &leaseExpiry,
		AllowedActions: []RuntimeResumeAction{
			RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult,
		},
	}
	if err := validateRuntimeResumeDecision(validResume); err != nil {
		t.Fatal(err)
	}
	invalidResume := validResume
	invalidResume.AllowedActions = append(invalidResume.AllowedActions, RuntimeActionClearSpool)
	if err := validateRuntimeResumeDecision(invalidResume); err == nil {
		t.Fatal("inconsistent resume actions passed")
	}
	invalidResume = RuntimeResumeAcceptedPayload{
		AttemptIdentity: identity, Decision: RuntimeResumeUploadSpool,
		AllowedActions: []RuntimeResumeAction{RuntimeActionUploadEvents, RuntimeActionUploadEvents},
	}
	if err := validateRuntimeResumeDecision(invalidResume); err == nil {
		t.Fatal("duplicate resume actions passed")
	}

	event := RuntimeRunEventPayload{
		AttemptIdentity: identity, ClientEventID: runtimeTestEventID, ClientEventSeq: 1,
		EventType: "run.progress", Payload: map[string]any{},
	}
	if err := validateRuntimeEvent(event); err != nil {
		t.Fatal(err)
	}
	for _, eventType := range []string{"progress", "Run.progress", "run.completed"} {
		event.EventType = eventType
		if err := validateRuntimeEvent(event); err == nil {
			t.Fatalf("event type %q passed", eventType)
		}
	}
	result := RuntimeRunResultPayload{
		AttemptIdentity: identity, ResultID: runtimeTestResultID, Status: "success",
		Output: map[string]any{}, DurationMS: math.MaxInt32 + 1,
	}
	if err := validateRuntimeResult(result); err == nil {
		t.Fatal("oversized duration passed")
	}

	validCancelAck := RuntimeRunCancelAckPayload{
		CancellationID: runtimeTestCancellationID, AttemptIdentity: identity,
		CancelState: RuntimeCancelFailed, ErrorCode: "STOP_FAILED",
	}
	if err := validateRuntimeCancelAck(validCancelAck); err != nil {
		t.Fatal(err)
	}
	validCancelAck.ErrorCode = ""
	if err := validateRuntimeCancelAck(validCancelAck); err == nil {
		t.Fatal("failed cancellation ACK without error passed")
	}
	validCancelAck.CancelState = RuntimeCancelRequested
	if err := validateRuntimeCancelAck(validCancelAck); err == nil {
		t.Fatal("requested is not a legal cancellation ACK")
	}
}

func TestRuntimeErrorEnvelopeIsBoundedAndStrict(t *testing.T) {
	t.Parallel()

	valid := runtimeHTTPResponse(http.StatusConflict, `{"error":{"code":"STALE_LEASE","message":"Lease is stale","retryable":false,"current_run_status":"running","current_dispatch_state":"executing"}}`)
	err := parseRuntimeError(valid)
	var sdkError *Error
	if !errors.As(err, &sdkError) || sdkError.Code != "STALE_LEASE" || sdkError.StatusCode != http.StatusConflict {
		t.Fatalf("valid error = %#v", err)
	}
	body, ok := sdkError.Details.(RuntimeErrorBody)
	if !ok || body.CurrentRunStatus != RuntimeRunRunning || body.CurrentDispatchState != RuntimeDispatchExecuting {
		t.Fatalf("details = %#v", sdkError.Details)
	}

	unknown := runtimeHTTPResponse(http.StatusBadRequest, `{"error":{"code":"BAD_REQUEST","message":"bad","retryable":false,"unknown":true}}`)
	if err := parseRuntimeError(unknown); err == nil || errors.As(err, &sdkError) {
		t.Fatalf("unknown error field = %#v", err)
	}

	oversized := &http.Response{
		StatusCode: http.StatusBadRequest,
		Status:     "400 Bad Request",
		Header:     make(http.Header),
		Body:       io.NopCloser(bytes.NewReader(bytes.Repeat([]byte("x"), int(RuntimeMaxMessageBytes)+1))),
	}
	if err := parseRuntimeError(oversized); err == nil || !strings.Contains(err.Error(), "too large") {
		t.Fatalf("oversized error = %v", err)
	}
}

func runtimeMarshalTest(t *testing.T, value any) []byte {
	t.Helper()
	raw, err := json.Marshal(value)
	if err != nil {
		t.Fatal(err)
	}
	return raw
}

func runtimeHTTPResponse(status int, body string) *http.Response {
	return &http.Response{
		StatusCode: status,
		Status:     http.StatusText(status),
		Header:     make(http.Header),
		Body:       io.NopCloser(strings.NewReader(body)),
	}
}
