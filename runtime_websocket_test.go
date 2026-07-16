package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"sync"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestRuntimeWebSocketFallbackReasonHeaderIsBounded(t *testing.T) {
	tests := []struct {
		name   string
		reason runtimeFallbackReason
		want   string
	}{
		{name: "explicit", reason: runtimeFallbackExplicit, want: "explicit"},
		{name: "websocket unavailable", reason: runtimeFallbackWebSocketUnavailable, want: "websocket_unavailable"},
		{name: "policy forced", reason: runtimeFallbackPolicyForced, want: "policy_forced"},
		{name: "recovery", reason: runtimeFallbackRecovery, want: "recovery"},
		{name: "invalid omitted", reason: "private_network_error", want: ""},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			serverErr := make(chan error, 1)
			server := newRuntimeSDKWSServer(t, func(request *http.Request, conn *websocket.Conn) error {
				if got := request.Header.Get(RuntimeFallbackReasonHeader); got != test.want {
					return fmt.Errorf("fallback reason header = %q, want %q", got, test.want)
				}
				if got := request.Header.Get("Authorization"); got != "Bearer ol_agent_v2" {
					return fmt.Errorf("Runtime authorization = %q", got)
				}
				if got := request.Header.Get("X-OpenLinker-SDK"); got == "attacker-sdk" || got == "" {
					return fmt.Errorf("Runtime SDK identity = %q", got)
				}
				return serveRuntimeSDKWSReady(conn, time.Now().UTC())
			}, serverErr)
			defer server.Close()
			runtimeClient, err := NewRuntime(
				server.URL,
				WithAgentToken("ol_agent_v2"),
				WithHeader(RuntimeFallbackReasonHeader, "private_network_error"),
				WithHeader("Authorization", "Bearer public-token"),
				WithHeader("X-OpenLinker-SDK", "attacker-sdk"),
			)
			if err != nil {
				t.Fatal(err)
			}
			ctx := context.WithValue(context.Background(), runtimeFallbackReasonContextKey{}, test.reason)
			connection, err := runtimeClient.DialRuntimeWebSocket(ctx, runtimeTestHello())
			if err != nil {
				t.Fatal(err)
			}
			_ = connection.Close()
			if err = <-serverErr; err != nil {
				t.Fatal(err)
			}
		})
	}
}

func TestRuntimeWebSocketHandshakeAssignmentAndCancelCorrelation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeTestHello()
	identity := runtimeTestIdentity()
	serverErr := make(chan error, 1)
	server := newRuntimeSDKWSServer(t, func(request *http.Request, conn *websocket.Conn) error {
		if request.URL.Path != "/api/v1/agent-runtime/ws" {
			return fmt.Errorf("WebSocket path = %q", request.URL.Path)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer ol_agent_v2" {
			return fmt.Errorf("authorization = %q", got)
		}
		helloEnvelope, err := readRuntimeSDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		if helloEnvelope.Type != RuntimeHello || helloEnvelope.ReplyToMessageID != "" {
			return fmt.Errorf("hello envelope = %#v", helloEnvelope.RuntimeEnvelopeFields)
		}
		if _, err = decodeRuntimeWSPayload[RuntimeHelloPayload](helloEnvelope, RuntimeHello); err != nil {
			return err
		}
		if err = writeRuntimeSDKWSEnvelope(conn, RuntimeReady, helloEnvelope.MessageID, RuntimeReadyPayload{
			CoreInstanceID: runtimeTestCoreID, AttachmentID: runtimeTestAttachmentID, Features: RuntimeRequiredFeatures(),
			OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
		}); err != nil {
			return err
		}

		assignmentMessageID, err := writeRuntimeSDKWSEnvelopeID(conn, RuntimeRunAssigned, "", runtimeTestAssignment(now, identity))
		if err != nil {
			return err
		}
		ackEnvelope, err := readRuntimeSDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		if ackEnvelope.Type != RuntimeAssignmentAck || ackEnvelope.ReplyToMessageID != assignmentMessageID {
			return fmt.Errorf("assignment ACK correlation = %#v", ackEnvelope.RuntimeEnvelopeFields)
		}
		ack, err := decodeRuntimeWSPayload[RuntimeAssignmentAckPayload](ackEnvelope, RuntimeAssignmentAck)
		if err != nil || ack.AttemptIdentity != identity {
			return fmt.Errorf("assignment ACK = %#v, %w", ack, err)
		}
		if err = writeRuntimeSDKWSEnvelope(conn, RuntimeAssignmentConfirmed, ackEnvelope.MessageID, RuntimeAssignmentConfirmedPayload{
			AttemptIdentity: identity, AttemptNo: 1, LeaseExpiresAt: now.Add(time.Minute),
		}); err != nil {
			return err
		}

		cancel := RuntimeRunCancelPayload{
			CancellationID: runtimeTestCancellationID, AttemptIdentity: identity,
			ReasonCode: "OWNER_REQUEST", DeadlineAt: now.Add(time.Minute),
		}
		cancelMessageID, err := writeRuntimeSDKWSEnvelopeID(conn, RuntimeRunCancel, "", cancel)
		if err != nil {
			return err
		}
		cancelAckEnvelope, err := readRuntimeSDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		if cancelAckEnvelope.Type != RuntimeRunCancelAck || cancelAckEnvelope.ReplyToMessageID != cancelMessageID {
			return fmt.Errorf("cancel ACK correlation = %#v", cancelAckEnvelope.RuntimeEnvelopeFields)
		}
		cancelAck, err := decodeRuntimeWSPayload[RuntimeRunCancelAckPayload](cancelAckEnvelope, RuntimeRunCancelAck)
		if err != nil || cancelAck.CancelState != RuntimeCancelStopping {
			return fmt.Errorf("cancel ACK = %#v, %w", cancelAck, err)
		}
		terminalAckEnvelope, err := readRuntimeSDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		if terminalAckEnvelope.Type != RuntimeRunCancelAck || terminalAckEnvelope.ReplyToMessageID != cancelMessageID {
			return fmt.Errorf("terminal cancel ACK correlation = %#v", terminalAckEnvelope.RuntimeEnvelopeFields)
		}
		terminalAck, err := decodeRuntimeWSPayload[RuntimeRunCancelAckPayload](terminalAckEnvelope, RuntimeRunCancelAck)
		if err != nil || terminalAck.CancelState != RuntimeCancelStopped {
			return fmt.Errorf("terminal cancel ACK = %#v, %w", terminalAck, err)
		}
		return nil
	}, serverErr)
	defer server.Close()

	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtimeClient.DialRuntimeWebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if connection.Ready().CoreInstanceID != runtimeTestCoreID {
		t.Fatalf("ready = %#v", connection.Ready())
	}

	assigned, err := connection.ClaimRuntimeRun(context.Background(), 25, RuntimeClaimRequest{
		RuntimeSessionID: hello.RuntimeSessionID, Capacity: 1,
	})
	if err != nil || assigned == nil {
		t.Fatalf("assignment = %#v, %v", assigned, err)
	}
	confirmed, err := connection.AckRuntimeAssignment(context.Background(), RuntimeAssignmentAckPayload{
		AttemptIdentity: assigned.AttemptIdentity,
	})
	if err != nil || confirmed.AttemptNo != 1 {
		t.Fatalf("confirmation = %#v, %v", confirmed, err)
	}
	commands, err := connection.PollRuntimeCommands(context.Background(), hello.RuntimeSessionID, 25)
	if err != nil || len(commands.Commands) != 1 {
		t.Fatalf("commands = %#v, %v", commands, err)
	}
	decoded, err := commands.Commands[0].Decode()
	if err != nil || decoded.Cancel == nil {
		t.Fatalf("decoded command = %#v, %v", decoded, err)
	}
	state, err := connection.AckRuntimeCancel(context.Background(), RuntimeRunCancelAckPayload{
		CancellationID: decoded.Cancel.CancellationID, AttemptIdentity: decoded.Cancel.AttemptIdentity,
		CancelState: RuntimeCancelStopping,
	})
	if err != nil || state.CancelState != RuntimeCancelStopping {
		t.Fatalf("cancel state = %#v, %v", state, err)
	}
	state, err = connection.AckRuntimeCancel(context.Background(), RuntimeRunCancelAckPayload{
		CancellationID: decoded.Cancel.CancellationID, AttemptIdentity: decoded.Cancel.AttemptIdentity,
		CancelState: RuntimeCancelStopped,
	})
	if err != nil || state.CancelState != RuntimeCancelStopped {
		t.Fatalf("terminal cancel state = %#v, %v", state, err)
	}
	connection.correlationMu.RLock()
	remainingCancellations := len(connection.cancellations)
	connection.correlationMu.RUnlock()
	if remainingCancellations != 0 {
		t.Fatalf("terminal cancellation correlations = %d, want 0", remainingCancellations)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeWebSocketCorrelatesConcurrentBusinessACKs(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeTestHello()
	identity := runtimeTestIdentity()
	serverErr := make(chan error, 1)
	server := newRuntimeSDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		if err := serveRuntimeSDKWSReady(conn, now); err != nil {
			return err
		}
		first, err := readRuntimeSDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		second, err := readRuntimeSDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		eventByID := map[string]RuntimeRunEventPayload{}
		for _, envelope := range []RuntimeEnvelope{first, second} {
			event, decodeErr := decodeRuntimeWSPayload[RuntimeRunEventPayload](envelope, RuntimeRunEvent)
			if decodeErr != nil {
				return decodeErr
			}
			eventByID[envelope.MessageID] = event
		}
		// Reverse write order. A FIFO response implementation would return the
		// wrong business ACK to one caller.
		for _, envelope := range []RuntimeEnvelope{second, first} {
			event := eventByID[envelope.MessageID]
			if err := writeRuntimeSDKWSEnvelope(conn, RuntimeRunEventAck, envelope.MessageID, RuntimeRunEventAckPayload{
				ClientEventID: event.ClientEventID, ClientEventSeq: event.ClientEventSeq,
				Sequence: event.ClientEventSeq,
			}); err != nil {
				return err
			}
		}
		return nil
	}, serverErr)
	defer server.Close()
	runtimeClient, _ := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	connection, err := runtimeClient.DialRuntimeWebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	requests := []RuntimeRunEventPayload{
		{AttemptIdentity: identity, ClientEventID: runtimeTestEventID, ClientEventSeq: 1, EventType: "run.progress", Payload: map[string]any{"n": 1}},
		{AttemptIdentity: identity, ClientEventID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", ClientEventSeq: 2, EventType: "run.progress", Payload: map[string]any{"n": 2}},
	}
	var wait sync.WaitGroup
	errs := make(chan error, len(requests))
	for _, request := range requests {
		request := request
		wait.Add(1)
		go func() {
			defer wait.Done()
			ack, callErr := connection.AppendRuntimeEvent(context.Background(), request)
			if callErr != nil {
				errs <- callErr
				return
			}
			if ack.ClientEventID != request.ClientEventID || ack.ClientEventSeq != request.ClientEventSeq {
				errs <- errors.New("business ACK was delivered to the wrong request")
			}
		}()
	}
	wait.Wait()
	close(errs)
	for err = range errs {
		t.Error(err)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeWebSocketResumeCollectsEveryCorrelatedDecision(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeTestHello()
	first := runtimeTestIdentity()
	second := first
	second.RunID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	second.AttemptID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	second.LeaseID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	serverErr := make(chan error, 1)
	server := newRuntimeSDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		if err := serveRuntimeSDKWSReady(conn, now); err != nil {
			return err
		}
		resumeEnvelope, err := readRuntimeSDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		resume, err := decodeRuntimeWSPayload[RuntimeResumePayload](resumeEnvelope, RuntimeResume)
		if err != nil || len(resume.Attempts) != 2 {
			return fmt.Errorf("resume = %#v, %w", resume, err)
		}
		leaseExpiry := now.Add(time.Minute)
		decisions := []RuntimeResumeAcceptedPayload{
			{AttemptIdentity: first, Decision: RuntimeResumeContinue, LeaseExpiresAt: &leaseExpiry, AllowedActions: []RuntimeResumeAction{RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult}},
			{AttemptIdentity: second, Decision: RuntimeResumeResultAcked, AllowedActions: []RuntimeResumeAction{RuntimeActionClearSpool}},
		}
		for _, decision := range decisions {
			if err = writeRuntimeSDKWSEnvelope(conn, RuntimeResumeAccepted, resumeEnvelope.MessageID, decision); err != nil {
				return err
			}
		}
		return nil
	}, serverErr)
	defer server.Close()
	runtimeClient, _ := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	connection, err := runtimeClient.DialRuntimeWebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	response, err := connection.ResumeRuntimeRuns(context.Background(), RuntimeResumePayload{
		NodeID: hello.NodeID, AgentID: hello.AgentID, WorkerID: hello.WorkerID,
		RuntimeSessionID: hello.RuntimeSessionID,
		Attempts: []RuntimeResumeAttempt{
			{AttemptIdentity: first, PendingClientEventRanges: []RuntimeEventRange{}},
			{AttemptIdentity: second, PendingClientEventRanges: []RuntimeEventRange{}},
		},
	})
	if err != nil || len(response.Decisions) != 2 || response.Decisions[1].Decision != RuntimeResumeResultAcked {
		t.Fatalf("resume response = %#v, %v", response, err)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeWebSocketResumeRejectsPartialDecisionsOnClose(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeTestHello()
	first := runtimeTestIdentity()
	second := first
	second.RunID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	second.AttemptID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	second.LeaseID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	serverErr := make(chan error, 1)
	server := newRuntimeSDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		if err := serveRuntimeSDKWSReady(conn, now); err != nil {
			return err
		}
		resumeEnvelope, err := readRuntimeSDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		leaseExpiry := now.Add(time.Minute)
		return writeRuntimeSDKWSEnvelope(conn, RuntimeResumeAccepted, resumeEnvelope.MessageID, RuntimeResumeAcceptedPayload{
			AttemptIdentity: first, Decision: RuntimeResumeContinue, LeaseExpiresAt: &leaseExpiry,
			AllowedActions: []RuntimeResumeAction{RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult},
		})
	}, serverErr)
	defer server.Close()
	runtimeClient, _ := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	connection, err := runtimeClient.DialRuntimeWebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	response, err := connection.ResumeRuntimeRuns(context.Background(), RuntimeResumePayload{
		NodeID: hello.NodeID, AgentID: hello.AgentID, WorkerID: hello.WorkerID,
		RuntimeSessionID: hello.RuntimeSessionID,
		Attempts: []RuntimeResumeAttempt{
			{AttemptIdentity: first, PendingClientEventRanges: []RuntimeEventRange{}},
			{AttemptIdentity: second, PendingClientEventRanges: []RuntimeEventRange{}},
		},
	})
	if err == nil || response != nil {
		t.Fatalf("partial resume response = %#v, %v", response, err)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeWebSocketResumePreservesErrorAfterPartialDecision(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeTestHello()
	first := runtimeTestIdentity()
	second := first
	second.RunID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	second.AttemptID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	second.LeaseID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	serverErr := make(chan error, 1)
	server := newRuntimeSDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		if err := serveRuntimeSDKWSReady(conn, now); err != nil {
			return err
		}
		resumeEnvelope, err := readRuntimeSDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		leaseExpiry := now.Add(time.Minute)
		if err = writeRuntimeSDKWSEnvelope(conn, RuntimeResumeAccepted, resumeEnvelope.MessageID, RuntimeResumeAcceptedPayload{
			AttemptIdentity: first, Decision: RuntimeResumeContinue, LeaseExpiresAt: &leaseExpiry,
			AllowedActions: []RuntimeResumeAction{RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult},
		}); err != nil {
			return err
		}
		return writeRuntimeSDKWSEnvelope(conn, RuntimeError, resumeEnvelope.MessageID, RuntimeErrorBody{
			Code: "EVENTS_MISSING", Message: "Event range is missing",
			MissingEventRanges: []RuntimeEventRange{{Start: 1, End: 1}},
		})
	}, serverErr)
	defer server.Close()
	runtimeClient, _ := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	connection, err := runtimeClient.DialRuntimeWebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	response, err := connection.ResumeRuntimeRuns(context.Background(), RuntimeResumePayload{
		NodeID: hello.NodeID, AgentID: hello.AgentID, WorkerID: hello.WorkerID,
		RuntimeSessionID: hello.RuntimeSessionID,
		Attempts: []RuntimeResumeAttempt{
			{AttemptIdentity: first, PendingClientEventRanges: []RuntimeEventRange{}},
			{AttemptIdentity: second, PendingClientEventRanges: []RuntimeEventRange{}},
		},
	})
	var runtimeErr *Error
	if response != nil || !errors.As(err, &runtimeErr) || runtimeErr.Code != "EVENTS_MISSING" {
		t.Fatalf("resume error = %#v, %#v", response, err)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeWebSocketCanceledRequestsDoNotLeakPendingEntries(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeTestHello()
	serverDone := make(chan struct{})
	serverErr := make(chan error, 1)
	server := newRuntimeSDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		if err := serveRuntimeSDKWSReady(conn, now); err != nil {
			return err
		}
		for index := 0; index < 64; index++ {
			if _, err := readRuntimeSDKWSEnvelope(conn); err != nil {
				return err
			}
		}
		close(serverDone)
		<-time.After(100 * time.Millisecond)
		return nil
	}, serverErr)
	defer server.Close()
	runtimeClient, _ := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	connection, err := runtimeClient.DialRuntimeWebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	identity := runtimeTestIdentity()
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func(sequence int) {
			defer wait.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			_, _ = connection.AppendRuntimeEvent(ctx, RuntimeRunEventPayload{
				AttemptIdentity: identity,
				ClientEventID:   runtimeSDKTestUUID(sequence + 1),
				ClientEventSeq:  int64(sequence + 1),
				EventType:       "run.progress",
				Payload:         map[string]any{"sequence": sequence + 1},
			})
		}(index)
	}
	wait.Wait()
	<-serverDone
	connection.pendingMu.Lock()
	pending := len(connection.pending)
	abandoned := len(connection.abandoned)
	connection.pendingMu.Unlock()
	if pending != 0 || abandoned > runtimeWSLateReplyLimit {
		t.Fatalf("pending=%d abandoned=%d", pending, abandoned)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeWebSocketRejectsUnknownPayloadAndReportsProtocolClose(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeTestHello()
	closeCode := make(chan int, 1)
	serverErr := make(chan error, 1)
	server := newRuntimeSDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		if err := serveRuntimeSDKWSReady(conn, now); err != nil {
			return err
		}
		_, raw, err := newRuntimeWSEnvelope(RuntimeRunAssigned, "", runtimeTestAssignment(now, runtimeTestIdentity()))
		if err != nil {
			return err
		}
		var object map[string]any
		if err = json.Unmarshal(raw, &object); err != nil {
			return err
		}
		object["payload"].(map[string]any)["unexpected"] = true
		if err = conn.WriteJSON(object); err != nil {
			return err
		}
		_, _, err = conn.NextReader()
		var closed *websocket.CloseError
		if errors.As(err, &closed) {
			closeCode <- closed.Code
			return nil
		}
		return fmt.Errorf("expected protocol close, got %w", err)
	}, serverErr)
	defer server.Close()
	runtimeClient, _ := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	connection, err := runtimeClient.DialRuntimeWebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-connection.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("WebSocket did not close")
	}
	if code := <-closeCode; code != RuntimeWSCloseProtocolError {
		t.Fatalf("close code = %d", code)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeWebSocketNilCloseReturnsError(t *testing.T) {
	var connection *RuntimeWebSocket
	err := connection.CloseRuntimeSession(context.Background(), RuntimeSessionCloseRequest{
		NodeID: runtimeTestNodeID, AgentID: runtimeTestAgentID, WorkerID: "worker-a",
		RuntimeSessionID: runtimeTestSessionID, SessionEpoch: 1,
		Status: "closed", Reason: "test_close",
	})
	if err == nil {
		t.Fatal("nil WebSocket close must return an error")
	}
}

func TestRuntimeWebSocketAttachInvalidatesPullGeneration(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	serverErr := make(chan error, 1)
	server := newRuntimeSDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		return serveRuntimeSDKWSReady(conn, now)
	}, serverErr)
	defer server.Close()
	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient.attachmentMu.Lock()
	runtimeClient.attachmentID = runtimeTestAttachmentID
	runtimeClient.attachmentMu.Unlock()
	connection, err := runtimeClient.DialRuntimeWebSocket(context.Background(), runtimeTestHello())
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	runtimeClient.attachmentMu.RLock()
	attachmentID := runtimeClient.attachmentID
	runtimeClient.attachmentMu.RUnlock()
	if attachmentID != "" {
		t.Fatalf("Pull attachment remained after WebSocket attach: %q", attachmentID)
	}
	if _, err = runtimeClient.HeartbeatRuntimeSession(context.Background(), runtimeTestHello()); err == nil {
		t.Fatal("HTTP heartbeat reused the generation detached by WebSocket attach")
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeWebSocketFailedAttachStillInvalidatesPullGeneration(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		http.Error(w, "upgrade unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()
	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient.attachmentMu.Lock()
	runtimeClient.attachmentID = runtimeTestAttachmentID
	runtimeClient.attachmentMu.Unlock()
	if _, err = runtimeClient.DialRuntimeWebSocket(context.Background(), runtimeTestHello()); err == nil {
		t.Fatal("failed WebSocket upgrade returned no error")
	}
	runtimeClient.attachmentMu.RLock()
	attachmentID := runtimeClient.attachmentID
	runtimeClient.attachmentMu.RUnlock()
	if attachmentID != "" {
		t.Fatalf("ambiguous WebSocket attach retained Pull generation %q", attachmentID)
	}
}

func TestRuntimeWebSocketAttachWaitsForInflightPullGeneration(t *testing.T) {
	pullStarted := make(chan struct{})
	releasePull := make(chan struct{})
	webSocketReached := make(chan struct{})
	serverErr := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path == "/api/v1/agent-runtime/ws" {
			close(webSocketReached)
			conn, err := upgrader.Upgrade(w, request, nil)
			if err != nil {
				serverErr <- err
				return
			}
			defer conn.Close()
			serverErr <- serveRuntimeSDKWSReady(conn, time.Now().UTC())
			return
		}
		if request.Header.Get(RuntimeAttachmentHeader) != runtimeTestAttachmentID {
			http.Error(w, "wrong attachment", http.StatusConflict)
			return
		}
		close(pullStarted)
		<-releasePull
		w.Header().Set("Content-Type", "application/json")
		_ = json.NewEncoder(w).Encode(RuntimeRunEventAckPayload{
			ClientEventID: runtimeTestEventID, ClientEventSeq: 1, Sequence: 1,
		})
	}))
	defer server.Close()
	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	runtimeClient.attachmentMu.Lock()
	runtimeClient.attachmentID = runtimeTestAttachmentID
	runtimeClient.attachmentMu.Unlock()
	eventDone := make(chan error, 1)
	go func() {
		_, eventErr := runtimeClient.AppendRuntimeEvent(context.Background(), RuntimeRunEventPayload{
			AttemptIdentity: runtimeTestIdentity(), ClientEventID: runtimeTestEventID,
			ClientEventSeq: 1, EventType: "run.progress", Payload: RuntimeJSONMap{"step": 1},
		})
		eventDone <- eventErr
	}()
	<-pullStarted
	dialDone := make(chan struct {
		connection *RuntimeWebSocket
		err        error
	}, 1)
	go func() {
		connection, dialErr := runtimeClient.DialRuntimeWebSocket(context.Background(), runtimeTestHello())
		dialDone <- struct {
			connection *RuntimeWebSocket
			err        error
		}{connection: connection, err: dialErr}
	}()
	select {
	case <-webSocketReached:
		t.Fatal("WebSocket attach crossed an in-flight Pull generation")
	case <-time.After(100 * time.Millisecond):
	}
	close(releasePull)
	if err = <-eventDone; err != nil {
		t.Fatal(err)
	}
	result := <-dialDone
	if result.err != nil {
		t.Fatal(result.err)
	}
	defer result.connection.Close()
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func newRuntimeSDKWSServer(
	t *testing.T,
	handle func(*http.Request, *websocket.Conn) error,
	result chan<- error,
) *httptest.Server {
	t.Helper()
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			result <- err
			return
		}
		defer conn.Close()
		result <- handle(request, conn)
	}))
}

func serveRuntimeSDKWSReady(conn *websocket.Conn, now time.Time) error {
	hello, err := readRuntimeSDKWSEnvelope(conn)
	if err != nil {
		return err
	}
	return writeRuntimeSDKWSEnvelope(conn, RuntimeReady, hello.MessageID, RuntimeReadyPayload{
		CoreInstanceID: runtimeTestCoreID, AttachmentID: runtimeTestAttachmentID, Features: RuntimeRequiredFeatures(),
		OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
	})
}

func readRuntimeSDKWSEnvelope(conn *websocket.Conn) (RuntimeEnvelope, error) {
	messageType, raw, err := conn.ReadMessage()
	if err != nil {
		return RuntimeEnvelope{}, err
	}
	if messageType != websocket.TextMessage {
		return RuntimeEnvelope{}, errors.New("message was not text")
	}
	return decodeRuntimeWSEnvelope(raw)
}

func writeRuntimeSDKWSEnvelope(
	conn *websocket.Conn,
	messageType RuntimeMessageType,
	replyTo string,
	payload any,
) error {
	_, err := writeRuntimeSDKWSEnvelopeID(conn, messageType, replyTo, payload)
	return err
}

func writeRuntimeSDKWSEnvelopeID(
	conn *websocket.Conn,
	messageType RuntimeMessageType,
	replyTo string,
	payload any,
) (string, error) {
	envelope, raw, err := newRuntimeWSEnvelope(messageType, replyTo, payload)
	if err != nil {
		return "", err
	}
	return envelope.MessageID, conn.WriteMessage(websocket.TextMessage, raw)
}

func runtimeTestAssignment(now time.Time, identity RuntimeAttemptIdentity) RuntimeRunAssignedPayload {
	return RuntimeRunAssignedPayload{
		AttemptIdentity: identity, OfferNo: 1, OfferExpiresAt: now.Add(time.Minute),
		AttemptDeadlineAt: now.Add(2 * time.Minute), RunDeadlineAt: now.Add(3 * time.Minute),
		Input: map[string]any{"task": "test"}, Metadata: map[string]any{},
		NodeEnvelope:         "ol_ctx_v2.current.payload.signature",
		AgentInvocationToken: "ol_inv_v2.current.payload.signature",
	}
}

func runtimeSDKTestUUID(value int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", value, value)
}
