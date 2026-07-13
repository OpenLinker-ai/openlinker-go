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

func TestRuntimeV2WebSocketHandshakeAssignmentAndCancelCorrelation(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeV2TestHello()
	identity := runtimeV2TestIdentity()
	serverErr := make(chan error, 1)
	server := newRuntimeV2SDKWSServer(t, func(request *http.Request, conn *websocket.Conn) error {
		if request.URL.Path != "/api/v1/agent-runtime/ws" {
			return fmt.Errorf("WebSocket path = %q", request.URL.Path)
		}
		if got := request.Header.Get("Authorization"); got != "Bearer ol_agent_v2" {
			return fmt.Errorf("authorization = %q", got)
		}
		helloEnvelope, err := readRuntimeV2SDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		if helloEnvelope.Type != RuntimeV2Hello || helloEnvelope.ReplyToMessageID != "" {
			return fmt.Errorf("hello envelope = %#v", helloEnvelope.RuntimeV2EnvelopeFields)
		}
		if _, err = decodeRuntimeV2WSPayload[RuntimeV2HelloPayload](helloEnvelope, RuntimeV2Hello); err != nil {
			return err
		}
		if err = writeRuntimeV2SDKWSEnvelope(conn, RuntimeV2Ready, helloEnvelope.MessageID, RuntimeV2ReadyPayload{
			CoreInstanceID: runtimeV2TestCoreID, Features: RuntimeRequiredFeatures(),
			OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
		}); err != nil {
			return err
		}

		assignmentMessageID, err := writeRuntimeV2SDKWSEnvelopeID(conn, RuntimeV2RunAssigned, "", runtimeV2TestAssignment(now, identity))
		if err != nil {
			return err
		}
		ackEnvelope, err := readRuntimeV2SDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		if ackEnvelope.Type != RuntimeV2AssignmentAck || ackEnvelope.ReplyToMessageID != assignmentMessageID {
			return fmt.Errorf("assignment ACK correlation = %#v", ackEnvelope.RuntimeV2EnvelopeFields)
		}
		ack, err := decodeRuntimeV2WSPayload[RuntimeV2AssignmentAckPayload](ackEnvelope, RuntimeV2AssignmentAck)
		if err != nil || ack.AttemptIdentity != identity {
			return fmt.Errorf("assignment ACK = %#v, %w", ack, err)
		}
		if err = writeRuntimeV2SDKWSEnvelope(conn, RuntimeV2AssignmentConfirmed, ackEnvelope.MessageID, RuntimeV2AssignmentConfirmedPayload{
			AttemptIdentity: identity, AttemptNo: 1, LeaseExpiresAt: now.Add(time.Minute),
		}); err != nil {
			return err
		}

		cancel := RuntimeV2RunCancelPayload{
			CancellationID: runtimeV2TestCancellationID, AttemptIdentity: identity,
			ReasonCode: "OWNER_REQUEST", DeadlineAt: now.Add(time.Minute),
		}
		cancelMessageID, err := writeRuntimeV2SDKWSEnvelopeID(conn, RuntimeV2RunCancel, "", cancel)
		if err != nil {
			return err
		}
		cancelAckEnvelope, err := readRuntimeV2SDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		if cancelAckEnvelope.Type != RuntimeV2RunCancelAck || cancelAckEnvelope.ReplyToMessageID != cancelMessageID {
			return fmt.Errorf("cancel ACK correlation = %#v", cancelAckEnvelope.RuntimeV2EnvelopeFields)
		}
		cancelAck, err := decodeRuntimeV2WSPayload[RuntimeV2RunCancelAckPayload](cancelAckEnvelope, RuntimeV2RunCancelAck)
		if err != nil || cancelAck.CancelState != RuntimeV2CancelStopping {
			return fmt.Errorf("cancel ACK = %#v, %w", cancelAck, err)
		}
		return nil
	}, serverErr)
	defer server.Close()

	runtimeClient, err := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtimeClient.DialRuntimeV2WebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	if connection.Ready().CoreInstanceID != runtimeV2TestCoreID {
		t.Fatalf("ready = %#v", connection.Ready())
	}

	assigned, err := connection.ClaimRuntimeV2Run(context.Background(), 25, RuntimeV2ClaimRequest{
		RuntimeSessionID: hello.RuntimeSessionID, Capacity: 1,
	})
	if err != nil || assigned == nil {
		t.Fatalf("assignment = %#v, %v", assigned, err)
	}
	confirmed, err := connection.AckRuntimeV2Assignment(context.Background(), RuntimeV2AssignmentAckPayload{
		AttemptIdentity: assigned.AttemptIdentity,
	})
	if err != nil || confirmed.AttemptNo != 1 {
		t.Fatalf("confirmation = %#v, %v", confirmed, err)
	}
	commands, err := connection.PollRuntimeV2Commands(context.Background(), hello.RuntimeSessionID, 25)
	if err != nil || len(commands.Commands) != 1 {
		t.Fatalf("commands = %#v, %v", commands, err)
	}
	decoded, err := commands.Commands[0].Decode()
	if err != nil || decoded.Cancel == nil {
		t.Fatalf("decoded command = %#v, %v", decoded, err)
	}
	state, err := connection.AckRuntimeV2Cancel(context.Background(), RuntimeV2RunCancelAckPayload{
		CancellationID: decoded.Cancel.CancellationID, AttemptIdentity: decoded.Cancel.AttemptIdentity,
		CancelState: RuntimeV2CancelStopping,
	})
	if err != nil || state.CancelState != RuntimeV2CancelStopping {
		t.Fatalf("cancel state = %#v, %v", state, err)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeV2WebSocketCorrelatesConcurrentBusinessACKs(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeV2TestHello()
	identity := runtimeV2TestIdentity()
	serverErr := make(chan error, 1)
	server := newRuntimeV2SDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		if err := serveRuntimeV2SDKWSReady(conn, now); err != nil {
			return err
		}
		first, err := readRuntimeV2SDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		second, err := readRuntimeV2SDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		eventByID := map[string]RuntimeV2RunEventPayload{}
		for _, envelope := range []RuntimeV2Envelope{first, second} {
			event, decodeErr := decodeRuntimeV2WSPayload[RuntimeV2RunEventPayload](envelope, RuntimeV2RunEvent)
			if decodeErr != nil {
				return decodeErr
			}
			eventByID[envelope.MessageID] = event
		}
		// Reverse write order. A FIFO response implementation would return the
		// wrong business ACK to one caller.
		for _, envelope := range []RuntimeV2Envelope{second, first} {
			event := eventByID[envelope.MessageID]
			if err := writeRuntimeV2SDKWSEnvelope(conn, RuntimeV2RunEventAck, envelope.MessageID, RuntimeV2RunEventAckPayload{
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
	connection, err := runtimeClient.DialRuntimeV2WebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()

	requests := []RuntimeV2RunEventPayload{
		{AttemptIdentity: identity, ClientEventID: runtimeV2TestEventID, ClientEventSeq: 1, EventType: "run.progress", Payload: map[string]any{"n": 1}},
		{AttemptIdentity: identity, ClientEventID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", ClientEventSeq: 2, EventType: "run.progress", Payload: map[string]any{"n": 2}},
	}
	var wait sync.WaitGroup
	errs := make(chan error, len(requests))
	for _, request := range requests {
		request := request
		wait.Add(1)
		go func() {
			defer wait.Done()
			ack, callErr := connection.AppendRuntimeV2Event(context.Background(), request)
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

func TestRuntimeV2WebSocketResumeCollectsEveryCorrelatedDecision(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeV2TestHello()
	first := runtimeV2TestIdentity()
	second := first
	second.RunID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	second.AttemptID = "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb"
	second.LeaseID = "cccccccc-cccc-4ccc-8ccc-cccccccccccc"
	serverErr := make(chan error, 1)
	server := newRuntimeV2SDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		if err := serveRuntimeV2SDKWSReady(conn, now); err != nil {
			return err
		}
		resumeEnvelope, err := readRuntimeV2SDKWSEnvelope(conn)
		if err != nil {
			return err
		}
		resume, err := decodeRuntimeV2WSPayload[RuntimeV2ResumePayload](resumeEnvelope, RuntimeV2Resume)
		if err != nil || len(resume.Attempts) != 2 {
			return fmt.Errorf("resume = %#v, %w", resume, err)
		}
		leaseExpiry := now.Add(time.Minute)
		decisions := []RuntimeV2ResumeAcceptedPayload{
			{AttemptIdentity: first, Decision: RuntimeV2ResumeContinue, LeaseExpiresAt: &leaseExpiry, AllowedActions: []RuntimeV2ResumeAction{RuntimeV2ActionContinueExecution, RuntimeV2ActionUploadEvents, RuntimeV2ActionUploadResult}},
			{AttemptIdentity: second, Decision: RuntimeV2ResumeResultAcked, AllowedActions: []RuntimeV2ResumeAction{RuntimeV2ActionClearSpool}},
		}
		for _, decision := range decisions {
			if err = writeRuntimeV2SDKWSEnvelope(conn, RuntimeV2ResumeAccepted, resumeEnvelope.MessageID, decision); err != nil {
				return err
			}
		}
		return nil
	}, serverErr)
	defer server.Close()
	runtimeClient, _ := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	connection, err := runtimeClient.DialRuntimeV2WebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	response, err := connection.ResumeRuntimeV2Runs(context.Background(), RuntimeV2ResumePayload{
		NodeID: hello.NodeID, AgentID: hello.AgentID, WorkerID: hello.WorkerID,
		RuntimeSessionID: hello.RuntimeSessionID,
		Attempts: []RuntimeV2ResumeAttempt{
			{AttemptIdentity: first, PendingClientEventRanges: []RuntimeV2EventRange{}},
			{AttemptIdentity: second, PendingClientEventRanges: []RuntimeV2EventRange{}},
		},
	})
	if err != nil || len(response.Decisions) != 2 || response.Decisions[1].Decision != RuntimeV2ResumeResultAcked {
		t.Fatalf("resume response = %#v, %v", response, err)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeV2WebSocketCanceledRequestsDoNotLeakPendingEntries(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeV2TestHello()
	serverDone := make(chan struct{})
	serverErr := make(chan error, 1)
	server := newRuntimeV2SDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		if err := serveRuntimeV2SDKWSReady(conn, now); err != nil {
			return err
		}
		for index := 0; index < 64; index++ {
			if _, err := readRuntimeV2SDKWSEnvelope(conn); err != nil {
				return err
			}
		}
		close(serverDone)
		<-time.After(100 * time.Millisecond)
		return nil
	}, serverErr)
	defer server.Close()
	runtimeClient, _ := NewRuntime(server.URL, WithAgentToken("ol_agent_v2"))
	connection, err := runtimeClient.DialRuntimeV2WebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	identity := runtimeV2TestIdentity()
	var wait sync.WaitGroup
	for index := 0; index < 64; index++ {
		wait.Add(1)
		go func(sequence int) {
			defer wait.Done()
			ctx, cancel := context.WithTimeout(context.Background(), 20*time.Millisecond)
			defer cancel()
			_, _ = connection.AppendRuntimeV2Event(ctx, RuntimeV2RunEventPayload{
				AttemptIdentity: identity,
				ClientEventID:   runtimeV2SDKTestUUID(sequence + 1),
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
	if pending != 0 || abandoned > runtimeV2WSLateReplyLimit {
		t.Fatalf("pending=%d abandoned=%d", pending, abandoned)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeV2WebSocketRejectsUnknownPayloadAndReportsProtocolClose(t *testing.T) {
	now := time.Now().UTC().Truncate(time.Millisecond)
	hello := runtimeV2TestHello()
	closeCode := make(chan int, 1)
	serverErr := make(chan error, 1)
	server := newRuntimeV2SDKWSServer(t, func(_ *http.Request, conn *websocket.Conn) error {
		if err := serveRuntimeV2SDKWSReady(conn, now); err != nil {
			return err
		}
		_, raw, err := newRuntimeV2WSEnvelope(RuntimeV2RunAssigned, "", runtimeV2TestAssignment(now, runtimeV2TestIdentity()))
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
	connection, err := runtimeClient.DialRuntimeV2WebSocket(context.Background(), hello)
	if err != nil {
		t.Fatal(err)
	}
	select {
	case <-connection.Done():
	case <-time.After(2 * time.Second):
		t.Fatal("WebSocket did not close")
	}
	if code := <-closeCode; code != RuntimeV2WSCloseProtocolError {
		t.Fatalf("close code = %d", code)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeV2WebSocketNilCloseReturnsError(t *testing.T) {
	var connection *RuntimeV2WebSocket
	err := connection.CloseRuntimeV2Session(context.Background(), RuntimeV2SessionCloseRequest{
		NodeID: runtimeV2TestNodeID, AgentID: runtimeV2TestAgentID, WorkerID: "worker-a",
		RuntimeSessionID: runtimeV2TestSessionID, SessionEpoch: 1,
		Status: "closed", Reason: "test_close",
	})
	if err == nil {
		t.Fatal("nil WebSocket close must return an error")
	}
}

func newRuntimeV2SDKWSServer(
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

func serveRuntimeV2SDKWSReady(conn *websocket.Conn, now time.Time) error {
	hello, err := readRuntimeV2SDKWSEnvelope(conn)
	if err != nil {
		return err
	}
	return writeRuntimeV2SDKWSEnvelope(conn, RuntimeV2Ready, hello.MessageID, RuntimeV2ReadyPayload{
		CoreInstanceID: runtimeV2TestCoreID, Features: RuntimeRequiredFeatures(),
		OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
	})
}

func readRuntimeV2SDKWSEnvelope(conn *websocket.Conn) (RuntimeV2Envelope, error) {
	messageType, raw, err := conn.ReadMessage()
	if err != nil {
		return RuntimeV2Envelope{}, err
	}
	if messageType != websocket.TextMessage {
		return RuntimeV2Envelope{}, errors.New("message was not text")
	}
	return decodeRuntimeV2WSEnvelope(raw)
}

func writeRuntimeV2SDKWSEnvelope(
	conn *websocket.Conn,
	messageType RuntimeV2MessageType,
	replyTo string,
	payload any,
) error {
	_, err := writeRuntimeV2SDKWSEnvelopeID(conn, messageType, replyTo, payload)
	return err
}

func writeRuntimeV2SDKWSEnvelopeID(
	conn *websocket.Conn,
	messageType RuntimeV2MessageType,
	replyTo string,
	payload any,
) (string, error) {
	envelope, raw, err := newRuntimeV2WSEnvelope(messageType, replyTo, payload)
	if err != nil {
		return "", err
	}
	return envelope.MessageID, conn.WriteMessage(websocket.TextMessage, raw)
}

func runtimeV2TestAssignment(now time.Time, identity RuntimeV2AttemptIdentity) RuntimeV2RunAssignedPayload {
	return RuntimeV2RunAssignedPayload{
		AttemptIdentity: identity, OfferNo: 1, OfferExpiresAt: now.Add(time.Minute),
		AttemptDeadlineAt: now.Add(2 * time.Minute), RunDeadlineAt: now.Add(3 * time.Minute),
		Input: map[string]any{"task": "test"}, Metadata: map[string]any{},
		NodeEnvelope:         "ol_ctx_v2.current.payload.signature",
		AgentInvocationToken: "ol_inv_v2.current.payload.signature",
	}
}

func runtimeV2SDKTestUUID(value int) string {
	return fmt.Sprintf("%08x-0000-4000-8000-%012x", value, value)
}
