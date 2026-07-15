package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/runtimetest"
	"github.com/gorilla/websocket"
)

func TestRunUsesWebSocketProtocolPrimitives(t *testing.T) {
	errCh := make(chan error, 1)
	upgrader := websocket.Upgrader{CheckOrigin: func(*http.Request) bool { return true }}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/api/v1/agent-runtime/ws" || request.Header.Get("Authorization") != "Bearer "+runtimetest.AgentToken {
			errCh <- fmt.Errorf("handshake path/auth = %s / %q", request.URL.Path, request.Header.Get("Authorization"))
			return
		}
		conn, err := upgrader.Upgrade(w, request, nil)
		if err != nil {
			errCh <- err
			return
		}
		defer conn.Close()
		var helloEnvelope openlinker.RuntimeEnvelope
		if err = conn.ReadJSON(&helloEnvelope); err != nil {
			errCh <- err
			return
		}
		var hello openlinker.RuntimeHelloPayload
		if err = json.Unmarshal(helloEnvelope.Payload, &hello); err != nil {
			errCh <- err
			return
		}
		if err = writeEnvelope(conn, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa1", helloEnvelope.MessageID, openlinker.RuntimeReady, openlinker.RuntimeReadyPayload{
			CoreInstanceID: runtimetest.CoreID, AttachmentID: runtimetest.AttachmentID, Features: openlinker.RuntimeRequiredFeatures(),
			OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: time.Now().UTC(),
		}); err != nil {
			errCh <- err
			return
		}
		identity := openlinker.RuntimeAttemptIdentity{
			RunID: runtimetest.RunID, AttemptID: runtimetest.AttemptID, LeaseID: runtimetest.LeaseID, FencingToken: 1,
			NodeID: hello.NodeID, AgentID: hello.AgentID, WorkerID: hello.WorkerID, RuntimeSessionID: hello.RuntimeSessionID,
		}
		if err = writeEnvelope(conn, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2", "", openlinker.RuntimeRunAssigned, openlinker.RuntimeRunAssignedPayload{
			AttemptIdentity: identity, OfferNo: 1, OfferExpiresAt: time.Now().Add(time.Minute).UTC(),
			AttemptDeadlineAt: time.Now().Add(time.Minute).UTC(), RunDeadlineAt: time.Now().Add(2 * time.Minute).UTC(),
			Input: map[string]any{"text": "hello"}, NodeEnvelope: "ol_ctx_v2.header.payload.signature", AgentInvocationToken: "ol_inv_v2.header.payload.signature",
		}); err != nil {
			errCh <- err
			return
		}
		var rejectEnvelope openlinker.RuntimeEnvelope
		if err = conn.ReadJSON(&rejectEnvelope); err != nil {
			errCh <- err
			return
		}
		var reject openlinker.RuntimeAssignmentRejectPayload
		if err = json.Unmarshal(rejectEnvelope.Payload, &reject); err != nil {
			errCh <- err
			return
		}
		if rejectEnvelope.ReplyToMessageID != "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa2" {
			errCh <- fmt.Errorf("reject reply_to=%s", rejectEnvelope.ReplyToMessageID)
			return
		}
		if err = writeEnvelope(conn, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaa3", rejectEnvelope.MessageID, openlinker.RuntimeAssignmentRejected, openlinker.RuntimeAssignmentRejectedPayload{
			AttemptIdentity: reject.AttemptIdentity, Outcome: openlinker.RuntimeOfferRejected, DispatchState: openlinker.RuntimeDispatchPending,
		}); err != nil {
			errCh <- err
			return
		}
		errCh <- nil
	}))
	defer server.Close()

	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := run(ctx, config{RuntimeBase: server.URL, NodeID: runtimetest.NodeID, AgentID: runtimetest.AgentID, AgentToken: runtimetest.AgentToken, HTTPClient: server.Client()}, &output); err != nil {
		t.Fatal(err)
	}
	if err := <-errCh; err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"outcome": "offer_rejected"`) || !strings.Contains(output.String(), "durable spool") {
		t.Fatalf("output = %s", output.String())
	}
}

func writeEnvelope(conn *websocket.Conn, messageID, replyTo string, messageType openlinker.RuntimeMessageType, payload any) error {
	raw, err := json.Marshal(payload)
	if err != nil {
		return err
	}
	return conn.WriteJSON(openlinker.RuntimeEnvelope{
		RuntimeEnvelopeFields: openlinker.RuntimeEnvelopeFields{
			ProtocolVersion: openlinker.RuntimeProtocolVersion, RuntimeContractID: openlinker.RuntimeContractID,
			MessageID: messageID, ReplyToMessageID: replyTo, Type: messageType, SentAt: time.Now().UTC(),
		}, Payload: raw,
	})
}
