package openlinker

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

const (
	testExtensionCommand RuntimeMessageType = "test.control.command"
	testExtensionRequest RuntimeMessageType = "test.control.frame"
	testExtensionReply   RuntimeMessageType = "test.control.frame_ack"
)

func TestRuntimeExtensionsRequireExplicitBoundedRegistration(t *testing.T) {
	routes, registry, err := normalizeRuntimeExtensionRoutes(
		[]RuntimeExtensionRoute{testExtensionRoute()},
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(routes) != 1 || registry == nil {
		t.Fatalf("normalized Runtime extension routes = %#v, %#v", routes, registry)
	}
	for _, invalid := range [][]RuntimeExtensionRoute{
		{{
			CommandType: RuntimeRunCancel,
			RequestType: testExtensionRequest,
			ReplyType:   testExtensionReply,
		}},
		{{
			CommandType: testExtensionCommand,
			RequestType: testExtensionCommand,
			ReplyType:   testExtensionReply,
		}},
		{{
			CommandType: "UPPER.command",
			RequestType: testExtensionRequest,
			ReplyType:   testExtensionReply,
		}},
		{{
			CommandType: "runtime.custom",
			RequestType: testExtensionRequest,
			ReplyType:   testExtensionReply,
		}},
	} {
		if _, _, err := normalizeRuntimeExtensionRoutes(invalid); err == nil {
			t.Fatalf("invalid Runtime extension route was accepted: %#v", invalid)
		}
	}
	tooMany := make([]RuntimeExtensionRoute, RuntimeMaxExtensionRoutes+1)
	if _, _, err := normalizeRuntimeExtensionRoutes(tooMany); err == nil {
		t.Fatal("unbounded Runtime extension route set was accepted")
	}
}

func TestRuntimeExtensionsRouteOnlyRegisteredAttemptScopedMessages(t *testing.T) {
	_, registry, err := normalizeRuntimeExtensionRoutes(
		[]RuntimeExtensionRoute{testExtensionRoute()},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := testRuntimeExtensionAttemptIdentity()
	payload, err := json.Marshal(map[string]any{
		"attempt_identity": identity,
		"extension_value":  "opaque-to-sdk",
	})
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := decodeRuntimeExtensionCommand(
		RuntimePendingCommand{Type: testExtensionCommand, Payload: payload},
		registry,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Extension == nil ||
		decoded.Extension.AttemptIdentity != identity ||
		string(decoded.Extension.Payload) != string(payload) {
		t.Fatalf("decoded Runtime extension = %#v", decoded.Extension)
	}
	if _, err = decodeRuntimeExtensionCommand(
		RuntimePendingCommand{Type: "test.unregistered.command", Payload: payload},
		registry,
	); err == nil {
		t.Fatal("unregistered Runtime extension command was accepted")
	}

	envelope, _, err := newRuntimeWSEnvelopeWithExtensions(
		testExtensionCommand,
		"",
		json.RawMessage(payload),
		registry,
	)
	if err != nil {
		t.Fatal(err)
	}
	if envelope.Type != testExtensionCommand {
		t.Fatalf("Runtime extension envelope type = %q", envelope.Type)
	}
	if _, _, err = newRuntimeWSEnvelope(
		testExtensionCommand,
		"",
		json.RawMessage(payload),
	); err == nil {
		t.Fatal("unregistered Runtime extension envelope was accepted")
	}
}

func TestRuntimeExtensionRepliesRemainCorrelatedAndOpaque(t *testing.T) {
	_, registry, err := normalizeRuntimeExtensionRoutes(
		[]RuntimeExtensionRoute{testExtensionRoute()},
	)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err = newRuntimeWSEnvelopeWithExtensions(
		testExtensionReply,
		"",
		map[string]any{"status": "ok"},
		registry,
	); err == nil || !strings.Contains(err.Error(), "reply_to_message_id") {
		t.Fatalf("uncorrelated Runtime extension reply error = %v", err)
	}
	replyTo := "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
	envelope, _, err := newRuntimeWSEnvelopeWithExtensions(
		testExtensionReply,
		replyTo,
		map[string]any{"extension_ack": true},
		registry,
	)
	if err != nil {
		t.Fatal(err)
	}
	if err = validateRuntimeWSReplyPayloadWithExtensions(
		envelope,
		registry,
	); err != nil {
		t.Fatal(err)
	}
	if err = validateRuntimeWSReplyPayload(envelope); err == nil {
		t.Fatal("extension reply was accepted without its registry")
	}
}

func TestRuntimeWebSocketRoutesRegisteredExtensionsWithoutOwningTheirSchema(
	t *testing.T,
) {
	_, registry, err := normalizeRuntimeExtensionRoutes(
		[]RuntimeExtensionRoute{testExtensionRoute()},
	)
	if err != nil {
		t.Fatal(err)
	}
	identity := runtimeTestIdentity()
	commandPayload, err := json.Marshal(map[string]any{
		"attempt_identity": identity,
		"product_action":   "owned-by-consumer",
	})
	if err != nil {
		t.Fatal(err)
	}
	serverErr := make(chan error, 1)
	server := newRuntimeSDKWSServer(
		t,
		func(_ *http.Request, conn *websocket.Conn) error {
			if err := serveRuntimeSDKWSReady(conn, time.Now().UTC()); err != nil {
				return err
			}
			if err := writeRuntimeSDKWSExtensionEnvelope(
				conn,
				registry,
				testExtensionCommand,
				"",
				json.RawMessage(commandPayload),
			); err != nil {
				return err
			}
			_, raw, err := conn.ReadMessage()
			if err != nil {
				return err
			}
			request, err := decodeRuntimeWSEnvelopeWithExtensions(raw, registry)
			if err != nil {
				return err
			}
			if request.Type != testExtensionRequest {
				return fmt.Errorf("Runtime extension request type = %q", request.Type)
			}
			return writeRuntimeSDKWSExtensionEnvelope(
				conn,
				registry,
				testExtensionReply,
				request.MessageID,
				map[string]any{"product_ack": "opaque-to-sdk"},
			)
		},
		serverErr,
	)
	defer server.Close()
	runtimeClient, err := NewRuntime(
		server.URL,
		WithAgentToken("ol_agent_v2"),
	)
	if err != nil {
		t.Fatal(err)
	}
	connection, err := runtimeClient.dialRuntimeWebSocketWithExtensions(
		context.Background(),
		runtimeTestHello(),
		registry,
	)
	if err != nil {
		t.Fatal(err)
	}
	defer connection.Close()
	commands, err := connection.PollRuntimeCommands(
		context.Background(),
		runtimeTestSessionID,
		1,
	)
	if err != nil {
		t.Fatal(err)
	}
	if len(commands.Commands) != 1 {
		t.Fatalf("Runtime extension commands = %#v", commands.Commands)
	}
	decoded, err := decodeRuntimeExtensionCommand(
		commands.Commands[0],
		registry,
	)
	if err != nil {
		t.Fatal(err)
	}
	if decoded.Extension == nil ||
		decoded.Extension.AttemptIdentity != identity {
		t.Fatalf("Runtime extension push = %#v", decoded.Extension)
	}
	requestPayload, err := json.Marshal(map[string]any{
		"attempt_identity": identity,
		"product_frame":    "opaque-to-sdk",
	})
	if err != nil {
		t.Fatal(err)
	}
	reply, err := connection.PublishRuntimeExtension(
		context.Background(),
		RuntimeExtensionRequest{
			Type:    testExtensionRequest,
			Payload: requestPayload,
		},
	)
	if err != nil {
		t.Fatal(err)
	}
	if reply.Type != testExtensionReply ||
		!strings.Contains(string(reply.Payload), "opaque-to-sdk") {
		t.Fatalf("Runtime extension reply = %#v", reply)
	}
	if err = <-serverErr; err != nil {
		t.Fatal(err)
	}
}

func testExtensionRoute() RuntimeExtensionRoute {
	return RuntimeExtensionRoute{
		CommandType: testExtensionCommand,
		RequestType: testExtensionRequest,
		ReplyType:   testExtensionReply,
	}
}

func testRuntimeExtensionAttemptIdentity() RuntimeAttemptIdentity {
	return RuntimeAttemptIdentity{
		RunID:            "11111111-1111-4111-8111-111111111111",
		AttemptID:        "22222222-2222-4222-8222-222222222222",
		LeaseID:          "33333333-3333-4333-8333-333333333333",
		FencingToken:     1,
		NodeID:           "44444444-4444-4444-8444-444444444444",
		AgentID:          "55555555-5555-4555-8555-555555555555",
		WorkerID:         "66666666-6666-4666-8666-666666666666",
		RuntimeSessionID: "77777777-7777-4777-8777-777777777777",
	}
}

func testRuntimeExtensionEnvelope(
	messageType RuntimeMessageType,
	replyTo string,
	payload json.RawMessage,
) RuntimeEnvelope {
	return RuntimeEnvelope{
		RuntimeEnvelopeFields: RuntimeEnvelopeFields{
			ProtocolVersion:   RuntimeProtocolVersion,
			RuntimeContractID: RuntimeContractID,
			MessageID:         "88888888-8888-4888-8888-888888888888",
			ReplyToMessageID:  replyTo,
			Type:              messageType,
			SentAt:            time.Now().UTC(),
		},
		Payload: payload,
	}
}

func writeRuntimeSDKWSExtensionEnvelope(
	conn *websocket.Conn,
	registry *runtimeExtensionRegistry,
	messageType RuntimeMessageType,
	replyTo string,
	payload any,
) error {
	_, raw, err := newRuntimeWSEnvelopeWithExtensions(
		messageType,
		replyTo,
		payload,
		registry,
	)
	if err != nil {
		return err
	}
	return conn.WriteMessage(websocket.TextMessage, raw)
}
