package openlinker

import (
	"bytes"
	"context"
	"crypto/rand"
	"crypto/tls"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"sync"
	"time"

	"github.com/gorilla/websocket"
)

const (
	RuntimeWSCloseAuthenticationFailed   = 4401
	RuntimeWSCloseClientUpgradeRequired  = 4406
	RuntimeWSCloseSessionConflict        = 4409
	RuntimeWSCloseRequiredFeatureMissing = 4412
	RuntimeWSCloseProtocolError          = 1002
	RuntimeWSCloseInternalError          = 1011

	runtimeWSWriteWait      = 10 * time.Second
	runtimeWSReadWait       = 75 * time.Second
	runtimeWSPingInterval   = 30 * time.Second
	runtimeWSHandshakeWait  = 10 * time.Second
	runtimeWSWriteQueueSize = 64
	runtimeWSPushQueueSize  = RuntimeMaxNodeCapacity
	runtimeWSLateReplyTTL   = 2 * time.Minute
	runtimeWSLateReplyLimit = 2048
)

// RuntimeWebSocketAssignment preserves the server message ID that an
// assignment ACK or rejection must correlate to. Payload alone is not enough
// to prove that the response belongs to a concrete offer.
type RuntimeWebSocketAssignment struct {
	MessageID string
	Payload   RuntimeRunAssignedPayload
}

// RuntimeWebSocketCommand is a strictly decoded server push. MessageID is
// required when acknowledging a cancellation; drain and lease revocation are
// one-way commands.
type RuntimeWebSocketCommand struct {
	MessageID string
	Command   RuntimeDecodedPendingCommand
}

// RuntimeWebSocket is one attached Runtime session. It has exactly one
// socket writer, correlates every business response by reply_to_message_id,
// and exposes only typed server pushes.
type RuntimeWebSocket struct {
	runtime *Runtime
	conn    *websocket.Conn
	hello   RuntimeHelloPayload
	ready   RuntimeReadyPayload

	ctx    context.Context
	cancel context.CancelFunc

	writes chan runtimeWSWrite
	done   chan struct{}

	assignments chan RuntimeWebSocketAssignment
	commands    chan RuntimeWebSocketCommand

	finishOnce sync.Once
	errMu      sync.RWMutex
	err        error

	pendingMu sync.Mutex
	pending   map[string]*runtimeWSPending
	abandoned map[string]time.Time

	correlationMu sync.RWMutex
	offers        map[string]string
	cancellations map[string]string
}

type runtimeWSWrite struct {
	message     []byte
	controlType int
	controlData []byte
	result      chan error
}

type runtimeWSPending struct {
	expected  map[RuntimeMessageType]struct{}
	remaining int
	replies   chan runtimeWSReply
}

type runtimeWSReply struct {
	envelope RuntimeEnvelope
	err      error
}

// DialRuntimeWebSocket authenticates the HTTP upgrade with the Agent Token
// and the mTLS transport configured through WithHTTPClient, sends runtime.hello,
// and returns only after a strictly validated runtime.ready reply.
func (r *Runtime) DialRuntimeWebSocket(
	ctx context.Context,
	hello RuntimeHelloPayload,
) (*RuntimeWebSocket, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateRuntimeHello(hello); err != nil {
		return nil, err
	}
	if r == nil || r.client == nil {
		return nil, errors.New("openlinker: runtime client is nil")
	}
	// A WebSocket hello rotates the same durable attachment as Pull create.
	// Wait for in-flight Pull operations, then fail closed by invalidating the
	// cached Pull generation before any upgrade bytes can reach Core. Even an
	// ambiguous/failed handshake must not let later Pull calls reuse a generation
	// that Core may already have detached.
	r.attachmentMu.Lock()
	r.attachmentID = ""
	defer r.attachmentMu.Unlock()
	conn, response, err := r.dialRuntimeWebSocket(ctx)
	if err != nil {
		return nil, runtimeWSDialError(response, err)
	}
	connectionCtx, cancel := context.WithCancel(context.Background())
	client := &RuntimeWebSocket{
		runtime:       r,
		conn:          conn,
		hello:         hello,
		ctx:           connectionCtx,
		cancel:        cancel,
		writes:        make(chan runtimeWSWrite, runtimeWSWriteQueueSize),
		done:          make(chan struct{}),
		assignments:   make(chan RuntimeWebSocketAssignment, runtimeWSPushQueueSize),
		commands:      make(chan RuntimeWebSocketCommand, runtimeWSPushQueueSize),
		pending:       make(map[string]*runtimeWSPending),
		abandoned:     make(map[string]time.Time),
		offers:        make(map[string]string),
		cancellations: make(map[string]string),
	}
	client.configureSocket()
	go client.writeLoop()
	go client.readLoop()

	envelope, err := client.requestOne(ctx, RuntimeHello, "", hello, RuntimeReady)
	if err != nil {
		client.finish(err)
		_ = conn.Close()
		return nil, err
	}
	ready, err := decodeRuntimeWSPayload[RuntimeReadyPayload](envelope, RuntimeReady)
	if err == nil {
		err = validateRuntimeReady(ready)
	}
	if err != nil {
		client.closeProtocol(err)
		return nil, err
	}
	client.ready = ready
	return client, nil
}

// ProbeRuntimeWebSocket verifies that the authenticated WebSocket upgrade is
// reachable without attaching a durable session. It is intended for an auto
// transport state machine that is currently serving through HTTP long-poll.
func (r *Runtime) ProbeRuntimeWebSocket(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, response, err := r.dialRuntimeWebSocket(ctx)
	if err != nil {
		return runtimeWSDialError(response, err)
	}
	deadline := time.Now().Add(runtimeWSWriteWait)
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "probe_complete"),
		deadline,
	)
	return conn.Close()
}

func (r *Runtime) dialRuntimeWebSocket(ctx context.Context) (*websocket.Conn, *http.Response, error) {
	if r == nil || r.client == nil {
		return nil, nil, errors.New("openlinker: runtime client is nil")
	}
	if err := r.client.requireRuntime(); err != nil {
		return nil, nil, err
	}
	transport := r.client.httpClient.Transport
	if transport == nil {
		transport = http.DefaultTransport
	}
	httpTransport, ok := transport.(*http.Transport)
	if !ok {
		return nil, nil, errors.New("openlinker: runtime WebSocket requires an *http.Transport")
	}
	dialer := websocket.Dialer{
		Proxy:             httpTransport.Proxy,
		NetDialContext:    httpTransport.DialContext,
		TLSClientConfig:   cloneTLSConfig(httpTransport),
		HandshakeTimeout:  runtimeWSHandshakeWait,
		EnableCompression: false,
	}
	target, err := url.Parse(r.client.endpoint("/agent-runtime/ws", nil))
	if err != nil {
		return nil, nil, fmt.Errorf("openlinker: parse runtime WebSocket URL: %w", err)
	}
	switch target.Scheme {
	case "https":
		target.Scheme = "wss"
	case "http":
		target.Scheme = "ws"
	default:
		return nil, nil, errors.New("openlinker: runtime WebSocket URL must use http or https")
	}
	headers := make(http.Header)
	headers.Set("Authorization", "Bearer "+r.client.agentToken)
	headers.Set("X-OpenLinker-SDK", r.client.sdkAgent)
	for name, values := range r.client.headers {
		for _, value := range values {
			headers.Add(name, value)
		}
	}
	return dialer.DialContext(ctx, target.String(), headers)
}

func cloneTLSConfig(transport *http.Transport) *tls.Config {
	if transport == nil || transport.TLSClientConfig == nil {
		return nil
	}
	return transport.TLSClientConfig.Clone()
}

func runtimeWSDialError(response *http.Response, cause error) error {
	if response == nil {
		return fmt.Errorf("openlinker: dial runtime WebSocket: %w", cause)
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return fmt.Errorf("openlinker: upgrade runtime WebSocket: %w", cause)
	}
	parsed := parseRuntimeError(response)
	if parsed != nil {
		return parsed
	}
	return fmt.Errorf("openlinker: upgrade runtime WebSocket: %w", cause)
}

func (c *RuntimeWebSocket) configureSocket() {
	c.conn.SetReadLimit(RuntimeMaxMessageBytes)
	_ = c.conn.SetReadDeadline(time.Now().Add(runtimeWSReadWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(runtimeWSReadWait))
	})
	c.conn.SetPingHandler(func(data string) error {
		return c.writeControl(websocket.PongMessage, []byte(data))
	})
}

// Ready returns the immutable handshake result for this attached session.
func (c *RuntimeWebSocket) Ready() RuntimeReadyPayload {
	if c == nil {
		return RuntimeReadyPayload{}
	}
	ready := c.ready
	ready.Features = append([]string(nil), c.ready.Features...)
	return ready
}

func (c *RuntimeWebSocket) Done() <-chan struct{} {
	if c == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return c.done
}

func (c *RuntimeWebSocket) Err() error {
	if c == nil {
		return errors.New("openlinker: runtime WebSocket is nil")
	}
	c.errMu.RLock()
	defer c.errMu.RUnlock()
	return c.err
}

func (c *RuntimeWebSocket) Assignments() <-chan RuntimeWebSocketAssignment {
	return c.assignments
}

func (c *RuntimeWebSocket) Commands() <-chan RuntimeWebSocketCommand {
	return c.commands
}

// Close performs a normal WebSocket close. Core detaches the session and
// releases only an unacknowledged offer; executing attempts remain recoverable.
func (c *RuntimeWebSocket) Close() error {
	if c == nil {
		return nil
	}
	err := c.writeControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "node_close"),
	)
	c.finish(nil)
	_ = c.conn.Close()
	return err
}

func (c *RuntimeWebSocket) readLoop() {
	for {
		messageType, reader, err := c.conn.NextReader()
		if err != nil {
			c.finish(runtimeWSReadError(err))
			return
		}
		if messageType != websocket.TextMessage {
			c.closeProtocol(errors.New("openlinker: runtime WebSocket accepts text messages only"))
			return
		}
		raw, err := io.ReadAll(io.LimitReader(reader, RuntimeMaxMessageBytes+1))
		if err != nil || int64(len(raw)) > RuntimeMaxMessageBytes {
			if err == nil {
				err = errors.New("openlinker: runtime WebSocket message exceeds 4 MiB")
			}
			c.closeProtocol(err)
			return
		}
		envelope, err := decodeRuntimeWSEnvelope(raw)
		if err != nil {
			c.closeProtocol(err)
			return
		}
		if err = c.routeEnvelope(envelope); err != nil {
			c.closeProtocol(err)
			return
		}
	}
}

func (c *RuntimeWebSocket) routeEnvelope(envelope RuntimeEnvelope) error {
	if envelope.ReplyToMessageID != "" {
		return c.routeReply(envelope)
	}
	switch envelope.Type {
	case RuntimeRunAssigned:
		payload, err := decodeRuntimeWSPayload[RuntimeRunAssignedPayload](envelope, RuntimeRunAssigned)
		if err == nil {
			err = validateRuntimeAssignment(payload)
		}
		if err != nil {
			return err
		}
		key := runtimeAttemptKey(payload.AttemptIdentity)
		c.correlationMu.Lock()
		c.offers[key] = envelope.MessageID
		c.correlationMu.Unlock()
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case c.assignments <- RuntimeWebSocketAssignment{MessageID: envelope.MessageID, Payload: payload}:
			return nil
		}
	case RuntimeRunCancel, RuntimeDrain, RuntimeLeaseRevoked:
		command := RuntimePendingCommand{Type: envelope.Type, Payload: append(json.RawMessage(nil), envelope.Payload...)}
		decoded, err := DecodeRuntimePendingCommand(command)
		if err != nil {
			return err
		}
		if decoded.Cancel != nil {
			c.correlationMu.Lock()
			c.cancellations[runtimeCancellationKey(*decoded.Cancel)] = envelope.MessageID
			c.correlationMu.Unlock()
		}
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case c.commands <- RuntimeWebSocketCommand{MessageID: envelope.MessageID, Command: decoded}:
			return nil
		}
	default:
		return fmt.Errorf("openlinker: unexpected runtime WebSocket push %q", envelope.Type)
	}
}

func (c *RuntimeWebSocket) routeReply(envelope RuntimeEnvelope) error {
	if err := validateRuntimeWSReplyPayload(envelope); err != nil {
		return err
	}
	c.pendingMu.Lock()
	c.pruneAbandonedLocked(time.Now())
	pending := c.pending[envelope.ReplyToMessageID]
	if pending == nil {
		_, abandoned := c.abandoned[envelope.ReplyToMessageID]
		c.pendingMu.Unlock()
		if abandoned {
			return nil
		}
		return errors.New("openlinker: runtime WebSocket reply has no pending request")
	}
	if envelope.Type != RuntimeError {
		if _, ok := pending.expected[envelope.Type]; !ok {
			c.pendingMu.Unlock()
			return errors.New("openlinker: runtime WebSocket reply type does not match request")
		}
	}
	pending.remaining--
	if pending.remaining == 0 || envelope.Type == RuntimeError {
		delete(c.pending, envelope.ReplyToMessageID)
	}
	c.pendingMu.Unlock()

	reply := runtimeWSReply{envelope: envelope}
	if envelope.Type == RuntimeError {
		body, err := decodeRuntimeWSPayload[RuntimeErrorBody](envelope, RuntimeError)
		if err == nil {
			err = validateRuntimeErrorBody(body)
		}
		if err != nil {
			return err
		}
		reply.err = &Error{Code: body.Code, Message: body.Message, Details: body}
	}
	select {
	case <-c.ctx.Done():
		return c.ctx.Err()
	case pending.replies <- reply:
		return nil
	}
}

func (c *RuntimeWebSocket) writeLoop() {
	ticker := time.NewTicker(runtimeWSPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case request := <-c.writes:
			_ = c.conn.SetWriteDeadline(time.Now().Add(runtimeWSWriteWait))
			var err error
			switch {
			case request.message != nil:
				err = c.conn.WriteMessage(websocket.TextMessage, request.message)
			case request.controlType != 0:
				err = c.conn.WriteControl(
					request.controlType,
					request.controlData,
					time.Now().Add(runtimeWSWriteWait),
				)
			default:
				err = errors.New("openlinker: empty runtime WebSocket write")
			}
			request.result <- err
			if err != nil || request.controlType == websocket.CloseMessage {
				if err != nil {
					c.finish(err)
				}
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(runtimeWSWriteWait))
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(runtimeWSWriteWait)); err != nil {
				c.finish(err)
				return
			}
		}
	}
}

func (c *RuntimeWebSocket) requestOne(
	ctx context.Context,
	messageType RuntimeMessageType,
	replyTo string,
	payload any,
	expected RuntimeMessageType,
) (RuntimeEnvelope, error) {
	replies, _, err := c.request(ctx, messageType, replyTo, payload, []RuntimeMessageType{expected}, 1)
	if err != nil {
		return RuntimeEnvelope{}, err
	}
	return replies[0], nil
}

func (c *RuntimeWebSocket) request(
	ctx context.Context,
	messageType RuntimeMessageType,
	replyTo string,
	payload any,
	expected []RuntimeMessageType,
	replyCount int,
) ([]RuntimeEnvelope, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if replyCount < 1 {
		return nil, "", errors.New("openlinker: runtime WebSocket request needs a reply")
	}
	envelope, raw, err := newRuntimeWSEnvelope(messageType, replyTo, payload)
	if err != nil {
		return nil, "", err
	}
	pending := &runtimeWSPending{
		expected:  make(map[RuntimeMessageType]struct{}, len(expected)),
		remaining: replyCount,
		replies:   make(chan runtimeWSReply, replyCount),
	}
	for _, allowed := range expected {
		pending.expected[allowed] = struct{}{}
	}
	c.pendingMu.Lock()
	if _, duplicate := c.pending[envelope.MessageID]; duplicate {
		c.pendingMu.Unlock()
		return nil, "", errors.New("openlinker: duplicate runtime WebSocket message ID")
	}
	c.pending[envelope.MessageID] = pending
	c.pendingMu.Unlock()
	if err := c.writeMessage(raw); err != nil {
		c.abandonPending(envelope.MessageID)
		return nil, envelope.MessageID, err
	}

	replies := make([]RuntimeEnvelope, 0, replyCount)
	for len(replies) < replyCount {
		select {
		case <-ctx.Done():
			c.abandonPending(envelope.MessageID)
			return nil, envelope.MessageID, ctx.Err()
		case <-c.done:
			return nil, envelope.MessageID, c.closedError()
		case reply := <-pending.replies:
			if reply.err != nil {
				return nil, envelope.MessageID, reply.err
			}
			replies = append(replies, reply.envelope)
		}
	}
	return replies, envelope.MessageID, nil
}

func (c *RuntimeWebSocket) writeMessage(raw []byte) error {
	request := runtimeWSWrite{message: append([]byte(nil), raw...), result: make(chan error, 1)}
	select {
	case <-c.done:
		return c.closedError()
	case c.writes <- request:
	}
	select {
	case <-c.done:
		select {
		case err := <-request.result:
			return err
		default:
			return c.closedError()
		}
	case err := <-request.result:
		return err
	}
}

func (c *RuntimeWebSocket) writeControl(messageType int, data []byte) error {
	request := runtimeWSWrite{
		controlType: messageType,
		controlData: append([]byte(nil), data...),
		result:      make(chan error, 1),
	}
	select {
	case <-c.done:
		return c.closedError()
	case c.writes <- request:
	}
	select {
	case <-c.done:
		select {
		case err := <-request.result:
			return err
		default:
			return c.closedError()
		}
	case err := <-request.result:
		return err
	}
}

func (c *RuntimeWebSocket) abandonPending(messageID string) {
	c.pendingMu.Lock()
	delete(c.pending, messageID)
	c.abandoned[messageID] = time.Now().Add(runtimeWSLateReplyTTL)
	c.pruneAbandonedLocked(time.Now())
	c.pendingMu.Unlock()
}

func (c *RuntimeWebSocket) pruneAbandonedLocked(now time.Time) {
	for messageID, expiresAt := range c.abandoned {
		if !expiresAt.After(now) {
			delete(c.abandoned, messageID)
		}
	}
	for len(c.abandoned) > runtimeWSLateReplyLimit {
		for messageID := range c.abandoned {
			delete(c.abandoned, messageID)
			break
		}
	}
}

func (c *RuntimeWebSocket) finish(err error) {
	c.finishOnce.Do(func() {
		if err == nil {
			err = errors.New("openlinker: runtime WebSocket closed")
		}
		c.errMu.Lock()
		c.err = err
		c.errMu.Unlock()
		c.cancel()
		_ = c.conn.Close()
		c.pendingMu.Lock()
		for messageID, pending := range c.pending {
			delete(c.pending, messageID)
			select {
			case pending.replies <- runtimeWSReply{err: err}:
			default:
			}
		}
		c.pendingMu.Unlock()
		close(c.done)
	})
}

func (c *RuntimeWebSocket) closeProtocol(cause error) {
	_ = c.writeControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(RuntimeWSCloseProtocolError, "VALIDATION_FAILED"),
	)
	c.finish(cause)
	_ = c.conn.Close()
}

func (c *RuntimeWebSocket) closedError() error {
	err := c.Err()
	if err == nil {
		return errors.New("openlinker: runtime WebSocket closed")
	}
	return err
}

func runtimeWSReadError(err error) error {
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return fmt.Errorf("openlinker: runtime WebSocket closed with code %d (%s): %w", closeErr.Code, closeErr.Text, err)
	}
	return fmt.Errorf("openlinker: read runtime WebSocket: %w", err)
}

func newRuntimeWSEnvelope(
	messageType RuntimeMessageType,
	replyTo string,
	payload any,
) (RuntimeEnvelope, []byte, error) {
	messageID, err := newRuntimeMessageID()
	if err != nil {
		return RuntimeEnvelope{}, nil, err
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return RuntimeEnvelope{}, nil, fmt.Errorf("openlinker: encode runtime WebSocket payload: %w", err)
	}
	envelope := RuntimeEnvelope{
		RuntimeEnvelopeFields: RuntimeEnvelopeFields{
			ProtocolVersion:   RuntimeProtocolVersion,
			RuntimeContractID: RuntimeContractID,
			MessageID:         messageID,
			ReplyToMessageID:  replyTo,
			Type:              messageType,
			SentAt:            time.Now().UTC(),
		},
		Payload: rawPayload,
	}
	if err = validateRuntimeWSEnvelope(envelope); err != nil {
		return RuntimeEnvelope{}, nil, err
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return RuntimeEnvelope{}, nil, fmt.Errorf("openlinker: encode runtime WebSocket envelope: %w", err)
	}
	if int64(len(raw)) > RuntimeMaxMessageBytes {
		return RuntimeEnvelope{}, nil, errors.New("openlinker: runtime WebSocket message exceeds 4 MiB")
	}
	return envelope, raw, nil
}

func decodeRuntimeWSEnvelope(raw []byte) (RuntimeEnvelope, error) {
	if len(raw) == 0 || int64(len(raw)) > RuntimeMaxMessageBytes {
		return RuntimeEnvelope{}, errors.New("openlinker: runtime WebSocket message is empty or too large")
	}
	var envelope RuntimeEnvelope
	if err := decodeRuntimeResponse(bytes.NewReader(raw), &envelope); err != nil {
		return RuntimeEnvelope{}, fmt.Errorf("openlinker: decode runtime WebSocket envelope: %w", err)
	}
	if err := validateRuntimeWSEnvelope(envelope); err != nil {
		return RuntimeEnvelope{}, err
	}
	return envelope, nil
}

func decodeRuntimeWSPayload[P any](envelope RuntimeEnvelope, expected RuntimeMessageType) (P, error) {
	var payload P
	if envelope.Type != expected {
		return payload, errors.New("openlinker: runtime WebSocket payload type mismatch")
	}
	if err := decodeRuntimeResponse(bytes.NewReader(envelope.Payload), &payload); err != nil {
		return payload, fmt.Errorf("openlinker: decode runtime WebSocket payload: %w", err)
	}
	return payload, nil
}

func validateRuntimeWSReplyPayload(envelope RuntimeEnvelope) error {
	switch envelope.Type {
	case RuntimeReady:
		payload, err := decodeRuntimeWSPayload[RuntimeReadyPayload](envelope, RuntimeReady)
		if err != nil {
			return err
		}
		return validateRuntimeReady(payload)
	case RuntimeAssignmentConfirmed:
		payload, err := decodeRuntimeWSPayload[RuntimeAssignmentConfirmedPayload](envelope, RuntimeAssignmentConfirmed)
		if err != nil {
			return err
		}
		if err = validateRuntimeAttemptIdentity(payload.AttemptIdentity); err != nil || payload.AttemptNo < 1 || payload.LeaseExpiresAt.IsZero() {
			return errors.New("openlinker: invalid runtime assignment confirmation")
		}
		return nil
	case RuntimeAssignmentRejected:
		payload, err := decodeRuntimeWSPayload[RuntimeAssignmentRejectedPayload](envelope, RuntimeAssignmentRejected)
		if err != nil {
			return err
		}
		if err = validateRuntimeAttemptIdentity(payload.AttemptIdentity); err != nil ||
			!runtimeAssignmentRejectOutcome(payload.Outcome) || !runtimeDispatchState(payload.DispatchState) {
			return errors.New("openlinker: invalid runtime assignment rejection response")
		}
		return nil
	case RuntimeLeaseRenewed:
		payload, err := decodeRuntimeWSPayload[RuntimeLeaseRenewedPayload](envelope, RuntimeLeaseRenewed)
		if err != nil {
			return err
		}
		if err = validateRuntimeAttemptIdentity(payload.AttemptIdentity); err != nil || payload.LeaseExpiresAt.IsZero() {
			return errors.New("openlinker: invalid runtime lease response")
		}
		if payload.PendingCommand != nil {
			_, err = DecodeRuntimePendingCommand(*payload.PendingCommand)
		}
		return err
	case RuntimeRunEventAck:
		payload, err := decodeRuntimeWSPayload[RuntimeRunEventAckPayload](envelope, RuntimeRunEventAck)
		if err != nil {
			return err
		}
		if !runtimeUUID(payload.ClientEventID) || payload.ClientEventSeq < 1 || payload.Sequence < 1 {
			return errors.New("openlinker: invalid runtime event acknowledgement")
		}
		return nil
	case RuntimeRunResultAck:
		payload, err := decodeRuntimeWSPayload[RuntimeRunResultAckPayload](envelope, RuntimeRunResultAck)
		if err != nil {
			return err
		}
		return validateRuntimeResultAck(payload)
	case RuntimeResumeAccepted:
		payload, err := decodeRuntimeWSPayload[RuntimeResumeAcceptedPayload](envelope, RuntimeResumeAccepted)
		if err != nil {
			return err
		}
		return validateRuntimeResumeDecision(payload)
	case RuntimeError:
		payload, err := decodeRuntimeWSPayload[RuntimeErrorBody](envelope, RuntimeError)
		if err != nil {
			return err
		}
		return validateRuntimeErrorBody(payload)
	default:
		return fmt.Errorf("openlinker: unexpected runtime WebSocket reply %q", envelope.Type)
	}
}

func validateRuntimeWSEnvelope(envelope RuntimeEnvelope) error {
	if envelope.ProtocolVersion != RuntimeProtocolVersion || envelope.RuntimeContractID != RuntimeContractID {
		return errors.New("openlinker: runtime WebSocket contract mismatch")
	}
	if !runtimeUUID(envelope.MessageID) || envelope.SentAt.IsZero() || !runtimeWSMessageType(envelope.Type) {
		return errors.New("openlinker: invalid runtime WebSocket envelope")
	}
	if envelope.ReplyToMessageID != "" {
		if !runtimeUUID(envelope.ReplyToMessageID) || envelope.ReplyToMessageID == envelope.MessageID {
			return errors.New("openlinker: invalid runtime WebSocket reply correlation")
		}
	}
	if runtimeWSRequiresReplyTo(envelope.Type) && envelope.ReplyToMessageID == "" {
		return errors.New("openlinker: runtime WebSocket reply_to_message_id is required")
	}
	var object map[string]json.RawMessage
	if len(envelope.Payload) == 0 || json.Unmarshal(envelope.Payload, &object) != nil || object == nil {
		return errors.New("openlinker: runtime WebSocket payload must be an object")
	}
	return nil
}

func runtimeWSMessageType(value RuntimeMessageType) bool {
	switch value {
	case RuntimeHello, RuntimeReady, RuntimeRunAssigned, RuntimeAssignmentAck,
		RuntimeAssignmentConfirmed, RuntimeAssignmentReject, RuntimeAssignmentRejected,
		RuntimeLeaseRenew, RuntimeLeaseRenewed, RuntimeRunEvent, RuntimeRunEventAck,
		RuntimeRunResult, RuntimeRunResultAck, RuntimeRunCancel, RuntimeRunCancelAck,
		RuntimeResume, RuntimeResumeAccepted, RuntimeLeaseRevoked, RuntimeDrain,
		RuntimeError:
		return true
	default:
		return false
	}
}

func runtimeWSRequiresReplyTo(value RuntimeMessageType) bool {
	switch value {
	case RuntimeReady, RuntimeAssignmentAck, RuntimeAssignmentConfirmed,
		RuntimeAssignmentReject, RuntimeAssignmentRejected, RuntimeLeaseRenewed,
		RuntimeRunEventAck, RuntimeRunResultAck, RuntimeRunCancelAck,
		RuntimeResumeAccepted, RuntimeError:
		return true
	default:
		return false
	}
}

func newRuntimeMessageID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("openlinker: generate runtime message ID: %w", err)
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}

func runtimeAttemptKey(identity RuntimeAttemptIdentity) string {
	return strings.Join([]string{
		identity.RunID, identity.AttemptID, identity.LeaseID,
		fmt.Sprint(identity.FencingToken), identity.NodeID, identity.AgentID,
		identity.WorkerID, identity.RuntimeSessionID,
	}, "\x00")
}

func runtimeCancellationKey(payload RuntimeRunCancelPayload) string {
	return payload.CancellationID + "\x00" + runtimeAttemptKey(payload.AttemptIdentity)
}
