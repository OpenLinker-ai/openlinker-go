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
	RuntimeV2WSCloseAuthenticationFailed   = 4401
	RuntimeV2WSCloseClientUpgradeRequired  = 4406
	RuntimeV2WSCloseSessionConflict        = 4409
	RuntimeV2WSCloseRequiredFeatureMissing = 4412
	RuntimeV2WSCloseProtocolError          = 1002
	RuntimeV2WSCloseInternalError          = 1011

	runtimeV2WSWriteWait      = 10 * time.Second
	runtimeV2WSReadWait       = 75 * time.Second
	runtimeV2WSPingInterval   = 30 * time.Second
	runtimeV2WSHandshakeWait  = 10 * time.Second
	runtimeV2WSWriteQueueSize = 64
	runtimeV2WSPushQueueSize  = RuntimeV2MaxNodeCapacity
	runtimeV2WSLateReplyTTL   = 2 * time.Minute
	runtimeV2WSLateReplyLimit = 2048
)

// RuntimeV2WebSocketAssignment preserves the server message ID that an
// assignment ACK or rejection must correlate to. Payload alone is not enough
// to prove that the response belongs to a concrete offer.
type RuntimeV2WebSocketAssignment struct {
	MessageID string
	Payload   RuntimeV2RunAssignedPayload
}

// RuntimeV2WebSocketCommand is a strictly decoded server push. MessageID is
// required when acknowledging a cancellation; drain and lease revocation are
// one-way commands.
type RuntimeV2WebSocketCommand struct {
	MessageID string
	Command   RuntimeV2DecodedPendingCommand
}

// RuntimeV2WebSocket is one attached Runtime v2 session. It has exactly one
// socket writer, correlates every business response by reply_to_message_id,
// and exposes only typed server pushes.
type RuntimeV2WebSocket struct {
	runtime *Runtime
	conn    *websocket.Conn
	hello   RuntimeV2HelloPayload
	ready   RuntimeV2ReadyPayload

	ctx    context.Context
	cancel context.CancelFunc

	writes chan runtimeV2WSWrite
	done   chan struct{}

	assignments chan RuntimeV2WebSocketAssignment
	commands    chan RuntimeV2WebSocketCommand

	finishOnce sync.Once
	errMu      sync.RWMutex
	err        error

	pendingMu sync.Mutex
	pending   map[string]*runtimeV2WSPending
	abandoned map[string]time.Time

	correlationMu sync.RWMutex
	offers        map[string]string
	cancellations map[string]string
}

type runtimeV2WSWrite struct {
	message     []byte
	controlType int
	controlData []byte
	result      chan error
}

type runtimeV2WSPending struct {
	expected  map[RuntimeV2MessageType]struct{}
	remaining int
	replies   chan runtimeV2WSReply
}

type runtimeV2WSReply struct {
	envelope RuntimeV2Envelope
	err      error
}

// DialRuntimeV2WebSocket authenticates the HTTP upgrade with the Agent Token
// and the mTLS transport configured through WithHTTPClient, sends runtime.hello,
// and returns only after a strictly validated runtime.ready reply.
func (r *Runtime) DialRuntimeV2WebSocket(
	ctx context.Context,
	hello RuntimeV2HelloPayload,
) (*RuntimeV2WebSocket, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if err := validateRuntimeV2Hello(hello); err != nil {
		return nil, err
	}
	conn, response, err := r.dialRuntimeV2WebSocket(ctx)
	if err != nil {
		return nil, runtimeV2WSDialError(response, err)
	}
	connectionCtx, cancel := context.WithCancel(context.Background())
	client := &RuntimeV2WebSocket{
		runtime:       r,
		conn:          conn,
		hello:         hello,
		ctx:           connectionCtx,
		cancel:        cancel,
		writes:        make(chan runtimeV2WSWrite, runtimeV2WSWriteQueueSize),
		done:          make(chan struct{}),
		assignments:   make(chan RuntimeV2WebSocketAssignment, runtimeV2WSPushQueueSize),
		commands:      make(chan RuntimeV2WebSocketCommand, runtimeV2WSPushQueueSize),
		pending:       make(map[string]*runtimeV2WSPending),
		abandoned:     make(map[string]time.Time),
		offers:        make(map[string]string),
		cancellations: make(map[string]string),
	}
	client.configureSocket()
	go client.writeLoop()
	go client.readLoop()

	envelope, err := client.requestOne(ctx, RuntimeV2Hello, "", hello, RuntimeV2Ready)
	if err != nil {
		client.finish(err)
		_ = conn.Close()
		return nil, err
	}
	ready, err := decodeRuntimeV2WSPayload[RuntimeV2ReadyPayload](envelope, RuntimeV2Ready)
	if err == nil {
		err = validateRuntimeV2Ready(ready)
	}
	if err != nil {
		client.closeProtocol(err)
		return nil, err
	}
	client.ready = ready
	return client, nil
}

// ProbeRuntimeV2WebSocket verifies that the authenticated WebSocket upgrade is
// reachable without attaching a durable session. It is intended for an auto
// transport state machine that is currently serving through HTTP long-poll.
func (r *Runtime) ProbeRuntimeV2WebSocket(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	conn, response, err := r.dialRuntimeV2WebSocket(ctx)
	if err != nil {
		return runtimeV2WSDialError(response, err)
	}
	deadline := time.Now().Add(runtimeV2WSWriteWait)
	_ = conn.WriteControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(websocket.CloseNormalClosure, "probe_complete"),
		deadline,
	)
	return conn.Close()
}

func (r *Runtime) dialRuntimeV2WebSocket(ctx context.Context) (*websocket.Conn, *http.Response, error) {
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
		HandshakeTimeout:  runtimeV2WSHandshakeWait,
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

func runtimeV2WSDialError(response *http.Response, cause error) error {
	if response == nil {
		return fmt.Errorf("openlinker: dial runtime v2 WebSocket: %w", cause)
	}
	defer response.Body.Close()
	if response.StatusCode >= 200 && response.StatusCode < 300 {
		return fmt.Errorf("openlinker: upgrade runtime v2 WebSocket: %w", cause)
	}
	parsed := parseRuntimeV2Error(response)
	if parsed != nil {
		return parsed
	}
	return fmt.Errorf("openlinker: upgrade runtime v2 WebSocket: %w", cause)
}

func (c *RuntimeV2WebSocket) configureSocket() {
	c.conn.SetReadLimit(RuntimeV2MaxMessageBytes)
	_ = c.conn.SetReadDeadline(time.Now().Add(runtimeV2WSReadWait))
	c.conn.SetPongHandler(func(string) error {
		return c.conn.SetReadDeadline(time.Now().Add(runtimeV2WSReadWait))
	})
	c.conn.SetPingHandler(func(data string) error {
		return c.writeControl(websocket.PongMessage, []byte(data))
	})
}

// Ready returns the immutable handshake result for this attached session.
func (c *RuntimeV2WebSocket) Ready() RuntimeV2ReadyPayload {
	if c == nil {
		return RuntimeV2ReadyPayload{}
	}
	ready := c.ready
	ready.Features = append([]string(nil), c.ready.Features...)
	return ready
}

func (c *RuntimeV2WebSocket) Done() <-chan struct{} {
	if c == nil {
		closed := make(chan struct{})
		close(closed)
		return closed
	}
	return c.done
}

func (c *RuntimeV2WebSocket) Err() error {
	if c == nil {
		return errors.New("openlinker: runtime WebSocket is nil")
	}
	c.errMu.RLock()
	defer c.errMu.RUnlock()
	return c.err
}

func (c *RuntimeV2WebSocket) Assignments() <-chan RuntimeV2WebSocketAssignment {
	return c.assignments
}

func (c *RuntimeV2WebSocket) Commands() <-chan RuntimeV2WebSocketCommand {
	return c.commands
}

// Close performs a normal WebSocket close. Core detaches the session and
// releases only an unacknowledged offer; executing attempts remain recoverable.
func (c *RuntimeV2WebSocket) Close() error {
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

func (c *RuntimeV2WebSocket) readLoop() {
	for {
		messageType, reader, err := c.conn.NextReader()
		if err != nil {
			c.finish(runtimeV2WSReadError(err))
			return
		}
		if messageType != websocket.TextMessage {
			c.closeProtocol(errors.New("openlinker: runtime v2 WebSocket accepts text messages only"))
			return
		}
		raw, err := io.ReadAll(io.LimitReader(reader, RuntimeV2MaxMessageBytes+1))
		if err != nil || int64(len(raw)) > RuntimeV2MaxMessageBytes {
			if err == nil {
				err = errors.New("openlinker: runtime v2 WebSocket message exceeds 4 MiB")
			}
			c.closeProtocol(err)
			return
		}
		envelope, err := decodeRuntimeV2WSEnvelope(raw)
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

func (c *RuntimeV2WebSocket) routeEnvelope(envelope RuntimeV2Envelope) error {
	if envelope.ReplyToMessageID != "" {
		return c.routeReply(envelope)
	}
	switch envelope.Type {
	case RuntimeV2RunAssigned:
		payload, err := decodeRuntimeV2WSPayload[RuntimeV2RunAssignedPayload](envelope, RuntimeV2RunAssigned)
		if err == nil {
			err = validateRuntimeV2Assignment(payload)
		}
		if err != nil {
			return err
		}
		key := runtimeV2AttemptKey(payload.AttemptIdentity)
		c.correlationMu.Lock()
		c.offers[key] = envelope.MessageID
		c.correlationMu.Unlock()
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case c.assignments <- RuntimeV2WebSocketAssignment{MessageID: envelope.MessageID, Payload: payload}:
			return nil
		}
	case RuntimeV2RunCancel, RuntimeV2Drain, RuntimeV2LeaseRevoked:
		command := RuntimeV2PendingCommand{Type: envelope.Type, Payload: append(json.RawMessage(nil), envelope.Payload...)}
		decoded, err := DecodeRuntimeV2PendingCommand(command)
		if err != nil {
			return err
		}
		if decoded.Cancel != nil {
			c.correlationMu.Lock()
			c.cancellations[runtimeV2CancellationKey(*decoded.Cancel)] = envelope.MessageID
			c.correlationMu.Unlock()
		}
		select {
		case <-c.ctx.Done():
			return c.ctx.Err()
		case c.commands <- RuntimeV2WebSocketCommand{MessageID: envelope.MessageID, Command: decoded}:
			return nil
		}
	default:
		return fmt.Errorf("openlinker: unexpected runtime v2 WebSocket push %q", envelope.Type)
	}
}

func (c *RuntimeV2WebSocket) routeReply(envelope RuntimeV2Envelope) error {
	if err := validateRuntimeV2WSReplyPayload(envelope); err != nil {
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
		return errors.New("openlinker: runtime v2 WebSocket reply has no pending request")
	}
	if envelope.Type != RuntimeV2Error {
		if _, ok := pending.expected[envelope.Type]; !ok {
			c.pendingMu.Unlock()
			return errors.New("openlinker: runtime v2 WebSocket reply type does not match request")
		}
	}
	pending.remaining--
	if pending.remaining == 0 || envelope.Type == RuntimeV2Error {
		delete(c.pending, envelope.ReplyToMessageID)
	}
	c.pendingMu.Unlock()

	reply := runtimeV2WSReply{envelope: envelope}
	if envelope.Type == RuntimeV2Error {
		body, err := decodeRuntimeV2WSPayload[RuntimeV2ErrorBody](envelope, RuntimeV2Error)
		if err == nil {
			err = validateRuntimeV2ErrorBody(body)
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

func (c *RuntimeV2WebSocket) writeLoop() {
	ticker := time.NewTicker(runtimeV2WSPingInterval)
	defer ticker.Stop()
	for {
		select {
		case <-c.ctx.Done():
			return
		case request := <-c.writes:
			_ = c.conn.SetWriteDeadline(time.Now().Add(runtimeV2WSWriteWait))
			var err error
			switch {
			case request.message != nil:
				err = c.conn.WriteMessage(websocket.TextMessage, request.message)
			case request.controlType != 0:
				err = c.conn.WriteControl(
					request.controlType,
					request.controlData,
					time.Now().Add(runtimeV2WSWriteWait),
				)
			default:
				err = errors.New("openlinker: empty runtime v2 WebSocket write")
			}
			request.result <- err
			if err != nil || request.controlType == websocket.CloseMessage {
				if err != nil {
					c.finish(err)
				}
				return
			}
		case <-ticker.C:
			_ = c.conn.SetWriteDeadline(time.Now().Add(runtimeV2WSWriteWait))
			if err := c.conn.WriteControl(websocket.PingMessage, nil, time.Now().Add(runtimeV2WSWriteWait)); err != nil {
				c.finish(err)
				return
			}
		}
	}
}

func (c *RuntimeV2WebSocket) requestOne(
	ctx context.Context,
	messageType RuntimeV2MessageType,
	replyTo string,
	payload any,
	expected RuntimeV2MessageType,
) (RuntimeV2Envelope, error) {
	replies, _, err := c.request(ctx, messageType, replyTo, payload, []RuntimeV2MessageType{expected}, 1)
	if err != nil {
		return RuntimeV2Envelope{}, err
	}
	return replies[0], nil
}

func (c *RuntimeV2WebSocket) request(
	ctx context.Context,
	messageType RuntimeV2MessageType,
	replyTo string,
	payload any,
	expected []RuntimeV2MessageType,
	replyCount int,
) ([]RuntimeV2Envelope, string, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if replyCount < 1 {
		return nil, "", errors.New("openlinker: runtime v2 WebSocket request needs a reply")
	}
	envelope, raw, err := newRuntimeV2WSEnvelope(messageType, replyTo, payload)
	if err != nil {
		return nil, "", err
	}
	pending := &runtimeV2WSPending{
		expected:  make(map[RuntimeV2MessageType]struct{}, len(expected)),
		remaining: replyCount,
		replies:   make(chan runtimeV2WSReply, replyCount),
	}
	for _, allowed := range expected {
		pending.expected[allowed] = struct{}{}
	}
	c.pendingMu.Lock()
	if _, duplicate := c.pending[envelope.MessageID]; duplicate {
		c.pendingMu.Unlock()
		return nil, "", errors.New("openlinker: duplicate runtime v2 WebSocket message ID")
	}
	c.pending[envelope.MessageID] = pending
	c.pendingMu.Unlock()
	if err := c.writeMessage(raw); err != nil {
		c.abandonPending(envelope.MessageID)
		return nil, envelope.MessageID, err
	}

	replies := make([]RuntimeV2Envelope, 0, replyCount)
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

func (c *RuntimeV2WebSocket) writeMessage(raw []byte) error {
	request := runtimeV2WSWrite{message: append([]byte(nil), raw...), result: make(chan error, 1)}
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

func (c *RuntimeV2WebSocket) writeControl(messageType int, data []byte) error {
	request := runtimeV2WSWrite{
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

func (c *RuntimeV2WebSocket) abandonPending(messageID string) {
	c.pendingMu.Lock()
	delete(c.pending, messageID)
	c.abandoned[messageID] = time.Now().Add(runtimeV2WSLateReplyTTL)
	c.pruneAbandonedLocked(time.Now())
	c.pendingMu.Unlock()
}

func (c *RuntimeV2WebSocket) pruneAbandonedLocked(now time.Time) {
	for messageID, expiresAt := range c.abandoned {
		if !expiresAt.After(now) {
			delete(c.abandoned, messageID)
		}
	}
	for len(c.abandoned) > runtimeV2WSLateReplyLimit {
		for messageID := range c.abandoned {
			delete(c.abandoned, messageID)
			break
		}
	}
}

func (c *RuntimeV2WebSocket) finish(err error) {
	c.finishOnce.Do(func() {
		if err == nil {
			err = errors.New("openlinker: runtime v2 WebSocket closed")
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
			case pending.replies <- runtimeV2WSReply{err: err}:
			default:
			}
		}
		c.pendingMu.Unlock()
		close(c.done)
	})
}

func (c *RuntimeV2WebSocket) closeProtocol(cause error) {
	_ = c.writeControl(
		websocket.CloseMessage,
		websocket.FormatCloseMessage(RuntimeV2WSCloseProtocolError, "VALIDATION_FAILED"),
	)
	c.finish(cause)
	_ = c.conn.Close()
}

func (c *RuntimeV2WebSocket) closedError() error {
	err := c.Err()
	if err == nil {
		return errors.New("openlinker: runtime v2 WebSocket closed")
	}
	return err
}

func runtimeV2WSReadError(err error) error {
	var closeErr *websocket.CloseError
	if errors.As(err, &closeErr) {
		return fmt.Errorf("openlinker: runtime v2 WebSocket closed with code %d (%s): %w", closeErr.Code, closeErr.Text, err)
	}
	return fmt.Errorf("openlinker: read runtime v2 WebSocket: %w", err)
}

func newRuntimeV2WSEnvelope(
	messageType RuntimeV2MessageType,
	replyTo string,
	payload any,
) (RuntimeV2Envelope, []byte, error) {
	messageID, err := newRuntimeV2MessageID()
	if err != nil {
		return RuntimeV2Envelope{}, nil, err
	}
	rawPayload, err := json.Marshal(payload)
	if err != nil {
		return RuntimeV2Envelope{}, nil, fmt.Errorf("openlinker: encode runtime v2 WebSocket payload: %w", err)
	}
	envelope := RuntimeV2Envelope{
		RuntimeV2EnvelopeFields: RuntimeV2EnvelopeFields{
			ProtocolVersion:   RuntimeProtocolVersion,
			RuntimeContractID: RuntimeContractID,
			MessageID:         messageID,
			ReplyToMessageID:  replyTo,
			Type:              messageType,
			SentAt:            time.Now().UTC(),
		},
		Payload: rawPayload,
	}
	if err = validateRuntimeV2WSEnvelope(envelope); err != nil {
		return RuntimeV2Envelope{}, nil, err
	}
	raw, err := json.Marshal(envelope)
	if err != nil {
		return RuntimeV2Envelope{}, nil, fmt.Errorf("openlinker: encode runtime v2 WebSocket envelope: %w", err)
	}
	if int64(len(raw)) > RuntimeV2MaxMessageBytes {
		return RuntimeV2Envelope{}, nil, errors.New("openlinker: runtime v2 WebSocket message exceeds 4 MiB")
	}
	return envelope, raw, nil
}

func decodeRuntimeV2WSEnvelope(raw []byte) (RuntimeV2Envelope, error) {
	if len(raw) == 0 || int64(len(raw)) > RuntimeV2MaxMessageBytes {
		return RuntimeV2Envelope{}, errors.New("openlinker: runtime v2 WebSocket message is empty or too large")
	}
	var envelope RuntimeV2Envelope
	if err := decodeRuntimeV2Response(bytes.NewReader(raw), &envelope); err != nil {
		return RuntimeV2Envelope{}, fmt.Errorf("openlinker: decode runtime v2 WebSocket envelope: %w", err)
	}
	if err := validateRuntimeV2WSEnvelope(envelope); err != nil {
		return RuntimeV2Envelope{}, err
	}
	return envelope, nil
}

func decodeRuntimeV2WSPayload[P any](envelope RuntimeV2Envelope, expected RuntimeV2MessageType) (P, error) {
	var payload P
	if envelope.Type != expected {
		return payload, errors.New("openlinker: runtime v2 WebSocket payload type mismatch")
	}
	if err := decodeRuntimeV2Response(bytes.NewReader(envelope.Payload), &payload); err != nil {
		return payload, fmt.Errorf("openlinker: decode runtime v2 WebSocket payload: %w", err)
	}
	return payload, nil
}

func validateRuntimeV2WSReplyPayload(envelope RuntimeV2Envelope) error {
	switch envelope.Type {
	case RuntimeV2Ready:
		payload, err := decodeRuntimeV2WSPayload[RuntimeV2ReadyPayload](envelope, RuntimeV2Ready)
		if err != nil {
			return err
		}
		return validateRuntimeV2Ready(payload)
	case RuntimeV2AssignmentConfirmed:
		payload, err := decodeRuntimeV2WSPayload[RuntimeV2AssignmentConfirmedPayload](envelope, RuntimeV2AssignmentConfirmed)
		if err != nil {
			return err
		}
		if err = validateRuntimeV2AttemptIdentity(payload.AttemptIdentity); err != nil || payload.AttemptNo < 1 || payload.LeaseExpiresAt.IsZero() {
			return errors.New("openlinker: invalid runtime v2 assignment confirmation")
		}
		return nil
	case RuntimeV2AssignmentRejected:
		payload, err := decodeRuntimeV2WSPayload[RuntimeV2AssignmentRejectedPayload](envelope, RuntimeV2AssignmentRejected)
		if err != nil {
			return err
		}
		if err = validateRuntimeV2AttemptIdentity(payload.AttemptIdentity); err != nil ||
			!runtimeV2AssignmentRejectOutcome(payload.Outcome) || !runtimeV2DispatchState(payload.DispatchState) {
			return errors.New("openlinker: invalid runtime v2 assignment rejection response")
		}
		return nil
	case RuntimeV2LeaseRenewed:
		payload, err := decodeRuntimeV2WSPayload[RuntimeV2LeaseRenewedPayload](envelope, RuntimeV2LeaseRenewed)
		if err != nil {
			return err
		}
		if err = validateRuntimeV2AttemptIdentity(payload.AttemptIdentity); err != nil || payload.LeaseExpiresAt.IsZero() {
			return errors.New("openlinker: invalid runtime v2 lease response")
		}
		if payload.PendingCommand != nil {
			_, err = DecodeRuntimeV2PendingCommand(*payload.PendingCommand)
		}
		return err
	case RuntimeV2RunEventAck:
		payload, err := decodeRuntimeV2WSPayload[RuntimeV2RunEventAckPayload](envelope, RuntimeV2RunEventAck)
		if err != nil {
			return err
		}
		if !runtimeV2UUID(payload.ClientEventID) || payload.ClientEventSeq < 1 || payload.Sequence < 1 {
			return errors.New("openlinker: invalid runtime v2 event acknowledgement")
		}
		return nil
	case RuntimeV2RunResultAck:
		payload, err := decodeRuntimeV2WSPayload[RuntimeV2RunResultAckPayload](envelope, RuntimeV2RunResultAck)
		if err != nil {
			return err
		}
		return validateRuntimeV2ResultAck(payload)
	case RuntimeV2ResumeAccepted:
		payload, err := decodeRuntimeV2WSPayload[RuntimeV2ResumeAcceptedPayload](envelope, RuntimeV2ResumeAccepted)
		if err != nil {
			return err
		}
		return validateRuntimeV2ResumeDecision(payload)
	case RuntimeV2Error:
		payload, err := decodeRuntimeV2WSPayload[RuntimeV2ErrorBody](envelope, RuntimeV2Error)
		if err != nil {
			return err
		}
		return validateRuntimeV2ErrorBody(payload)
	default:
		return fmt.Errorf("openlinker: unexpected runtime v2 WebSocket reply %q", envelope.Type)
	}
}

func validateRuntimeV2WSEnvelope(envelope RuntimeV2Envelope) error {
	if envelope.ProtocolVersion != RuntimeProtocolVersion || envelope.RuntimeContractID != RuntimeContractID {
		return errors.New("openlinker: runtime v2 WebSocket contract mismatch")
	}
	if !runtimeV2UUID(envelope.MessageID) || envelope.SentAt.IsZero() || !runtimeV2WSMessageType(envelope.Type) {
		return errors.New("openlinker: invalid runtime v2 WebSocket envelope")
	}
	if envelope.ReplyToMessageID != "" {
		if !runtimeV2UUID(envelope.ReplyToMessageID) || envelope.ReplyToMessageID == envelope.MessageID {
			return errors.New("openlinker: invalid runtime v2 WebSocket reply correlation")
		}
	}
	if runtimeV2WSRequiresReplyTo(envelope.Type) && envelope.ReplyToMessageID == "" {
		return errors.New("openlinker: runtime v2 WebSocket reply_to_message_id is required")
	}
	var object map[string]json.RawMessage
	if len(envelope.Payload) == 0 || json.Unmarshal(envelope.Payload, &object) != nil || object == nil {
		return errors.New("openlinker: runtime v2 WebSocket payload must be an object")
	}
	return nil
}

func runtimeV2WSMessageType(value RuntimeV2MessageType) bool {
	switch value {
	case RuntimeV2Hello, RuntimeV2Ready, RuntimeV2RunAssigned, RuntimeV2AssignmentAck,
		RuntimeV2AssignmentConfirmed, RuntimeV2AssignmentReject, RuntimeV2AssignmentRejected,
		RuntimeV2LeaseRenew, RuntimeV2LeaseRenewed, RuntimeV2RunEvent, RuntimeV2RunEventAck,
		RuntimeV2RunResult, RuntimeV2RunResultAck, RuntimeV2RunCancel, RuntimeV2RunCancelAck,
		RuntimeV2Resume, RuntimeV2ResumeAccepted, RuntimeV2LeaseRevoked, RuntimeV2Drain,
		RuntimeV2Error:
		return true
	default:
		return false
	}
}

func runtimeV2WSRequiresReplyTo(value RuntimeV2MessageType) bool {
	switch value {
	case RuntimeV2Ready, RuntimeV2AssignmentAck, RuntimeV2AssignmentConfirmed,
		RuntimeV2AssignmentReject, RuntimeV2AssignmentRejected, RuntimeV2LeaseRenewed,
		RuntimeV2RunEventAck, RuntimeV2RunResultAck, RuntimeV2RunCancelAck,
		RuntimeV2ResumeAccepted, RuntimeV2Error:
		return true
	default:
		return false
	}
}

func newRuntimeV2MessageID() (string, error) {
	var value [16]byte
	if _, err := rand.Read(value[:]); err != nil {
		return "", fmt.Errorf("openlinker: generate runtime v2 message ID: %w", err)
	}
	value[6] = (value[6] & 0x0f) | 0x40
	value[8] = (value[8] & 0x3f) | 0x80
	return fmt.Sprintf("%08x-%04x-%04x-%04x-%012x", value[0:4], value[4:6], value[6:8], value[8:10], value[10:16]), nil
}

func runtimeV2AttemptKey(identity RuntimeV2AttemptIdentity) string {
	return strings.Join([]string{
		identity.RunID, identity.AttemptID, identity.LeaseID,
		fmt.Sprint(identity.FencingToken), identity.NodeID, identity.AgentID,
		identity.WorkerID, identity.RuntimeSessionID,
	}, "\x00")
}

func runtimeV2CancellationKey(payload RuntimeV2RunCancelPayload) string {
	return payload.CancellationID + "\x00" + runtimeV2AttemptKey(payload.AttemptIdentity)
}
