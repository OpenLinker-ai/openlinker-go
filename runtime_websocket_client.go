package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// CreateRuntimeSession makes RuntimeWebSocket usable behind the same
// protocol interface as the HTTP transport. DialRuntimeWebSocket already
// attached the session, so this method only verifies that the caller is using
// the identical durable identity.
func (c *RuntimeWebSocket) CreateRuntimeSession(
	_ context.Context,
	hello RuntimeHelloPayload,
) (*RuntimeReadyPayload, error) {
	if c == nil {
		return nil, errors.New("openlinker: runtime WebSocket is nil")
	}
	if err := validateRuntimeHello(hello); err != nil {
		return nil, err
	}
	if !runtimeHelloEqual(c.hello, hello) {
		return nil, errors.New("openlinker: runtime WebSocket is attached to another session")
	}
	ready := c.Ready()
	return &ready, nil
}

// HeartbeatRuntimeSession verifies liveness. Core owns WebSocket session
// heartbeats; ping/pong proves transport liveness and the server refreshes the
// durable session independently.
func (c *RuntimeWebSocket) HeartbeatRuntimeSession(
	ctx context.Context,
	hello RuntimeHelloPayload,
) (*RuntimeReadyPayload, error) {
	if _, err := c.CreateRuntimeSession(ctx, hello); err != nil {
		return nil, err
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.closedError()
	default:
		ready := c.Ready()
		return &ready, nil
	}
}

func (c *RuntimeWebSocket) CloseRuntimeSession(
	_ context.Context,
	request RuntimeSessionCloseRequest,
) error {
	if c == nil {
		return errors.New("openlinker: runtime WebSocket is nil")
	}
	if err := validateRuntimeSessionClose(request); err != nil {
		return err
	}
	if request.NodeID != c.hello.NodeID || request.AgentID != c.hello.AgentID ||
		request.WorkerID != c.hello.WorkerID || request.RuntimeSessionID != c.hello.RuntimeSessionID ||
		request.SessionEpoch != c.hello.SessionEpoch {
		return errors.New("openlinker: runtime WebSocket close identity mismatch")
	}
	return c.Close()
}

// ClaimRuntimeRun waits for a server-pushed offer. Core claims on behalf of
// the attached WebSocket session; this method never issues a competing HTTP
// claim.
func (c *RuntimeWebSocket) ClaimRuntimeRun(
	ctx context.Context,
	_ int,
	request RuntimeClaimRequest,
) (*RuntimeRunAssignedPayload, error) {
	if c == nil {
		return nil, errors.New("openlinker: runtime WebSocket is nil")
	}
	if !runtimeUUID(request.RuntimeSessionID) || request.RuntimeSessionID != c.hello.RuntimeSessionID ||
		!runtimeCapacity(request.Capacity, request.Inflight) {
		return nil, errors.New("openlinker: invalid runtime WebSocket claim")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.closedError()
	case assignment := <-c.assignments:
		payload := assignment.Payload
		return &payload, nil
	}
}

func (c *RuntimeWebSocket) AckRuntimeAssignment(
	ctx context.Context,
	request RuntimeAssignmentAckPayload,
) (*RuntimeAssignmentConfirmedPayload, error) {
	if err := validateRuntimeAttemptIdentity(request.AttemptIdentity); err != nil {
		return nil, err
	}
	replyTo, err := c.offerMessageID(request.AttemptIdentity)
	if err != nil {
		return nil, err
	}
	envelope, err := c.requestOne(ctx, RuntimeAssignmentAck, replyTo, request, RuntimeAssignmentConfirmed)
	if err != nil {
		return nil, err
	}
	confirmed, err := decodeRuntimeWSPayload[RuntimeAssignmentConfirmedPayload](envelope, RuntimeAssignmentConfirmed)
	if err != nil {
		return nil, err
	}
	if confirmed.AttemptIdentity != request.AttemptIdentity || confirmed.AttemptNo < 1 || confirmed.LeaseExpiresAt.IsZero() {
		return nil, errors.New("openlinker: invalid runtime assignment confirmation")
	}
	c.removeOffer(request.AttemptIdentity, replyTo)
	return &confirmed, nil
}

func (c *RuntimeWebSocket) RejectRuntimeAssignment(
	ctx context.Context,
	request RuntimeAssignmentRejectPayload,
) (*RuntimeAssignmentRejectedPayload, error) {
	if err := validateRuntimeAttemptIdentity(request.AttemptIdentity); err != nil ||
		!runtimeCapacity(request.Capacity, request.Inflight) || !runtimeRejectReason(request.ReasonCode) {
		return nil, errors.New("openlinker: invalid runtime assignment rejection")
	}
	replyTo, err := c.offerMessageID(request.AttemptIdentity)
	if err != nil {
		return nil, err
	}
	envelope, err := c.requestOne(ctx, RuntimeAssignmentReject, replyTo, request, RuntimeAssignmentRejected)
	if err != nil {
		return nil, err
	}
	rejected, err := decodeRuntimeWSPayload[RuntimeAssignmentRejectedPayload](envelope, RuntimeAssignmentRejected)
	if err != nil {
		return nil, err
	}
	if rejected.AttemptIdentity != request.AttemptIdentity ||
		!runtimeAssignmentRejectOutcome(rejected.Outcome) || !runtimeDispatchState(rejected.DispatchState) {
		return nil, errors.New("openlinker: invalid runtime assignment rejection response")
	}
	c.removeOffer(request.AttemptIdentity, replyTo)
	return &rejected, nil
}

func (c *RuntimeWebSocket) RenewRuntimeLease(
	ctx context.Context,
	request RuntimeLeaseRenewPayload,
) (*RuntimeLeaseRenewedPayload, error) {
	if err := validateRuntimeAttemptIdentity(request.AttemptIdentity); err != nil ||
		request.LastClientEventSeq < 0 || !runtimeCapacity(request.Capacity, request.Inflight) {
		return nil, errors.New("openlinker: invalid runtime lease renewal")
	}
	envelope, err := c.requestOne(ctx, RuntimeLeaseRenew, "", request, RuntimeLeaseRenewed)
	if err != nil {
		return nil, err
	}
	renewed, err := decodeRuntimeWSPayload[RuntimeLeaseRenewedPayload](envelope, RuntimeLeaseRenewed)
	if err != nil {
		return nil, err
	}
	if renewed.AttemptIdentity != request.AttemptIdentity || renewed.LeaseExpiresAt.IsZero() {
		return nil, errors.New("openlinker: invalid runtime lease response")
	}
	if renewed.PendingCommand != nil {
		if _, err = DecodeRuntimePendingCommand(*renewed.PendingCommand); err != nil {
			return nil, fmt.Errorf("openlinker: invalid runtime pending command: %w", err)
		}
	}
	return &renewed, nil
}

func (c *RuntimeWebSocket) AppendRuntimeEvent(
	ctx context.Context,
	request RuntimeRunEventPayload,
) (*RuntimeRunEventAckPayload, error) {
	if err := validateRuntimeEvent(request); err != nil {
		return nil, err
	}
	envelope, err := c.requestOne(ctx, RuntimeRunEvent, "", request, RuntimeRunEventAck)
	if err != nil {
		return nil, err
	}
	ack, err := decodeRuntimeWSPayload[RuntimeRunEventAckPayload](envelope, RuntimeRunEventAck)
	if err != nil {
		return nil, err
	}
	if ack.ClientEventID != request.ClientEventID || ack.ClientEventSeq != request.ClientEventSeq || ack.Sequence < 1 {
		return nil, errors.New("openlinker: runtime event acknowledgement mismatch")
	}
	return &ack, nil
}

func (c *RuntimeWebSocket) FinalizeRuntimeResult(
	ctx context.Context,
	request RuntimeRunResultPayload,
) (*RuntimeRunResultAckPayload, error) {
	if err := validateRuntimeResult(request); err != nil {
		return nil, err
	}
	envelope, err := c.requestOne(ctx, RuntimeRunResult, "", request, RuntimeRunResultAck)
	if err != nil {
		return nil, err
	}
	ack, err := decodeRuntimeWSPayload[RuntimeRunResultAckPayload](envelope, RuntimeRunResultAck)
	if err != nil {
		return nil, err
	}
	if ack.ResultID != request.ResultID {
		return nil, errors.New("openlinker: runtime result acknowledgement mismatch")
	}
	if err = validateRuntimeResultAck(ack); err != nil {
		return nil, err
	}
	return &ack, nil
}

func (c *RuntimeWebSocket) ResumeRuntimeRuns(
	ctx context.Context,
	request RuntimeResumePayload,
) (*RuntimeResumeResponse, error) {
	if err := validateRuntimeResume(request); err != nil {
		return nil, err
	}
	if request.RuntimeSessionID != c.hello.RuntimeSessionID {
		return nil, errors.New("openlinker: runtime resume session mismatch")
	}
	if len(request.Attempts) == 0 {
		return &RuntimeResumeResponse{Decisions: []RuntimeResumeAcceptedPayload{}}, nil
	}
	replies, _, err := c.request(
		ctx,
		RuntimeResume,
		"",
		request,
		[]RuntimeMessageType{RuntimeResumeAccepted},
		len(request.Attempts),
	)
	if err != nil {
		return nil, err
	}
	response := &RuntimeResumeResponse{Decisions: make([]RuntimeResumeAcceptedPayload, len(replies))}
	for index, envelope := range replies {
		decision, decodeErr := decodeRuntimeWSPayload[RuntimeResumeAcceptedPayload](envelope, RuntimeResumeAccepted)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if decision.AttemptIdentity != request.Attempts[index].AttemptIdentity {
			return nil, errors.New("openlinker: runtime resume response order mismatch")
		}
		if decodeErr = validateRuntimeResumeDecision(decision); decodeErr != nil {
			return nil, decodeErr
		}
		response.Decisions[index] = decision
	}
	return response, nil
}

func (c *RuntimeWebSocket) PollRuntimeCommands(
	ctx context.Context,
	runtimeSessionID string,
	_ int,
) (*RuntimeCommandsResponse, error) {
	if !runtimeUUID(runtimeSessionID) || runtimeSessionID != c.hello.RuntimeSessionID {
		return nil, errors.New("openlinker: invalid runtime WebSocket command poll")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	case <-c.done:
		return nil, c.closedError()
	case pushed := <-c.commands:
		payload, err := commandPayload(pushed.Command)
		if err != nil {
			return nil, err
		}
		return &RuntimeCommandsResponse{
			Commands:     []RuntimePendingCommand{{Type: pushed.Command.Type, Payload: payload}},
			DatabaseTime: time.Now().UTC(),
		}, nil
	}
}

func (c *RuntimeWebSocket) AckRuntimeCancel(
	ctx context.Context,
	request RuntimeRunCancelAckPayload,
) (*RuntimeRunCancellationState, error) {
	if err := validateRuntimeCancelAck(request); err != nil {
		return nil, err
	}
	replyTo, err := c.cancellationMessageID(request)
	if err != nil {
		return nil, err
	}
	_, raw, err := newRuntimeWSEnvelope(RuntimeRunCancelAck, replyTo, request)
	if err != nil {
		return nil, err
	}
	if err = c.writeMessage(raw); err != nil {
		return nil, err
	}
	return &RuntimeRunCancellationState{
		CancellationID: request.CancellationID,
		CancelState:    request.CancelState,
		UpdatedAt:      time.Now().UTC(),
		ErrorCode:      request.ErrorCode,
	}, nil
}

func (c *RuntimeWebSocket) CallRuntimeAgent(
	ctx context.Context,
	authorization RuntimeCallAgentAuthorization,
	request RuntimeCallAgentRequest,
) (*RuntimeRunSummary, error) {
	if c == nil || c.runtime == nil {
		return nil, errors.New("openlinker: runtime WebSocket is nil")
	}
	return c.runtime.CallRuntimeAgent(ctx, authorization, request)
}

func (c *RuntimeWebSocket) offerMessageID(identity RuntimeAttemptIdentity) (string, error) {
	c.correlationMu.RLock()
	messageID := c.offers[runtimeAttemptKey(identity)]
	c.correlationMu.RUnlock()
	if messageID == "" {
		return "", errors.New("openlinker: runtime assignment has no WebSocket offer correlation")
	}
	return messageID, nil
}

func (c *RuntimeWebSocket) removeOffer(identity RuntimeAttemptIdentity, messageID string) {
	key := runtimeAttemptKey(identity)
	c.correlationMu.Lock()
	if c.offers[key] == messageID {
		delete(c.offers, key)
	}
	c.correlationMu.Unlock()
}

func (c *RuntimeWebSocket) cancellationMessageID(request RuntimeRunCancelAckPayload) (string, error) {
	key := request.CancellationID + "\x00" + runtimeAttemptKey(request.AttemptIdentity)
	c.correlationMu.RLock()
	messageID := c.cancellations[key]
	c.correlationMu.RUnlock()
	if messageID == "" {
		return "", errors.New("openlinker: runtime cancellation has no WebSocket command correlation")
	}
	return messageID, nil
}

func runtimeHelloEqual(left, right RuntimeHelloPayload) bool {
	if left.NodeID != right.NodeID || left.AgentID != right.AgentID || left.WorkerID != right.WorkerID ||
		left.RuntimeSessionID != right.RuntimeSessionID || left.SessionEpoch != right.SessionEpoch ||
		left.NodeVersion != right.NodeVersion || left.Capacity != right.Capacity ||
		left.ContractDigest != right.ContractDigest || len(left.Features) != len(right.Features) {
		return false
	}
	for index := range left.Features {
		if left.Features[index] != right.Features[index] {
			return false
		}
	}
	return true
}

func commandPayload(command RuntimeDecodedPendingCommand) ([]byte, error) {
	switch command.Type {
	case RuntimeRunCancel:
		if command.Cancel != nil {
			return json.Marshal(command.Cancel)
		}
	case RuntimeDrain:
		if command.Drain != nil {
			return json.Marshal(command.Drain)
		}
	case RuntimeLeaseRevoked:
		if command.Revoke != nil {
			return json.Marshal(command.Revoke)
		}
	}
	return nil, errors.New("openlinker: invalid runtime WebSocket command")
}
