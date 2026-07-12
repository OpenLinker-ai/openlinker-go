package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

// CreateRuntimeV2Session makes RuntimeV2WebSocket usable behind the same v2
// protocol interface as the HTTP transport. DialRuntimeV2WebSocket already
// attached the session, so this method only verifies that the caller is using
// the identical durable identity.
func (c *RuntimeV2WebSocket) CreateRuntimeV2Session(
	_ context.Context,
	hello RuntimeV2HelloPayload,
) (*RuntimeV2ReadyPayload, error) {
	if c == nil {
		return nil, errors.New("openlinker: runtime WebSocket is nil")
	}
	if err := validateRuntimeV2Hello(hello); err != nil {
		return nil, err
	}
	if !runtimeV2HelloEqual(c.hello, hello) {
		return nil, errors.New("openlinker: runtime WebSocket is attached to another session")
	}
	ready := c.Ready()
	return &ready, nil
}

// HeartbeatRuntimeV2Session verifies liveness. Core owns WebSocket session
// heartbeats; ping/pong proves transport liveness and the server refreshes the
// durable session independently.
func (c *RuntimeV2WebSocket) HeartbeatRuntimeV2Session(
	ctx context.Context,
	hello RuntimeV2HelloPayload,
) (*RuntimeV2ReadyPayload, error) {
	if _, err := c.CreateRuntimeV2Session(ctx, hello); err != nil {
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

func (c *RuntimeV2WebSocket) CloseRuntimeV2Session(
	_ context.Context,
	request RuntimeV2SessionCloseRequest,
) error {
	if c == nil {
		return errors.New("openlinker: runtime WebSocket is nil")
	}
	if err := validateRuntimeV2SessionClose(request); err != nil {
		return err
	}
	if request.NodeID != c.hello.NodeID || request.AgentID != c.hello.AgentID ||
		request.WorkerID != c.hello.WorkerID || request.RuntimeSessionID != c.hello.RuntimeSessionID ||
		request.SessionEpoch != c.hello.SessionEpoch {
		return errors.New("openlinker: runtime WebSocket close identity mismatch")
	}
	return c.Close()
}

// ClaimRuntimeV2Run waits for a server-pushed offer. Core claims on behalf of
// the attached WebSocket session; this method never issues a competing HTTP
// claim.
func (c *RuntimeV2WebSocket) ClaimRuntimeV2Run(
	ctx context.Context,
	_ int,
	request RuntimeV2ClaimRequest,
) (*RuntimeV2RunAssignedPayload, error) {
	if c == nil {
		return nil, errors.New("openlinker: runtime WebSocket is nil")
	}
	if !runtimeV2UUID(request.RuntimeSessionID) || request.RuntimeSessionID != c.hello.RuntimeSessionID ||
		!runtimeV2Capacity(request.Capacity, request.Inflight) {
		return nil, errors.New("openlinker: invalid runtime v2 WebSocket claim")
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

func (c *RuntimeV2WebSocket) AckRuntimeV2Assignment(
	ctx context.Context,
	request RuntimeV2AssignmentAckPayload,
) (*RuntimeV2AssignmentConfirmedPayload, error) {
	if err := validateRuntimeV2AttemptIdentity(request.AttemptIdentity); err != nil {
		return nil, err
	}
	replyTo, err := c.offerMessageID(request.AttemptIdentity)
	if err != nil {
		return nil, err
	}
	envelope, err := c.requestOne(ctx, RuntimeV2AssignmentAck, replyTo, request, RuntimeV2AssignmentConfirmed)
	if err != nil {
		return nil, err
	}
	confirmed, err := decodeRuntimeV2WSPayload[RuntimeV2AssignmentConfirmedPayload](envelope, RuntimeV2AssignmentConfirmed)
	if err != nil {
		return nil, err
	}
	if confirmed.AttemptIdentity != request.AttemptIdentity || confirmed.AttemptNo < 1 || confirmed.LeaseExpiresAt.IsZero() {
		return nil, errors.New("openlinker: invalid runtime v2 assignment confirmation")
	}
	c.removeOffer(request.AttemptIdentity, replyTo)
	return &confirmed, nil
}

func (c *RuntimeV2WebSocket) RejectRuntimeV2Assignment(
	ctx context.Context,
	request RuntimeV2AssignmentRejectPayload,
) (*RuntimeV2AssignmentRejectedPayload, error) {
	if err := validateRuntimeV2AttemptIdentity(request.AttemptIdentity); err != nil ||
		!runtimeV2Capacity(request.Capacity, request.Inflight) || !runtimeV2RejectReason(request.ReasonCode) {
		return nil, errors.New("openlinker: invalid runtime v2 assignment rejection")
	}
	replyTo, err := c.offerMessageID(request.AttemptIdentity)
	if err != nil {
		return nil, err
	}
	envelope, err := c.requestOne(ctx, RuntimeV2AssignmentReject, replyTo, request, RuntimeV2AssignmentRejected)
	if err != nil {
		return nil, err
	}
	rejected, err := decodeRuntimeV2WSPayload[RuntimeV2AssignmentRejectedPayload](envelope, RuntimeV2AssignmentRejected)
	if err != nil {
		return nil, err
	}
	if rejected.AttemptIdentity != request.AttemptIdentity ||
		!runtimeV2AssignmentRejectOutcome(rejected.Outcome) || !runtimeV2DispatchState(rejected.DispatchState) {
		return nil, errors.New("openlinker: invalid runtime v2 assignment rejection response")
	}
	c.removeOffer(request.AttemptIdentity, replyTo)
	return &rejected, nil
}

func (c *RuntimeV2WebSocket) RenewRuntimeV2Lease(
	ctx context.Context,
	request RuntimeV2LeaseRenewPayload,
) (*RuntimeV2LeaseRenewedPayload, error) {
	if err := validateRuntimeV2AttemptIdentity(request.AttemptIdentity); err != nil ||
		request.LastClientEventSeq < 0 || !runtimeV2Capacity(request.Capacity, request.Inflight) {
		return nil, errors.New("openlinker: invalid runtime v2 lease renewal")
	}
	envelope, err := c.requestOne(ctx, RuntimeV2LeaseRenew, "", request, RuntimeV2LeaseRenewed)
	if err != nil {
		return nil, err
	}
	renewed, err := decodeRuntimeV2WSPayload[RuntimeV2LeaseRenewedPayload](envelope, RuntimeV2LeaseRenewed)
	if err != nil {
		return nil, err
	}
	if renewed.AttemptIdentity != request.AttemptIdentity || renewed.LeaseExpiresAt.IsZero() {
		return nil, errors.New("openlinker: invalid runtime v2 lease response")
	}
	if renewed.PendingCommand != nil {
		if _, err = DecodeRuntimeV2PendingCommand(*renewed.PendingCommand); err != nil {
			return nil, fmt.Errorf("openlinker: invalid runtime v2 pending command: %w", err)
		}
	}
	return &renewed, nil
}

func (c *RuntimeV2WebSocket) AppendRuntimeV2Event(
	ctx context.Context,
	request RuntimeV2RunEventPayload,
) (*RuntimeV2RunEventAckPayload, error) {
	if err := validateRuntimeV2Event(request); err != nil {
		return nil, err
	}
	envelope, err := c.requestOne(ctx, RuntimeV2RunEvent, "", request, RuntimeV2RunEventAck)
	if err != nil {
		return nil, err
	}
	ack, err := decodeRuntimeV2WSPayload[RuntimeV2RunEventAckPayload](envelope, RuntimeV2RunEventAck)
	if err != nil {
		return nil, err
	}
	if ack.ClientEventID != request.ClientEventID || ack.ClientEventSeq != request.ClientEventSeq || ack.Sequence < 1 {
		return nil, errors.New("openlinker: runtime v2 event acknowledgement mismatch")
	}
	return &ack, nil
}

func (c *RuntimeV2WebSocket) FinalizeRuntimeV2Result(
	ctx context.Context,
	request RuntimeV2RunResultPayload,
) (*RuntimeV2RunResultAckPayload, error) {
	if err := validateRuntimeV2Result(request); err != nil {
		return nil, err
	}
	envelope, err := c.requestOne(ctx, RuntimeV2RunResult, "", request, RuntimeV2RunResultAck)
	if err != nil {
		return nil, err
	}
	ack, err := decodeRuntimeV2WSPayload[RuntimeV2RunResultAckPayload](envelope, RuntimeV2RunResultAck)
	if err != nil {
		return nil, err
	}
	if ack.ResultID != request.ResultID {
		return nil, errors.New("openlinker: runtime v2 result acknowledgement mismatch")
	}
	if err = validateRuntimeV2ResultAck(ack); err != nil {
		return nil, err
	}
	return &ack, nil
}

func (c *RuntimeV2WebSocket) ResumeRuntimeV2Runs(
	ctx context.Context,
	request RuntimeV2ResumePayload,
) (*RuntimeV2ResumeResponse, error) {
	if err := validateRuntimeV2Resume(request); err != nil {
		return nil, err
	}
	if request.RuntimeSessionID != c.hello.RuntimeSessionID {
		return nil, errors.New("openlinker: runtime v2 resume session mismatch")
	}
	if len(request.Attempts) == 0 {
		return &RuntimeV2ResumeResponse{Decisions: []RuntimeV2ResumeAcceptedPayload{}}, nil
	}
	replies, _, err := c.request(
		ctx,
		RuntimeV2Resume,
		"",
		request,
		[]RuntimeV2MessageType{RuntimeV2ResumeAccepted},
		len(request.Attempts),
	)
	if err != nil {
		return nil, err
	}
	response := &RuntimeV2ResumeResponse{Decisions: make([]RuntimeV2ResumeAcceptedPayload, len(replies))}
	for index, envelope := range replies {
		decision, decodeErr := decodeRuntimeV2WSPayload[RuntimeV2ResumeAcceptedPayload](envelope, RuntimeV2ResumeAccepted)
		if decodeErr != nil {
			return nil, decodeErr
		}
		if decision.AttemptIdentity != request.Attempts[index].AttemptIdentity {
			return nil, errors.New("openlinker: runtime v2 resume response order mismatch")
		}
		if decodeErr = validateRuntimeV2ResumeDecision(decision); decodeErr != nil {
			return nil, decodeErr
		}
		response.Decisions[index] = decision
	}
	return response, nil
}

func (c *RuntimeV2WebSocket) PollRuntimeV2Commands(
	ctx context.Context,
	runtimeSessionID string,
	_ int,
) (*RuntimeV2CommandsResponse, error) {
	if !runtimeV2UUID(runtimeSessionID) || runtimeSessionID != c.hello.RuntimeSessionID {
		return nil, errors.New("openlinker: invalid runtime v2 WebSocket command poll")
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
		return &RuntimeV2CommandsResponse{
			Commands:     []RuntimeV2PendingCommand{{Type: pushed.Command.Type, Payload: payload}},
			DatabaseTime: time.Now().UTC(),
		}, nil
	}
}

func (c *RuntimeV2WebSocket) AckRuntimeV2Cancel(
	ctx context.Context,
	request RuntimeV2RunCancelAckPayload,
) (*RuntimeV2RunCancellationState, error) {
	if err := validateRuntimeV2CancelAck(request); err != nil {
		return nil, err
	}
	replyTo, err := c.cancellationMessageID(request)
	if err != nil {
		return nil, err
	}
	_, raw, err := newRuntimeV2WSEnvelope(RuntimeV2RunCancelAck, replyTo, request)
	if err != nil {
		return nil, err
	}
	if err = c.writeMessage(raw); err != nil {
		return nil, err
	}
	return &RuntimeV2RunCancellationState{
		CancellationID: request.CancellationID,
		CancelState:    request.CancelState,
		UpdatedAt:      time.Now().UTC(),
		ErrorCode:      request.ErrorCode,
	}, nil
}

func (c *RuntimeV2WebSocket) CallRuntimeV2Agent(
	ctx context.Context,
	authorization RuntimeV2CallAgentAuthorization,
	request RuntimeV2CallAgentRequest,
) (*RuntimeV2RunSummary, error) {
	if c == nil || c.runtime == nil {
		return nil, errors.New("openlinker: runtime WebSocket is nil")
	}
	return c.runtime.CallRuntimeV2Agent(ctx, authorization, request)
}

func (c *RuntimeV2WebSocket) offerMessageID(identity RuntimeV2AttemptIdentity) (string, error) {
	c.correlationMu.RLock()
	messageID := c.offers[runtimeV2AttemptKey(identity)]
	c.correlationMu.RUnlock()
	if messageID == "" {
		return "", errors.New("openlinker: runtime v2 assignment has no WebSocket offer correlation")
	}
	return messageID, nil
}

func (c *RuntimeV2WebSocket) removeOffer(identity RuntimeV2AttemptIdentity, messageID string) {
	key := runtimeV2AttemptKey(identity)
	c.correlationMu.Lock()
	if c.offers[key] == messageID {
		delete(c.offers, key)
	}
	c.correlationMu.Unlock()
}

func (c *RuntimeV2WebSocket) cancellationMessageID(request RuntimeV2RunCancelAckPayload) (string, error) {
	key := request.CancellationID + "\x00" + runtimeV2AttemptKey(request.AttemptIdentity)
	c.correlationMu.RLock()
	messageID := c.cancellations[key]
	c.correlationMu.RUnlock()
	if messageID == "" {
		return "", errors.New("openlinker: runtime v2 cancellation has no WebSocket command correlation")
	}
	return messageID, nil
}

func runtimeV2HelloEqual(left, right RuntimeV2HelloPayload) bool {
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

func commandPayload(command RuntimeV2DecodedPendingCommand) ([]byte, error) {
	switch command.Type {
	case RuntimeV2RunCancel:
		if command.Cancel != nil {
			return json.Marshal(command.Cancel)
		}
	case RuntimeV2Drain:
		if command.Drain != nil {
			return json.Marshal(command.Drain)
		}
	case RuntimeV2LeaseRevoked:
		if command.Revoke != nil {
			return json.Marshal(command.Revoke)
		}
	}
	return nil, errors.New("openlinker: invalid runtime v2 WebSocket command")
}
