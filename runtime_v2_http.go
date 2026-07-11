package openlinker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"net/url"
	"regexp"
	"strconv"
	"strings"
	"unicode/utf8"
)

func (r *Runtime) CreateRuntimeV2Session(ctx context.Context, hello RuntimeV2HelloPayload) (*RuntimeV2ReadyPayload, error) {
	if err := validateRuntimeV2Hello(hello); err != nil {
		return nil, err
	}
	var ready RuntimeV2ReadyPayload
	if _, err := r.doRuntimeV2(ctx, http.MethodPost, "/agent-runtime/v2/sessions", nil, hello, &ready); err != nil {
		return nil, err
	}
	if err := validateRuntimeV2Ready(ready); err != nil {
		return nil, err
	}
	return &ready, nil
}

func (r *Runtime) HeartbeatRuntimeV2Session(ctx context.Context, hello RuntimeV2HelloPayload) (*RuntimeV2ReadyPayload, error) {
	if err := validateRuntimeV2Hello(hello); err != nil {
		return nil, err
	}
	var ready RuntimeV2ReadyPayload
	path := "/agent-runtime/v2/sessions/" + url.PathEscape(hello.RuntimeSessionID) + "/heartbeat"
	if _, err := r.doRuntimeV2(ctx, http.MethodPost, path, nil, hello, &ready); err != nil {
		return nil, err
	}
	if err := validateRuntimeV2Ready(ready); err != nil {
		return nil, err
	}
	return &ready, nil
}

func (r *Runtime) CloseRuntimeV2Session(ctx context.Context, request RuntimeV2SessionCloseRequest) error {
	if err := validateRuntimeV2SessionClose(request); err != nil {
		return err
	}
	path := "/agent-runtime/v2/sessions/" + url.PathEscape(request.RuntimeSessionID) + "/close"
	status, err := r.doRuntimeV2(ctx, http.MethodPost, path, nil, request, nil)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent {
		return errors.New("openlinker: runtime v2 close did not return 204")
	}
	return nil
}

func (r *Runtime) ClaimRuntimeV2Run(
	ctx context.Context,
	waitSeconds int,
	request RuntimeV2ClaimRequest,
) (*RuntimeV2RunAssignedPayload, error) {
	if waitSeconds < 0 || waitSeconds > RuntimeV2MaxPullWaitSeconds ||
		!runtimeV2UUID(request.RuntimeSessionID) || !runtimeV2Capacity(request.Capacity, request.Inflight) {
		return nil, errors.New("openlinker: invalid runtime v2 claim")
	}
	query := make(url.Values)
	query.Set("wait", strconv.Itoa(waitSeconds))
	var assigned RuntimeV2RunAssignedPayload
	status, err := r.doRuntimeV2(ctx, http.MethodPost, "/agent-runtime/v2/runs/claim", query, request, &assigned)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent {
		return nil, nil
	}
	if err := validateRuntimeV2Assignment(assigned); err != nil {
		return nil, err
	}
	if assigned.AttemptIdentity.RuntimeSessionID != request.RuntimeSessionID {
		return nil, errors.New("openlinker: runtime v2 claim returned an assignment for another session")
	}
	return &assigned, nil
}

func (r *Runtime) AckRuntimeV2Assignment(
	ctx context.Context,
	request RuntimeV2AssignmentAckPayload,
) (*RuntimeV2AssignmentConfirmedPayload, error) {
	if err := validateRuntimeV2AttemptIdentity(request.AttemptIdentity); err != nil {
		return nil, err
	}
	var confirmed RuntimeV2AssignmentConfirmedPayload
	path := runtimeV2RunPath(request.AttemptIdentity.RunID, "assignment-ack")
	if _, err := r.doRuntimeV2(ctx, http.MethodPost, path, nil, request, &confirmed); err != nil {
		return nil, err
	}
	if confirmed.AttemptIdentity != request.AttemptIdentity || confirmed.AttemptNo < 1 || confirmed.LeaseExpiresAt.IsZero() {
		return nil, errors.New("openlinker: invalid runtime v2 assignment confirmation")
	}
	return &confirmed, nil
}

func (r *Runtime) RejectRuntimeV2Assignment(
	ctx context.Context,
	request RuntimeV2AssignmentRejectPayload,
) (*RuntimeV2AssignmentRejectedPayload, error) {
	if err := validateRuntimeV2AttemptIdentity(request.AttemptIdentity); err != nil ||
		!runtimeV2Capacity(request.Capacity, request.Inflight) || !runtimeV2RejectReason(request.ReasonCode) {
		return nil, errors.New("openlinker: invalid runtime v2 assignment rejection")
	}
	var rejected RuntimeV2AssignmentRejectedPayload
	path := runtimeV2RunPath(request.AttemptIdentity.RunID, "assignment-reject")
	if _, err := r.doRuntimeV2(ctx, http.MethodPost, path, nil, request, &rejected); err != nil {
		return nil, err
	}
	if rejected.AttemptIdentity != request.AttemptIdentity ||
		!runtimeV2AssignmentRejectOutcome(rejected.Outcome) || !runtimeV2DispatchState(rejected.DispatchState) {
		return nil, errors.New("openlinker: invalid runtime v2 assignment rejection response")
	}
	return &rejected, nil
}

func (r *Runtime) RenewRuntimeV2Lease(
	ctx context.Context,
	request RuntimeV2LeaseRenewPayload,
) (*RuntimeV2LeaseRenewedPayload, error) {
	if err := validateRuntimeV2AttemptIdentity(request.AttemptIdentity); err != nil ||
		request.LastClientEventSeq < 0 || !runtimeV2Capacity(request.Capacity, request.Inflight) {
		return nil, errors.New("openlinker: invalid runtime v2 lease renewal")
	}
	var renewed RuntimeV2LeaseRenewedPayload
	path := runtimeV2RunPath(request.AttemptIdentity.RunID, "lease-renew")
	if _, err := r.doRuntimeV2(ctx, http.MethodPost, path, nil, request, &renewed); err != nil {
		return nil, err
	}
	if renewed.AttemptIdentity != request.AttemptIdentity || renewed.LeaseExpiresAt.IsZero() {
		return nil, errors.New("openlinker: invalid runtime v2 lease response")
	}
	if renewed.PendingCommand != nil {
		if _, err := DecodeRuntimeV2PendingCommand(*renewed.PendingCommand); err != nil {
			return nil, fmt.Errorf("openlinker: invalid runtime v2 pending command: %w", err)
		}
	}
	return &renewed, nil
}

func (r *Runtime) AppendRuntimeV2Event(
	ctx context.Context,
	request RuntimeV2RunEventPayload,
) (*RuntimeV2RunEventAckPayload, error) {
	if err := validateRuntimeV2Event(request); err != nil {
		return nil, err
	}
	var ack RuntimeV2RunEventAckPayload
	path := runtimeV2RunPath(request.AttemptIdentity.RunID, "events")
	if _, err := r.doRuntimeV2(ctx, http.MethodPost, path, nil, request, &ack); err != nil {
		return nil, err
	}
	if ack.ClientEventID != request.ClientEventID || ack.ClientEventSeq != request.ClientEventSeq || ack.Sequence < 1 {
		return nil, errors.New("openlinker: runtime v2 event acknowledgement mismatch")
	}
	return &ack, nil
}

func (r *Runtime) FinalizeRuntimeV2Result(
	ctx context.Context,
	request RuntimeV2RunResultPayload,
) (*RuntimeV2RunResultAckPayload, error) {
	if err := validateRuntimeV2Result(request); err != nil {
		return nil, err
	}
	var ack RuntimeV2RunResultAckPayload
	path := runtimeV2RunPath(request.AttemptIdentity.RunID, "result")
	if _, err := r.doRuntimeV2(ctx, http.MethodPost, path, nil, request, &ack); err != nil {
		return nil, err
	}
	if ack.ResultID != request.ResultID {
		return nil, errors.New("openlinker: runtime v2 result acknowledgement mismatch")
	}
	if err := validateRuntimeV2ResultAck(ack); err != nil {
		return nil, err
	}
	return &ack, nil
}

func (r *Runtime) ResumeRuntimeV2Runs(ctx context.Context, request RuntimeV2ResumePayload) (*RuntimeV2ResumeResponse, error) {
	if err := validateRuntimeV2Resume(request); err != nil {
		return nil, err
	}
	var response RuntimeV2ResumeResponse
	if _, err := r.doRuntimeV2(ctx, http.MethodPost, "/agent-runtime/v2/runs/resume", nil, request, &response); err != nil {
		return nil, err
	}
	if len(response.Decisions) != len(request.Attempts) {
		return nil, errors.New("openlinker: runtime v2 resume response count mismatch")
	}
	for index := range response.Decisions {
		if response.Decisions[index].AttemptIdentity != request.Attempts[index].AttemptIdentity {
			return nil, errors.New("openlinker: runtime v2 resume response order mismatch")
		}
		if err := validateRuntimeV2ResumeDecision(response.Decisions[index]); err != nil {
			return nil, err
		}
	}
	return &response, nil
}

func (r *Runtime) PollRuntimeV2Commands(
	ctx context.Context,
	runtimeSessionID string,
	waitSeconds int,
) (*RuntimeV2CommandsResponse, error) {
	if !runtimeV2UUID(runtimeSessionID) || waitSeconds < 0 || waitSeconds > RuntimeV2MaxPullWaitSeconds {
		return nil, errors.New("openlinker: invalid runtime v2 command poll")
	}
	query := make(url.Values)
	query.Set("runtime_session_id", runtimeSessionID)
	query.Set("wait", strconv.Itoa(waitSeconds))
	var response RuntimeV2CommandsResponse
	if _, err := r.doRuntimeV2(ctx, http.MethodGet, "/agent-runtime/v2/commands", query, nil, &response); err != nil {
		return nil, err
	}
	if response.Commands == nil || response.DatabaseTime.IsZero() {
		return nil, errors.New("openlinker: invalid runtime v2 commands response")
	}
	for _, command := range response.Commands {
		if _, err := DecodeRuntimeV2PendingCommand(command); err != nil {
			return nil, fmt.Errorf("openlinker: invalid runtime v2 command: %w", err)
		}
	}
	return &response, nil
}

func (r *Runtime) AckRuntimeV2Cancel(
	ctx context.Context,
	request RuntimeV2RunCancelAckPayload,
) (*RuntimeV2RunCancellationState, error) {
	if err := validateRuntimeV2CancelAck(request); err != nil {
		return nil, err
	}
	var state RuntimeV2RunCancellationState
	path := runtimeV2RunPath(request.AttemptIdentity.RunID, "cancel-ack")
	if _, err := r.doRuntimeV2(ctx, http.MethodPost, path, nil, request, &state); err != nil {
		return nil, err
	}
	if state.CancellationID != request.CancellationID {
		return nil, errors.New("openlinker: runtime v2 cancellation acknowledgement mismatch")
	}
	if err := validateRuntimeV2CancellationState(state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *Runtime) doRuntimeV2(
	ctx context.Context,
	method, path string,
	query url.Values,
	body, out any,
) (int, error) {
	if r == nil || r.client == nil {
		return 0, errors.New("openlinker: runtime client is nil")
	}
	response, err := r.client.newRuntimeRequest(ctx, method, path, query, body, "application/json")
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, parseRuntimeV2Error(response)
	}
	if response.StatusCode == http.StatusNoContent {
		return response.StatusCode, nil
	}
	if out == nil {
		return response.StatusCode, errors.New("openlinker: unexpected runtime v2 response body")
	}
	if err := decodeRuntimeV2Response(response.Body, out); err != nil {
		return response.StatusCode, fmt.Errorf("openlinker: decode runtime v2 response: %w", err)
	}
	return response.StatusCode, nil
}

func parseRuntimeV2Error(response *http.Response) error {
	raw, err := io.ReadAll(io.LimitReader(response.Body, RuntimeV2MaxMessageBytes+1))
	if err != nil {
		return fmt.Errorf("openlinker: read runtime v2 error response: %w", err)
	}
	if int64(len(raw)) > RuntimeV2MaxMessageBytes || len(bytes.TrimSpace(raw)) == 0 {
		return errors.New("openlinker: runtime v2 error response is empty or too large")
	}
	var envelope RuntimeV2ErrorEnvelope
	if err := decodeRuntimeV2Response(bytes.NewReader(raw), &envelope); err != nil {
		return fmt.Errorf("openlinker: decode runtime v2 error response: %w", err)
	}
	if err := validateRuntimeV2ErrorBody(envelope.Error); err != nil {
		return err
	}
	return &Error{
		StatusCode:   response.StatusCode,
		Code:         envelope.Error.Code,
		Message:      envelope.Error.Message,
		Details:      envelope.Error,
		RequestID:    firstHeader(response.Header, "X-Request-Id", "X-Correlation-Id"),
		RetryAfter:   retryAfter(response.Header),
		ResponseBody: raw,
	}
}

func decodeRuntimeV2Response(body io.Reader, out any) error {
	raw, err := io.ReadAll(io.LimitReader(body, RuntimeV2MaxMessageBytes+1))
	if err != nil {
		return err
	}
	if int64(len(raw)) > RuntimeV2MaxMessageBytes || len(bytes.TrimSpace(raw)) == 0 {
		return errors.New("runtime v2 response is empty or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("runtime v2 response contains trailing JSON")
	}
	return nil
}

func validateRuntimeV2Hello(value RuntimeV2HelloPayload) error {
	if !runtimeV2UUID(value.NodeID) || !runtimeV2UUID(value.AgentID) || !runtimeV2UUID(value.RuntimeSessionID) ||
		!runtimeV2Text(value.WorkerID, 200) || value.SessionEpoch < 1 || !runtimeV2Text(value.NodeVersion, 100) ||
		value.Capacity < 0 || value.Capacity > RuntimeV2MaxNodeCapacity ||
		value.ContractDigest != RuntimeContractDigest || !hasRuntimeV2Features(value.Features, RuntimeRequiredFeatures()) {
		return errors.New("openlinker: invalid runtime v2 hello")
	}
	return nil
}

func validateRuntimeV2Ready(value RuntimeV2ReadyPayload) error {
	if !runtimeV2UUID(value.CoreInstanceID) || value.OfferTTLSeconds < 1 || value.LeaseTTLSeconds < 1 ||
		value.DatabaseTime.IsZero() || !hasRuntimeV2Features(value.Features, RuntimeRequiredFeatures()) {
		return errors.New("openlinker: invalid runtime v2 ready response")
	}
	return nil
}

func validateRuntimeV2SessionClose(value RuntimeV2SessionCloseRequest) error {
	if !runtimeV2UUID(value.NodeID) || !runtimeV2UUID(value.AgentID) || !runtimeV2UUID(value.RuntimeSessionID) ||
		!runtimeV2Text(value.WorkerID, 200) || value.SessionEpoch < 1 ||
		(value.Status != "offline" && value.Status != "closed") || !runtimeV2Text(value.Reason, 200) {
		return errors.New("openlinker: invalid runtime v2 session close")
	}
	return nil
}

func validateRuntimeV2Assignment(value RuntimeV2RunAssignedPayload) error {
	if err := validateRuntimeV2AttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if value.OfferNo < 1 || value.OfferExpiresAt.IsZero() || value.AttemptDeadlineAt.IsZero() ||
		value.RunDeadlineAt.IsZero() || value.Input == nil ||
		!runtimeV2InvocationCapability(value.NodeEnvelope, "ol_ctx_v2.") ||
		!runtimeV2InvocationCapability(value.AgentInvocationToken, "ol_inv_v2.") {
		return errors.New("openlinker: invalid runtime v2 assignment")
	}
	return nil
}

func validateRuntimeV2AttemptIdentity(value RuntimeV2AttemptIdentity) error {
	if !runtimeV2UUID(value.RunID) || !runtimeV2UUID(value.AttemptID) || !runtimeV2UUID(value.LeaseID) ||
		!runtimeV2UUID(value.NodeID) || !runtimeV2UUID(value.AgentID) || !runtimeV2UUID(value.RuntimeSessionID) ||
		value.FencingToken < 1 || !runtimeV2Text(value.WorkerID, 200) {
		return errors.New("openlinker: invalid runtime v2 Attempt identity")
	}
	return nil
}

func validateRuntimeV2Event(value RuntimeV2RunEventPayload) error {
	if err := validateRuntimeV2AttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if !runtimeV2UUID(value.ClientEventID) || value.ClientEventSeq < 1 ||
		!runtimeV2EventTypePattern.MatchString(value.EventType) || value.Payload == nil {
		return errors.New("openlinker: invalid runtime v2 Event")
	}
	if _, reserved := runtimeV2CoreOwnedEventTypes[value.EventType]; reserved {
		return errors.New("openlinker: runtime v2 Event type is reserved by Core")
	}
	return nil
}

func validateRuntimeV2Result(value RuntimeV2RunResultPayload) error {
	if err := validateRuntimeV2AttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if !runtimeV2UUID(value.ResultID) || value.DurationMS < 0 || value.DurationMS > math.MaxInt32 || value.FinalClientEventSeq < 0 {
		return errors.New("openlinker: invalid runtime v2 Result")
	}
	switch value.Status {
	case "success":
		if value.Output == nil || value.Error != nil {
			return errors.New("openlinker: successful runtime v2 Result requires output only")
		}
	case "failed":
		if value.Output != nil || value.Error == nil || !runtimeV2Text(value.Error.ErrorCode, 120) || !runtimeV2Text(value.Error.Message, 500) {
			return errors.New("openlinker: failed runtime v2 Result requires error only")
		}
	default:
		return errors.New("openlinker: invalid runtime v2 Result status")
	}
	return nil
}

var runtimeV2EventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)

var runtimeV2CoreOwnedEventTypes = map[string]struct{}{
	"run.completed":  {},
	"run.failed":     {},
	"run.canceled":   {},
	"run.stream.gap": {},
}

func validateRuntimeV2ResultAck(value RuntimeV2RunResultAckPayload) error {
	if !runtimeV2UUID(value.ResultID) || !runtimeV2ResultClassification(value.Classification) ||
		!runtimeV2RunStatus(value.RunStatus) || !runtimeV2DispatchState(value.DispatchState) ||
		(value.NextAttemptAt != nil && value.NextAttemptAt.IsZero()) {
		return errors.New("openlinker: invalid runtime v2 result acknowledgement")
	}
	hasNextAttempt := value.NextAttemptAt != nil
	switch value.Classification {
	case RuntimeV2ResultSuccess:
		if value.RunStatus != RuntimeV2RunSuccess || value.DispatchState != RuntimeV2DispatchTerminal || hasNextAttempt {
			return errors.New("openlinker: inconsistent runtime v2 success acknowledgement")
		}
	case RuntimeV2ResultNonRetryableFailure:
		if value.RunStatus != RuntimeV2RunFailed || value.DispatchState != RuntimeV2DispatchTerminal || hasNextAttempt {
			return errors.New("openlinker: inconsistent runtime v2 failure acknowledgement")
		}
	case RuntimeV2ResultTimeout:
		if value.RunStatus != RuntimeV2RunTimeout || value.DispatchState != RuntimeV2DispatchTerminal || hasNextAttempt {
			return errors.New("openlinker: inconsistent runtime v2 timeout acknowledgement")
		}
	case RuntimeV2ResultCanceled:
		if value.RunStatus != RuntimeV2RunCanceled || value.DispatchState != RuntimeV2DispatchTerminal || hasNextAttempt {
			return errors.New("openlinker: inconsistent runtime v2 cancellation acknowledgement")
		}
	case RuntimeV2ResultDeadLetter:
		if value.RunStatus != RuntimeV2RunFailed || value.DispatchState != RuntimeV2DispatchDeadLetter || hasNextAttempt {
			return errors.New("openlinker: inconsistent runtime v2 dead-letter acknowledgement")
		}
	case RuntimeV2ResultRetryableFailure:
		if value.RunStatus != RuntimeV2RunRunning {
			return errors.New("openlinker: inconsistent runtime v2 retry acknowledgement")
		}
		switch value.DispatchState {
		case RuntimeV2DispatchRetryWait:
			if !hasNextAttempt {
				return errors.New("openlinker: retry_wait acknowledgement requires next_attempt_at")
			}
		case RuntimeV2DispatchPending, RuntimeV2DispatchOffered, RuntimeV2DispatchExecuting:
			if hasNextAttempt {
				return errors.New("openlinker: progressed retry acknowledgement cannot retain next_attempt_at")
			}
		default:
			return errors.New("openlinker: inconsistent runtime v2 retry dispatch state")
		}
	}
	return nil
}

func validateRuntimeV2ResumeDecision(value RuntimeV2ResumeAcceptedPayload) error {
	if err := validateRuntimeV2AttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if value.LeaseExpiresAt != nil && value.LeaseExpiresAt.IsZero() {
		return errors.New("openlinker: invalid runtime v2 resumed lease expiry")
	}
	seen := make(map[RuntimeV2ResumeAction]struct{}, len(value.AllowedActions))
	for _, action := range value.AllowedActions {
		if !runtimeV2ResumeAction(action) {
			return errors.New("openlinker: invalid runtime v2 resume action")
		}
		if _, duplicate := seen[action]; duplicate {
			return errors.New("openlinker: duplicate runtime v2 resume action")
		}
		seen[action] = struct{}{}
	}
	has := func(action RuntimeV2ResumeAction) bool {
		_, ok := seen[action]
		return ok
	}
	switch value.Decision {
	case RuntimeV2ResumeContinue:
		if value.LeaseExpiresAt == nil || len(seen) != 3 ||
			!has(RuntimeV2ActionContinueExecution) || !has(RuntimeV2ActionUploadEvents) || !has(RuntimeV2ActionUploadResult) {
			return errors.New("openlinker: invalid continue_execution resume decision")
		}
	case RuntimeV2ResumeUploadSpool:
		if value.LeaseExpiresAt != nil || len(seen) == 0 || len(seen) > 2 ||
			has(RuntimeV2ActionContinueExecution) || has(RuntimeV2ActionStopExecution) || has(RuntimeV2ActionClearSpool) {
			return errors.New("openlinker: invalid upload_spool_only resume decision")
		}
	case RuntimeV2ResumeResultAcked:
		if value.LeaseExpiresAt != nil || len(seen) != 1 || !has(RuntimeV2ActionClearSpool) {
			return errors.New("openlinker: invalid result_already_acked resume decision")
		}
	case RuntimeV2ResumeRevoked:
		if value.LeaseExpiresAt != nil || len(seen) != 2 ||
			!has(RuntimeV2ActionStopExecution) || !has(RuntimeV2ActionClearSpool) {
			return errors.New("openlinker: invalid lease_revoked resume decision")
		}
	default:
		return errors.New("openlinker: invalid runtime v2 resume decision")
	}
	return nil
}

func DecodeRuntimeV2PendingCommand(command RuntimeV2PendingCommand) (RuntimeV2DecodedPendingCommand, error) {
	decoded := RuntimeV2DecodedPendingCommand{Type: command.Type}
	switch command.Type {
	case RuntimeV2RunCancel:
		var payload RuntimeV2RunCancelPayload
		if err := decodeRuntimeV2Response(bytes.NewReader(command.Payload), &payload); err != nil {
			return RuntimeV2DecodedPendingCommand{}, err
		}
		if err := validateRuntimeV2Cancel(payload); err != nil {
			return RuntimeV2DecodedPendingCommand{}, err
		}
		decoded.Cancel = &payload
	case RuntimeV2Drain:
		var payload RuntimeV2DrainPayload
		if err := decodeRuntimeV2Response(bytes.NewReader(command.Payload), &payload); err != nil {
			return RuntimeV2DecodedPendingCommand{}, err
		}
		if payload.DeadlineAt.IsZero() || !runtimeV2Text(payload.ReasonCode, 120) || payload.Capacity < 0 || payload.Inflight < 0 {
			return RuntimeV2DecodedPendingCommand{}, errors.New("openlinker: invalid runtime v2 drain command")
		}
		decoded.Drain = &payload
	case RuntimeV2LeaseRevoked:
		var payload RuntimeV2RunLeaseRevokedPayload
		if err := decodeRuntimeV2Response(bytes.NewReader(command.Payload), &payload); err != nil {
			return RuntimeV2DecodedPendingCommand{}, err
		}
		if err := validateRuntimeV2AttemptIdentity(payload.AttemptIdentity); err != nil ||
			!runtimeV2Text(payload.ReasonCode, 120) || !runtimeV2DispatchState(payload.DispatchState) ||
			!runtimeV2RunStatus(payload.RunStatus) {
			return RuntimeV2DecodedPendingCommand{}, errors.New("openlinker: invalid runtime v2 lease revocation command")
		}
		decoded.Revoke = &payload
	default:
		return RuntimeV2DecodedPendingCommand{}, errors.New("openlinker: unknown runtime v2 command type")
	}
	return decoded, nil
}

func (command RuntimeV2PendingCommand) Decode() (RuntimeV2DecodedPendingCommand, error) {
	return DecodeRuntimeV2PendingCommand(command)
}

func validateRuntimeV2Cancel(value RuntimeV2RunCancelPayload) error {
	if !runtimeV2UUID(value.CancellationID) || !runtimeV2Text(value.ReasonCode, 120) || value.DeadlineAt.IsZero() {
		return errors.New("openlinker: invalid runtime v2 cancellation command")
	}
	return validateRuntimeV2AttemptIdentity(value.AttemptIdentity)
}

func validateRuntimeV2CancelAck(value RuntimeV2RunCancelAckPayload) error {
	if !runtimeV2UUID(value.CancellationID) {
		return errors.New("openlinker: invalid runtime v2 cancellation acknowledgement")
	}
	if err := validateRuntimeV2AttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	switch value.CancelState {
	case RuntimeV2CancelDelivered, RuntimeV2CancelStopping, RuntimeV2CancelStopped:
		if value.ErrorCode != "" {
			return errors.New("openlinker: successful runtime v2 cancellation acknowledgement cannot include error_code")
		}
	case RuntimeV2CancelUnsupported, RuntimeV2CancelFailed:
		if !runtimeV2Text(value.ErrorCode, 120) {
			return errors.New("openlinker: failed runtime v2 cancellation acknowledgement requires error_code")
		}
	default:
		return errors.New("openlinker: invalid runtime v2 cancellation acknowledgement state")
	}
	return nil
}

func validateRuntimeV2CancellationState(value RuntimeV2RunCancellationState) error {
	if !runtimeV2UUID(value.CancellationID) || !runtimeV2CancelState(value.CancelState) || value.UpdatedAt.IsZero() {
		return errors.New("openlinker: invalid runtime v2 cancellation state")
	}
	switch value.CancelState {
	case RuntimeV2CancelRequested, RuntimeV2CancelDelivered, RuntimeV2CancelStopping, RuntimeV2CancelStopped:
		if value.ErrorCode != "" {
			return errors.New("openlinker: successful runtime v2 cancellation state cannot include error_code")
		}
	case RuntimeV2CancelUnsupported, RuntimeV2CancelFailed, RuntimeV2CancelUnconfirmed:
		if !runtimeV2Text(value.ErrorCode, 120) {
			return errors.New("openlinker: failed runtime v2 cancellation state requires error_code")
		}
	}
	return nil
}

func validateRuntimeV2ErrorBody(value RuntimeV2ErrorBody) error {
	if !runtimeV2ErrorCode(value.Code) || !runtimeV2Text(value.Message, 500) ||
		(value.CurrentRunStatus != "" && !runtimeV2RunStatus(value.CurrentRunStatus)) ||
		(value.CurrentDispatchState != "" && !runtimeV2DispatchState(value.CurrentDispatchState)) {
		return errors.New("openlinker: invalid runtime v2 error envelope")
	}
	previous := int64(0)
	for _, eventRange := range value.MissingEventRanges {
		if eventRange.Start < 1 || eventRange.End < eventRange.Start || eventRange.Start <= previous {
			return errors.New("openlinker: invalid runtime v2 error event ranges")
		}
		previous = eventRange.End
	}
	return nil
}

func validateRuntimeV2Resume(value RuntimeV2ResumePayload) error {
	if !runtimeV2UUID(value.NodeID) || !runtimeV2UUID(value.AgentID) || !runtimeV2UUID(value.RuntimeSessionID) ||
		!runtimeV2Text(value.WorkerID, 200) || len(value.Attempts) > 1024 {
		return errors.New("openlinker: invalid runtime v2 resume identity")
	}
	seen := make(map[RuntimeV2AttemptIdentity]struct{}, len(value.Attempts))
	for _, attempt := range value.Attempts {
		identity := attempt.AttemptIdentity
		if err := validateRuntimeV2AttemptIdentity(identity); err != nil {
			return err
		}
		if identity.NodeID != value.NodeID || identity.AgentID != value.AgentID || identity.WorkerID != value.WorkerID ||
			attempt.LastAckedClientEventSeq < 0 || (attempt.PendingResultID == "") != (attempt.FinalClientEventSeq == nil) {
			return errors.New("openlinker: invalid runtime v2 resume Attempt")
		}
		if _, duplicate := seen[identity]; duplicate {
			return errors.New("openlinker: duplicate runtime v2 resume Attempt")
		}
		seen[identity] = struct{}{}
		previous := attempt.LastAckedClientEventSeq
		for _, eventRange := range attempt.PendingClientEventRanges {
			if eventRange.Start <= previous || eventRange.End < eventRange.Start {
				return errors.New("openlinker: invalid runtime v2 resume event ranges")
			}
			previous = eventRange.End
		}
		if attempt.PendingResultID != "" {
			if !runtimeV2UUID(attempt.PendingResultID) || *attempt.FinalClientEventSeq < previous {
				return errors.New("openlinker: invalid runtime v2 pending Result")
			}
		}
	}
	return nil
}

func runtimeV2RunPath(runID, action string) string {
	return "/agent-runtime/v2/runs/" + url.PathEscape(runID) + "/" + action
}

func runtimeV2Capacity(capacity, inflight int64) bool {
	return capacity >= 0 && capacity <= RuntimeV2MaxNodeCapacity && inflight >= 0 && inflight <= RuntimeV2MaxNodeCapacity
}

func runtimeV2RejectReason(reason RuntimeV2AssignmentRejectReason) bool {
	switch reason {
	case RuntimeV2RejectNodeAtCapacity, RuntimeV2RejectNodeDraining,
		RuntimeV2RejectClientUpgradeRequired, RuntimeV2RejectRequiredFeatureMissing:
		return true
	default:
		return false
	}
}

func runtimeV2AssignmentRejectOutcome(outcome RuntimeV2AssignmentRejectOutcome) bool {
	return outcome == RuntimeV2OfferRejected || outcome == RuntimeV2AssignmentLeaseRevoked
}

func runtimeV2ResultClassification(classification RuntimeV2ResultClassification) bool {
	switch classification {
	case RuntimeV2ResultSuccess, RuntimeV2ResultRetryableFailure, RuntimeV2ResultNonRetryableFailure,
		RuntimeV2ResultTimeout, RuntimeV2ResultCanceled, RuntimeV2ResultDeadLetter:
		return true
	default:
		return false
	}
}

func runtimeV2RunStatus(status RuntimeV2RunStatus) bool {
	switch status {
	case RuntimeV2RunRunning, RuntimeV2RunSuccess, RuntimeV2RunFailed, RuntimeV2RunTimeout, RuntimeV2RunCanceled:
		return true
	default:
		return false
	}
}

func runtimeV2DispatchState(state RuntimeV2DispatchState) bool {
	switch state {
	case RuntimeV2DispatchPending, RuntimeV2DispatchOffered, RuntimeV2DispatchExecuting,
		RuntimeV2DispatchRetryWait, RuntimeV2DispatchTerminal, RuntimeV2DispatchDeadLetter:
		return true
	default:
		return false
	}
}

func runtimeV2CancelState(state RuntimeV2CancelState) bool {
	switch state {
	case RuntimeV2CancelRequested, RuntimeV2CancelDelivered, RuntimeV2CancelStopping, RuntimeV2CancelStopped,
		RuntimeV2CancelUnsupported, RuntimeV2CancelFailed, RuntimeV2CancelUnconfirmed:
		return true
	default:
		return false
	}
}

func runtimeV2ResumeAction(action RuntimeV2ResumeAction) bool {
	switch action {
	case RuntimeV2ActionContinueExecution, RuntimeV2ActionUploadEvents, RuntimeV2ActionUploadResult,
		RuntimeV2ActionStopExecution, RuntimeV2ActionClearSpool:
		return true
	default:
		return false
	}
}

func runtimeV2ErrorCode(code string) bool {
	switch code {
	case "BAD_REQUEST", "UNAUTHORIZED", "FORBIDDEN", "PERMISSION_DENIED", "NOT_FOUND", "CONFLICT",
		"VALIDATION_FAILED", "RATE_LIMITED", "INTERNAL_ERROR", "SERVICE_UNAVAILABLE",
		"IDEMPOTENCY_KEY_REUSED", "RUN_ALREADY_TERMINAL", "STALE_LEASE", "LEASE_EXPIRED",
		"LEASE_IDENTITY_MISMATCH", "RESULT_ID_CONFLICT", "EVENT_ID_CONFLICT", "NODE_AT_CAPACITY",
		"RUNTIME_CLIENT_UPGRADE_REQUIRED", "RUNTIME_REQUIRED_FEATURE_MISSING", "RUN_CANCEL_REQUESTED",
		"RUN_CANCEL_UNCONFIRMED", "RUNTIME_RETRY_EXHAUSTED", "RUNTIME_DISPATCH_TIMEOUT",
		"RUN_DEADLINE_EXCEEDED", "EVENTS_MISSING", "REPLAY_INPUT_UNAVAILABLE",
		"ENDPOINT_RESULT_UNKNOWN", "RUNTIME_SESSION_CONFLICT", "RUNTIME_SPOOL_CORRUPT":
		return true
	default:
		return false
	}
}

func runtimeV2Text(value string, maximum int) bool {
	return value != "" && strings.TrimSpace(value) != "" && utf8.ValidString(value) && utf8.RuneCountInString(value) <= maximum
}

func runtimeV2UUID(value string) bool {
	if len(value) != 36 || value[8] != '-' || value[13] != '-' || value[18] != '-' || value[23] != '-' {
		return false
	}
	for index, char := range value {
		if index == 8 || index == 13 || index == 18 || index == 23 {
			continue
		}
		if (char < '0' || char > '9') && (char < 'a' || char > 'f') {
			return false
		}
	}
	return value != "00000000-0000-0000-0000-000000000000"
}

func hasRuntimeV2Features(got, required []string) bool {
	seen := make(map[string]struct{}, len(got))
	for _, feature := range got {
		if !runtimeV2Text(feature, 100) {
			return false
		}
		if _, duplicate := seen[feature]; duplicate {
			return false
		}
		seen[feature] = struct{}{}
	}
	for _, feature := range required {
		if _, ok := seen[feature]; !ok {
			return false
		}
	}
	return true
}
