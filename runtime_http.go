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

const (
	RuntimeAttachmentHeader     = "OpenLinker-Runtime-Attachment"
	RuntimeFallbackReasonHeader = "OpenLinker-Runtime-Fallback-Reason"
)

type runtimeFallbackReason string

const (
	runtimeFallbackExplicit             runtimeFallbackReason = "explicit"
	runtimeFallbackWebSocketUnavailable runtimeFallbackReason = "websocket_unavailable"
	runtimeFallbackPolicyForced         runtimeFallbackReason = "policy_forced"
	runtimeFallbackRecovery             runtimeFallbackReason = "recovery"
)

type runtimeFallbackReasonContextKey struct{}

func withRuntimeFallbackReason(ctx context.Context, reason runtimeFallbackReason) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	switch reason {
	case runtimeFallbackExplicit, runtimeFallbackWebSocketUnavailable, runtimeFallbackPolicyForced, runtimeFallbackRecovery:
		return context.WithValue(ctx, runtimeFallbackReasonContextKey{}, reason)
	default:
		return ctx
	}
}

func runtimeFallbackReasonFromContext(ctx context.Context) string {
	if ctx == nil {
		return ""
	}
	reason, _ := ctx.Value(runtimeFallbackReasonContextKey{}).(runtimeFallbackReason)
	switch reason {
	case runtimeFallbackExplicit, runtimeFallbackWebSocketUnavailable, runtimeFallbackPolicyForced, runtimeFallbackRecovery:
		return string(reason)
	default:
		return ""
	}
}

func (r *Runtime) CreateRuntimeSession(ctx context.Context, hello RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
	if err := validateRuntimeHello(hello); err != nil {
		return nil, err
	}
	if r == nil || r.client == nil {
		return nil, errors.New("openlinker: runtime client is nil")
	}
	r.attachmentMu.Lock()
	defer r.attachmentMu.Unlock()
	var ready RuntimeReadyPayload
	if _, err := r.doRuntimeWithAttachment(ctx, http.MethodPost, "/agent-runtime/sessions", nil, hello, &ready, ""); err != nil {
		return nil, err
	}
	if err := validateRuntimeReady(ready); err != nil {
		return nil, err
	}
	r.attachmentID = ready.AttachmentID
	return &ready, nil
}

func (r *Runtime) HeartbeatRuntimeSession(ctx context.Context, hello RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
	if err := validateRuntimeHello(hello); err != nil {
		return nil, err
	}
	if r == nil || r.client == nil {
		return nil, errors.New("openlinker: runtime client is nil")
	}
	r.attachmentMu.RLock()
	defer r.attachmentMu.RUnlock()
	attachmentID := r.attachmentID
	if !runtimeUUID(attachmentID) {
		return nil, errors.New("openlinker: runtime attachment is not established")
	}
	var ready RuntimeReadyPayload
	path := "/agent-runtime/sessions/" + url.PathEscape(hello.RuntimeSessionID) + "/heartbeat"
	if _, err := r.doRuntimeWithAttachment(ctx, http.MethodPost, path, nil, hello, &ready, attachmentID); err != nil {
		return nil, err
	}
	if err := validateRuntimeReady(ready); err != nil {
		return nil, err
	}
	if ready.AttachmentID != attachmentID {
		return nil, errors.New("openlinker: runtime heartbeat attachment mismatch")
	}
	return &ready, nil
}

// DrainRuntimeSession asks Core to durably fence this attached Session at
// capacity zero. The response is Core's persisted first-writer state; callers
// must use it instead of treating the request body as an acknowledgement.
func (r *Runtime) DrainRuntimeSession(
	ctx context.Context,
	runtimeSessionID string,
	request RuntimeDrainPayload,
) (*RuntimeDrainPayload, error) {
	if !runtimeUUID(runtimeSessionID) {
		return nil, errors.New("openlinker: invalid runtime drain Session")
	}
	if err := validateRuntimeDrain(request); err != nil {
		return nil, err
	}
	var response RuntimeDrainPayload
	path := "/agent-runtime/sessions/" + url.PathEscape(runtimeSessionID) + "/drain"
	if _, err := r.doRuntime(ctx, http.MethodPost, path, nil, request, &response); err != nil {
		return nil, err
	}
	if err := validateRuntimeDrain(response); err != nil {
		return nil, fmt.Errorf("openlinker: invalid runtime drain acknowledgement: %w", err)
	}
	return &response, nil
}

func (r *Runtime) CloseRuntimeSession(ctx context.Context, request RuntimeSessionCloseRequest) error {
	if err := validateRuntimeSessionClose(request); err != nil {
		return err
	}
	if r == nil || r.client == nil {
		return errors.New("openlinker: runtime client is nil")
	}
	r.attachmentMu.Lock()
	defer r.attachmentMu.Unlock()
	attachmentID := r.attachmentID
	if !runtimeUUID(attachmentID) {
		return errors.New("openlinker: runtime attachment is not established")
	}
	path := "/agent-runtime/sessions/" + url.PathEscape(request.RuntimeSessionID) + "/close"
	status, err := r.doRuntimeWithAttachment(ctx, http.MethodPost, path, nil, request, nil, attachmentID)
	if err != nil {
		return err
	}
	if status != http.StatusNoContent {
		return errors.New("openlinker: runtime close did not return 204")
	}
	r.attachmentID = ""
	return nil
}

func (r *Runtime) ClaimRuntimeRun(
	ctx context.Context,
	waitSeconds int,
	request RuntimeClaimRequest,
) (*RuntimeRunAssignedPayload, error) {
	if waitSeconds < 0 || waitSeconds > RuntimeMaxPullWaitSeconds ||
		!runtimeUUID(request.RuntimeSessionID) || !runtimeCapacity(request.Capacity, request.Inflight) {
		return nil, errors.New("openlinker: invalid runtime claim")
	}
	query := make(url.Values)
	query.Set("wait", strconv.Itoa(waitSeconds))
	var assigned RuntimeRunAssignedPayload
	status, err := r.doRuntime(ctx, http.MethodPost, "/agent-runtime/runs/claim", query, request, &assigned)
	if err != nil {
		return nil, err
	}
	if status == http.StatusNoContent {
		return nil, nil
	}
	if err := validateRuntimeAssignment(assigned); err != nil {
		return nil, err
	}
	if assigned.AttemptIdentity.RuntimeSessionID != request.RuntimeSessionID {
		return nil, errors.New("openlinker: runtime claim returned an assignment for another session")
	}
	return &assigned, nil
}

func (r *Runtime) AckRuntimeAssignment(
	ctx context.Context,
	request RuntimeAssignmentAckPayload,
) (*RuntimeAssignmentConfirmedPayload, error) {
	if err := validateRuntimeAttemptIdentity(request.AttemptIdentity); err != nil {
		return nil, err
	}
	var confirmed RuntimeAssignmentConfirmedPayload
	path := runtimeRunPath(request.AttemptIdentity.RunID, "assignment-ack")
	if _, err := r.doRuntime(ctx, http.MethodPost, path, nil, request, &confirmed); err != nil {
		return nil, err
	}
	if confirmed.AttemptIdentity != request.AttemptIdentity || confirmed.AttemptNo < 1 || confirmed.LeaseExpiresAt.IsZero() {
		return nil, errors.New("openlinker: invalid runtime assignment confirmation")
	}
	return &confirmed, nil
}

func (r *Runtime) RejectRuntimeAssignment(
	ctx context.Context,
	request RuntimeAssignmentRejectPayload,
) (*RuntimeAssignmentRejectedPayload, error) {
	if err := validateRuntimeAttemptIdentity(request.AttemptIdentity); err != nil ||
		!runtimeCapacity(request.Capacity, request.Inflight) || !runtimeRejectReason(request.ReasonCode) {
		return nil, errors.New("openlinker: invalid runtime assignment rejection")
	}
	var rejected RuntimeAssignmentRejectedPayload
	path := runtimeRunPath(request.AttemptIdentity.RunID, "assignment-reject")
	if _, err := r.doRuntime(ctx, http.MethodPost, path, nil, request, &rejected); err != nil {
		return nil, err
	}
	if rejected.AttemptIdentity != request.AttemptIdentity ||
		!runtimeAssignmentRejectOutcome(rejected.Outcome) || !runtimeDispatchState(rejected.DispatchState) {
		return nil, errors.New("openlinker: invalid runtime assignment rejection response")
	}
	return &rejected, nil
}

func (r *Runtime) RenewRuntimeLease(
	ctx context.Context,
	request RuntimeLeaseRenewPayload,
) (*RuntimeLeaseRenewedPayload, error) {
	if err := validateRuntimeAttemptIdentity(request.AttemptIdentity); err != nil ||
		request.LastClientEventSeq < 0 || !runtimeCapacity(request.Capacity, request.Inflight) {
		return nil, errors.New("openlinker: invalid runtime lease renewal")
	}
	var renewed RuntimeLeaseRenewedPayload
	path := runtimeRunPath(request.AttemptIdentity.RunID, "lease-renew")
	if _, err := r.doRuntime(ctx, http.MethodPost, path, nil, request, &renewed); err != nil {
		return nil, err
	}
	if renewed.AttemptIdentity != request.AttemptIdentity || renewed.LeaseExpiresAt.IsZero() {
		return nil, errors.New("openlinker: invalid runtime lease response")
	}
	if renewed.PendingCommand != nil {
		if _, err := DecodeRuntimePendingCommand(*renewed.PendingCommand); err != nil {
			return nil, fmt.Errorf("openlinker: invalid runtime pending command: %w", err)
		}
	}
	return &renewed, nil
}

func (r *Runtime) AppendRuntimeEvent(
	ctx context.Context,
	request RuntimeRunEventPayload,
) (*RuntimeRunEventAckPayload, error) {
	if err := validateRuntimeEvent(request); err != nil {
		return nil, err
	}
	var ack RuntimeRunEventAckPayload
	path := runtimeRunPath(request.AttemptIdentity.RunID, "events")
	if _, err := r.doRuntime(ctx, http.MethodPost, path, nil, request, &ack); err != nil {
		return nil, err
	}
	if ack.ClientEventID != request.ClientEventID || ack.ClientEventSeq != request.ClientEventSeq || ack.Sequence < 1 {
		return nil, errors.New("openlinker: runtime event acknowledgement mismatch")
	}
	return &ack, nil
}

func (r *Runtime) FinalizeRuntimeResult(
	ctx context.Context,
	request RuntimeRunResultPayload,
) (*RuntimeRunResultAckPayload, error) {
	if err := validateRuntimeResult(request); err != nil {
		return nil, err
	}
	var ack RuntimeRunResultAckPayload
	path := runtimeRunPath(request.AttemptIdentity.RunID, "result")
	if _, err := r.doRuntime(ctx, http.MethodPost, path, nil, request, &ack); err != nil {
		return nil, err
	}
	if ack.ResultID != request.ResultID {
		return nil, errors.New("openlinker: runtime result acknowledgement mismatch")
	}
	if err := validateRuntimeResultAck(ack); err != nil {
		return nil, err
	}
	return &ack, nil
}

func (r *Runtime) ResumeRuntimeRuns(ctx context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
	if err := validateRuntimeResume(request); err != nil {
		return nil, err
	}
	var response RuntimeResumeResponse
	if _, err := r.doRuntime(ctx, http.MethodPost, "/agent-runtime/runs/resume", nil, request, &response); err != nil {
		return nil, err
	}
	if len(response.Decisions) != len(request.Attempts) {
		return nil, errors.New("openlinker: runtime resume response count mismatch")
	}
	for index := range response.Decisions {
		if response.Decisions[index].AttemptIdentity != request.Attempts[index].AttemptIdentity {
			return nil, errors.New("openlinker: runtime resume response order mismatch")
		}
		if err := validateRuntimeResumeDecision(response.Decisions[index]); err != nil {
			return nil, err
		}
	}
	return &response, nil
}

func (r *Runtime) PollRuntimeCommands(
	ctx context.Context,
	runtimeSessionID string,
	waitSeconds int,
) (*RuntimeCommandsResponse, error) {
	if !runtimeUUID(runtimeSessionID) || waitSeconds < 0 || waitSeconds > RuntimeMaxPullWaitSeconds {
		return nil, errors.New("openlinker: invalid runtime command poll")
	}
	query := make(url.Values)
	query.Set("runtime_session_id", runtimeSessionID)
	query.Set("wait", strconv.Itoa(waitSeconds))
	var response RuntimeCommandsResponse
	if _, err := r.doRuntime(ctx, http.MethodGet, "/agent-runtime/commands", query, nil, &response); err != nil {
		return nil, err
	}
	if response.Commands == nil || response.DatabaseTime.IsZero() {
		return nil, errors.New("openlinker: invalid runtime commands response")
	}
	for _, command := range response.Commands {
		if _, err := DecodeRuntimePendingCommand(command); err != nil {
			return nil, fmt.Errorf("openlinker: invalid runtime command: %w", err)
		}
	}
	return &response, nil
}

func (r *Runtime) AckRuntimeCancel(
	ctx context.Context,
	request RuntimeRunCancelAckPayload,
) (*RuntimeRunCancellationState, error) {
	if err := validateRuntimeCancelAck(request); err != nil {
		return nil, err
	}
	var state RuntimeRunCancellationState
	path := runtimeRunPath(request.AttemptIdentity.RunID, "cancel-ack")
	if _, err := r.doRuntime(ctx, http.MethodPost, path, nil, request, &state); err != nil {
		return nil, err
	}
	if state.CancellationID != request.CancellationID {
		return nil, errors.New("openlinker: runtime cancellation acknowledgement mismatch")
	}
	if err := validateRuntimeCancellationState(state); err != nil {
		return nil, err
	}
	return &state, nil
}

func (r *Runtime) doRuntime(
	ctx context.Context,
	method, path string,
	query url.Values,
	body, out any,
) (int, error) {
	if r == nil || r.client == nil {
		return 0, errors.New("openlinker: runtime client is nil")
	}
	r.attachmentMu.RLock()
	defer r.attachmentMu.RUnlock()
	return r.doRuntimeWithAttachment(ctx, method, path, query, body, out, r.attachmentID)
}

func (r *Runtime) doRuntimeWithAttachment(
	ctx context.Context,
	method, path string,
	query url.Values,
	body, out any,
	attachmentID string,
) (int, error) {
	headers := make(http.Header)
	// Reserve the SDK-owned header even when no reason applies so a caller
	// default cannot inject an unbounded value on create or any later request.
	headers[http.CanonicalHeaderKey(RuntimeFallbackReasonHeader)] = nil
	if path == "/agent-runtime/sessions" {
		if reason := runtimeFallbackReasonFromContext(ctx); reason != "" {
			headers.Set(RuntimeFallbackReasonHeader, reason)
		}
	}
	if path != "/agent-runtime/sessions" {
		if !runtimeUUID(attachmentID) {
			return 0, errors.New("openlinker: runtime attachment is not established")
		}
		headers.Set(RuntimeAttachmentHeader, attachmentID)
	}
	response, err := r.client.newRuntimeRequestWithHeaders(ctx, method, path, query, body, "application/json", headers)
	if err != nil {
		return 0, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return response.StatusCode, parseRuntimeError(response)
	}
	if response.StatusCode == http.StatusNoContent {
		return response.StatusCode, nil
	}
	if out == nil {
		return response.StatusCode, errors.New("openlinker: unexpected runtime response body")
	}
	if err := decodeRuntimeResponse(response.Body, out); err != nil {
		return response.StatusCode, fmt.Errorf("openlinker: decode runtime response: %w", err)
	}
	return response.StatusCode, nil
}

func parseRuntimeError(response *http.Response) error {
	raw, err := io.ReadAll(io.LimitReader(response.Body, RuntimeMaxMessageBytes+1))
	if err != nil {
		return fmt.Errorf("openlinker: read runtime error response: %w", err)
	}
	if int64(len(raw)) > RuntimeMaxMessageBytes || len(bytes.TrimSpace(raw)) == 0 {
		return errors.New("openlinker: runtime error response is empty or too large")
	}
	var envelope RuntimeErrorEnvelope
	if err := decodeRuntimeResponse(bytes.NewReader(raw), &envelope); err != nil {
		return fmt.Errorf("openlinker: decode runtime error response: %w", err)
	}
	if err := validateRuntimeErrorBody(envelope.Error); err != nil {
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

func decodeRuntimeResponse(body io.Reader, out any) error {
	raw, err := io.ReadAll(io.LimitReader(body, RuntimeMaxMessageBytes+1))
	if err != nil {
		return err
	}
	if int64(len(raw)) > RuntimeMaxMessageBytes || len(bytes.TrimSpace(raw)) == 0 {
		return errors.New("runtime response is empty or too large")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(out); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); !errors.Is(err, io.EOF) {
		return errors.New("runtime response contains trailing JSON")
	}
	return nil
}

func validateRuntimeHello(value RuntimeHelloPayload) error {
	if !runtimeUUID(value.NodeID) || !runtimeUUID(value.AgentID) || !runtimeUUID(value.RuntimeSessionID) ||
		!runtimeText(value.WorkerID, 200) || value.SessionEpoch < 1 || !runtimeText(value.NodeVersion, 100) ||
		value.Capacity < 0 || value.Capacity > RuntimeMaxNodeCapacity ||
		value.ContractDigest != RuntimeContractDigest || !hasRuntimeFeatures(value.Features, RuntimeRequiredFeatures()) {
		return errors.New("openlinker: invalid runtime hello")
	}
	return nil
}

func validateRuntimeReady(value RuntimeReadyPayload) error {
	if !runtimeUUID(value.CoreInstanceID) || !runtimeUUID(value.AttachmentID) || value.OfferTTLSeconds < 1 || value.LeaseTTLSeconds < 1 ||
		value.DatabaseTime.IsZero() || !hasRuntimeFeatures(value.Features, RuntimeRequiredFeatures()) {
		return errors.New("openlinker: invalid runtime ready response")
	}
	return nil
}

func validateRuntimeSessionClose(value RuntimeSessionCloseRequest) error {
	if !runtimeUUID(value.NodeID) || !runtimeUUID(value.AgentID) || !runtimeUUID(value.RuntimeSessionID) ||
		!runtimeText(value.WorkerID, 200) || value.SessionEpoch < 1 ||
		(value.Status != "offline" && value.Status != "closed") || !runtimeText(value.Reason, 200) {
		return errors.New("openlinker: invalid runtime session close")
	}
	return nil
}

func validateRuntimeDrain(value RuntimeDrainPayload) error {
	if value.DeadlineAt.IsZero() || !runtimeText(value.ReasonCode, 120) ||
		value.Capacity != 0 || value.Inflight < 0 {
		return errors.New("openlinker: invalid runtime drain")
	}
	return nil
}

func validateRuntimeAssignment(value RuntimeRunAssignedPayload) error {
	if err := validateRuntimeAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if value.OfferNo < 1 || value.OfferExpiresAt.IsZero() || value.AttemptDeadlineAt.IsZero() ||
		value.RunDeadlineAt.IsZero() || value.Input == nil ||
		!runtimeInvocationCapability(value.NodeEnvelope, "ol_ctx_v2.") ||
		!runtimeInvocationCapability(value.AgentInvocationToken, "ol_inv_v2.") {
		return errors.New("openlinker: invalid runtime assignment")
	}
	return nil
}

func validateRuntimeAttemptIdentity(value RuntimeAttemptIdentity) error {
	if !runtimeUUID(value.RunID) || !runtimeUUID(value.AttemptID) || !runtimeUUID(value.LeaseID) ||
		!runtimeUUID(value.NodeID) || !runtimeUUID(value.AgentID) || !runtimeUUID(value.RuntimeSessionID) ||
		value.FencingToken < 1 || !runtimeText(value.WorkerID, 200) {
		return errors.New("openlinker: invalid runtime Attempt identity")
	}
	return nil
}

func validateRuntimeEvent(value RuntimeRunEventPayload) error {
	if err := validateRuntimeAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if !runtimeUUID(value.ClientEventID) || value.ClientEventSeq < 1 ||
		!runtimeEventTypePattern.MatchString(value.EventType) || value.Payload == nil {
		return errors.New("openlinker: invalid runtime Event")
	}
	if _, reserved := runtimeCoreOwnedEventTypes[value.EventType]; reserved {
		return errors.New("openlinker: runtime Event type is reserved by Core")
	}
	return nil
}

func validateRuntimeResult(value RuntimeRunResultPayload) error {
	if err := validateRuntimeAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if !runtimeUUID(value.ResultID) || value.DurationMS < 0 || value.DurationMS > math.MaxInt32 || value.FinalClientEventSeq < 0 {
		return errors.New("openlinker: invalid runtime Result")
	}
	switch value.Status {
	case "success":
		if value.Output == nil || value.Error != nil {
			return errors.New("openlinker: successful runtime Result requires output only")
		}
	case "failed":
		if value.Output != nil || value.Error == nil || !runtimeText(value.Error.ErrorCode, 120) || !runtimeText(value.Error.Message, 500) {
			return errors.New("openlinker: failed runtime Result requires error only")
		}
	default:
		return errors.New("openlinker: invalid runtime Result status")
	}
	return nil
}

var runtimeEventTypePattern = regexp.MustCompile(`^[a-z][a-z0-9_]*(\.[a-z][a-z0-9_]*)+$`)

var runtimeCoreOwnedEventTypes = map[string]struct{}{
	"run.completed":  {},
	"run.failed":     {},
	"run.canceled":   {},
	"run.stream.gap": {},
}

func validateRuntimeResultAck(value RuntimeRunResultAckPayload) error {
	if !runtimeUUID(value.ResultID) || !runtimeResultClassification(value.Classification) ||
		!runtimeRunStatus(value.RunStatus) || !runtimeDispatchState(value.DispatchState) ||
		(value.NextAttemptAt != nil && value.NextAttemptAt.IsZero()) {
		return errors.New("openlinker: invalid runtime result acknowledgement")
	}
	hasNextAttempt := value.NextAttemptAt != nil
	switch value.Classification {
	case RuntimeResultSuccess:
		if value.RunStatus != RuntimeRunSuccess || value.DispatchState != RuntimeDispatchTerminal || hasNextAttempt {
			return errors.New("openlinker: inconsistent runtime success acknowledgement")
		}
	case RuntimeResultNonRetryableFailure:
		if value.RunStatus != RuntimeRunFailed || value.DispatchState != RuntimeDispatchTerminal || hasNextAttempt {
			return errors.New("openlinker: inconsistent runtime failure acknowledgement")
		}
	case RuntimeResultTimeout:
		if value.RunStatus != RuntimeRunTimeout || value.DispatchState != RuntimeDispatchTerminal || hasNextAttempt {
			return errors.New("openlinker: inconsistent runtime timeout acknowledgement")
		}
	case RuntimeResultCanceled:
		if value.RunStatus != RuntimeRunCanceled || value.DispatchState != RuntimeDispatchTerminal || hasNextAttempt {
			return errors.New("openlinker: inconsistent runtime cancellation acknowledgement")
		}
	case RuntimeResultDeadLetter:
		if value.RunStatus != RuntimeRunFailed || value.DispatchState != RuntimeDispatchDeadLetter || hasNextAttempt {
			return errors.New("openlinker: inconsistent runtime dead-letter acknowledgement")
		}
	case RuntimeResultRetryableFailure:
		if value.RunStatus != RuntimeRunRunning {
			return errors.New("openlinker: inconsistent runtime retry acknowledgement")
		}
		switch value.DispatchState {
		case RuntimeDispatchRetryWait:
			if !hasNextAttempt {
				return errors.New("openlinker: retry_wait acknowledgement requires next_attempt_at")
			}
		case RuntimeDispatchPending, RuntimeDispatchOffered, RuntimeDispatchExecuting:
			if hasNextAttempt {
				return errors.New("openlinker: progressed retry acknowledgement cannot retain next_attempt_at")
			}
		default:
			return errors.New("openlinker: inconsistent runtime retry dispatch state")
		}
	}
	return nil
}

func validateRuntimeResumeDecision(value RuntimeResumeAcceptedPayload) error {
	if err := validateRuntimeAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if value.LeaseExpiresAt != nil && value.LeaseExpiresAt.IsZero() {
		return errors.New("openlinker: invalid runtime resumed lease expiry")
	}
	seen := make(map[RuntimeResumeAction]struct{}, len(value.AllowedActions))
	for _, action := range value.AllowedActions {
		if !runtimeResumeAction(action) {
			return errors.New("openlinker: invalid runtime resume action")
		}
		if _, duplicate := seen[action]; duplicate {
			return errors.New("openlinker: duplicate runtime resume action")
		}
		seen[action] = struct{}{}
	}
	has := func(action RuntimeResumeAction) bool {
		_, ok := seen[action]
		return ok
	}
	switch value.Decision {
	case RuntimeResumeContinue:
		if value.LeaseExpiresAt == nil || len(seen) != 3 ||
			!has(RuntimeActionContinueExecution) || !has(RuntimeActionUploadEvents) || !has(RuntimeActionUploadResult) {
			return errors.New("openlinker: invalid continue_execution resume decision")
		}
	case RuntimeResumeUploadSpool:
		if value.LeaseExpiresAt != nil || len(seen) == 0 || len(seen) > 2 ||
			has(RuntimeActionContinueExecution) || has(RuntimeActionStopExecution) || has(RuntimeActionClearSpool) {
			return errors.New("openlinker: invalid upload_spool_only resume decision")
		}
	case RuntimeResumeResultAcked:
		if value.LeaseExpiresAt != nil || len(seen) != 1 || !has(RuntimeActionClearSpool) {
			return errors.New("openlinker: invalid result_already_acked resume decision")
		}
	case RuntimeResumeRevoked:
		if value.LeaseExpiresAt != nil || len(seen) != 2 ||
			!has(RuntimeActionStopExecution) || !has(RuntimeActionClearSpool) {
			return errors.New("openlinker: invalid lease_revoked resume decision")
		}
	default:
		return errors.New("openlinker: invalid runtime resume decision")
	}
	return nil
}

func DecodeRuntimePendingCommand(command RuntimePendingCommand) (RuntimeDecodedPendingCommand, error) {
	decoded := RuntimeDecodedPendingCommand{Type: command.Type}
	switch command.Type {
	case RuntimeRunCancel:
		var payload RuntimeRunCancelPayload
		if err := decodeRuntimeResponse(bytes.NewReader(command.Payload), &payload); err != nil {
			return RuntimeDecodedPendingCommand{}, err
		}
		if err := validateRuntimeCancel(payload); err != nil {
			return RuntimeDecodedPendingCommand{}, err
		}
		decoded.Cancel = &payload
	case RuntimeDrain:
		var payload RuntimeDrainPayload
		if err := decodeRuntimeResponse(bytes.NewReader(command.Payload), &payload); err != nil {
			return RuntimeDecodedPendingCommand{}, err
		}
		if err := validateRuntimeDrain(payload); err != nil {
			return RuntimeDecodedPendingCommand{}, errors.New("openlinker: invalid runtime drain command")
		}
		decoded.Drain = &payload
	case RuntimeLeaseRevoked:
		var payload RuntimeRunLeaseRevokedPayload
		if err := decodeRuntimeResponse(bytes.NewReader(command.Payload), &payload); err != nil {
			return RuntimeDecodedPendingCommand{}, err
		}
		if err := validateRuntimeAttemptIdentity(payload.AttemptIdentity); err != nil ||
			!runtimeText(payload.ReasonCode, 120) || !runtimeDispatchState(payload.DispatchState) ||
			!runtimeRunStatus(payload.RunStatus) {
			return RuntimeDecodedPendingCommand{}, errors.New("openlinker: invalid runtime lease revocation command")
		}
		decoded.Revoke = &payload
	default:
		return RuntimeDecodedPendingCommand{}, errors.New("openlinker: unknown runtime command type")
	}
	return decoded, nil
}

func (command RuntimePendingCommand) Decode() (RuntimeDecodedPendingCommand, error) {
	return DecodeRuntimePendingCommand(command)
}

func validateRuntimeCancel(value RuntimeRunCancelPayload) error {
	if !runtimeUUID(value.CancellationID) || !runtimeText(value.ReasonCode, 120) || value.DeadlineAt.IsZero() {
		return errors.New("openlinker: invalid runtime cancellation command")
	}
	return validateRuntimeAttemptIdentity(value.AttemptIdentity)
}

func validateRuntimeCancelAck(value RuntimeRunCancelAckPayload) error {
	if !runtimeUUID(value.CancellationID) {
		return errors.New("openlinker: invalid runtime cancellation acknowledgement")
	}
	if err := validateRuntimeAttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	switch value.CancelState {
	case RuntimeCancelDelivered, RuntimeCancelStopping, RuntimeCancelStopped:
		if value.ErrorCode != "" {
			return errors.New("openlinker: successful runtime cancellation acknowledgement cannot include error_code")
		}
	case RuntimeCancelUnsupported, RuntimeCancelFailed:
		if !runtimeText(value.ErrorCode, 120) {
			return errors.New("openlinker: failed runtime cancellation acknowledgement requires error_code")
		}
	default:
		return errors.New("openlinker: invalid runtime cancellation acknowledgement state")
	}
	return nil
}

func validateRuntimeCancellationState(value RuntimeRunCancellationState) error {
	if !runtimeUUID(value.CancellationID) || !runtimeCancelState(value.CancelState) || value.UpdatedAt.IsZero() {
		return errors.New("openlinker: invalid runtime cancellation state")
	}
	switch value.CancelState {
	case RuntimeCancelRequested, RuntimeCancelDelivered, RuntimeCancelStopping, RuntimeCancelStopped:
		if value.ErrorCode != "" {
			return errors.New("openlinker: successful runtime cancellation state cannot include error_code")
		}
	case RuntimeCancelUnsupported, RuntimeCancelFailed, RuntimeCancelUnconfirmed:
		if !runtimeText(value.ErrorCode, 120) {
			return errors.New("openlinker: failed runtime cancellation state requires error_code")
		}
	}
	return nil
}

func validateRuntimeErrorBody(value RuntimeErrorBody) error {
	if !validRuntimeErrorCode(value.Code) || !runtimeText(value.Message, 500) ||
		(value.CurrentRunStatus != "" && !runtimeRunStatus(value.CurrentRunStatus)) ||
		(value.CurrentDispatchState != "" && !runtimeDispatchState(value.CurrentDispatchState)) {
		return errors.New("openlinker: invalid runtime error envelope")
	}
	previous := int64(0)
	for _, eventRange := range value.MissingEventRanges {
		if eventRange.Start < 1 || eventRange.End < eventRange.Start || eventRange.Start <= previous {
			return errors.New("openlinker: invalid runtime error event ranges")
		}
		previous = eventRange.End
	}
	return nil
}

func validateRuntimeResume(value RuntimeResumePayload) error {
	if !runtimeUUID(value.NodeID) || !runtimeUUID(value.AgentID) || !runtimeUUID(value.RuntimeSessionID) ||
		!runtimeText(value.WorkerID, 200) || len(value.Attempts) > 1024 {
		return errors.New("openlinker: invalid runtime resume identity")
	}
	seen := make(map[RuntimeAttemptIdentity]struct{}, len(value.Attempts))
	for _, attempt := range value.Attempts {
		identity := attempt.AttemptIdentity
		if err := validateRuntimeAttemptIdentity(identity); err != nil {
			return err
		}
		if identity.NodeID != value.NodeID || identity.AgentID != value.AgentID || identity.WorkerID != value.WorkerID ||
			attempt.LastAckedClientEventSeq < 0 || (attempt.PendingResultID == "") != (attempt.FinalClientEventSeq == nil) {
			return errors.New("openlinker: invalid runtime resume Attempt")
		}
		if _, duplicate := seen[identity]; duplicate {
			return errors.New("openlinker: duplicate runtime resume Attempt")
		}
		seen[identity] = struct{}{}
		previous := attempt.LastAckedClientEventSeq
		for _, eventRange := range attempt.PendingClientEventRanges {
			if eventRange.Start <= previous || eventRange.End < eventRange.Start {
				return errors.New("openlinker: invalid runtime resume event ranges")
			}
			previous = eventRange.End
		}
		if attempt.PendingResultID != "" {
			if !runtimeUUID(attempt.PendingResultID) || *attempt.FinalClientEventSeq < previous {
				return errors.New("openlinker: invalid runtime pending Result")
			}
		}
	}
	return nil
}

func runtimeRunPath(runID, action string) string {
	return "/agent-runtime/runs/" + url.PathEscape(runID) + "/" + action
}

func runtimeCapacity(capacity, inflight int64) bool {
	return capacity >= 0 && capacity <= RuntimeMaxNodeCapacity && inflight >= 0 && inflight <= RuntimeMaxNodeCapacity
}

func runtimeRejectReason(reason RuntimeAssignmentRejectReason) bool {
	switch reason {
	case RuntimeRejectNodeAtCapacity, RuntimeRejectNodeDraining,
		RuntimeRejectClientUpgradeRequired, RuntimeRejectRequiredFeatureMissing:
		return true
	default:
		return false
	}
}

func runtimeAssignmentRejectOutcome(outcome RuntimeAssignmentRejectOutcome) bool {
	return outcome == RuntimeOfferRejected || outcome == RuntimeAssignmentLeaseRevoked
}

func runtimeResultClassification(classification RuntimeResultClassification) bool {
	switch classification {
	case RuntimeResultSuccess, RuntimeResultRetryableFailure, RuntimeResultNonRetryableFailure,
		RuntimeResultTimeout, RuntimeResultCanceled, RuntimeResultDeadLetter:
		return true
	default:
		return false
	}
}

func runtimeRunStatus(status RuntimeRunStatus) bool {
	switch status {
	case RuntimeRunRunning, RuntimeRunSuccess, RuntimeRunFailed, RuntimeRunTimeout, RuntimeRunCanceled:
		return true
	default:
		return false
	}
}

func runtimeDispatchState(state RuntimeDispatchState) bool {
	switch state {
	case RuntimeDispatchPending, RuntimeDispatchOffered, RuntimeDispatchExecuting,
		RuntimeDispatchRetryWait, RuntimeDispatchTerminal, RuntimeDispatchDeadLetter:
		return true
	default:
		return false
	}
}

func runtimeCancelState(state RuntimeCancelState) bool {
	switch state {
	case RuntimeCancelRequested, RuntimeCancelDelivered, RuntimeCancelStopping, RuntimeCancelStopped,
		RuntimeCancelUnsupported, RuntimeCancelFailed, RuntimeCancelUnconfirmed:
		return true
	default:
		return false
	}
}

func runtimeResumeAction(action RuntimeResumeAction) bool {
	switch action {
	case RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult,
		RuntimeActionStopExecution, RuntimeActionClearSpool:
		return true
	default:
		return false
	}
}

func validRuntimeErrorCode(code string) bool {
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

func runtimeText(value string, maximum int) bool {
	return value != "" && strings.TrimSpace(value) != "" && utf8.ValidString(value) && utf8.RuneCountInString(value) <= maximum
}

func runtimeUUID(value string) bool {
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

func hasRuntimeFeatures(got, required []string) bool {
	seen := make(map[string]struct{}, len(got))
	for _, feature := range got {
		if !runtimeText(feature, 100) {
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
