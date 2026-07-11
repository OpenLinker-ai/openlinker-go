package openlinker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/url"
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
	if rejected.AttemptIdentity != request.AttemptIdentity {
		return nil, errors.New("openlinker: runtime v2 rejection identity mismatch")
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
	if ack.ResultID != request.ResultID || ack.RunStatus == "" || ack.DispatchState == "" || ack.Classification == "" {
		return nil, errors.New("openlinker: runtime v2 result acknowledgement mismatch")
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
	}
	return &response, nil
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
		return response.StatusCode, parseError(response)
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
		value.RunDeadlineAt.IsZero() || value.Input == nil || value.NodeEnvelope == "" || value.AgentInvocationToken == "" {
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
	if !runtimeV2UUID(value.ClientEventID) || value.ClientEventSeq < 1 || !runtimeV2Text(value.EventType, 120) || value.Payload == nil {
		return errors.New("openlinker: invalid runtime v2 Event")
	}
	return nil
}

func validateRuntimeV2Result(value RuntimeV2RunResultPayload) error {
	if err := validateRuntimeV2AttemptIdentity(value.AttemptIdentity); err != nil {
		return err
	}
	if !runtimeV2UUID(value.ResultID) || value.DurationMS < 0 || value.FinalClientEventSeq < 0 {
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
