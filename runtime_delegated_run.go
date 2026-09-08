package openlinker

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"strings"
)

const RuntimeDelegatedRunReadFeature = "delegated_run_read.v1"
const runtimeDelegatedRunReadPath = "/api/v1/agent-runtime/delegated-runs/read"

var ErrRuntimeDelegationUnsupported = errors.New("openlinker: Core/SDK did not negotiate delegated Run results")

type RuntimeDelegatedRun struct {
	RuntimeRunSummary
	Output       map[string]any `json:"output,omitempty"`
	ErrorCode    string         `json:"error_code,omitempty"`
	ErrorMessage string         `json:"error_message,omitempty"`
}

type runtimeDelegatedRunClient interface {
	ReadRuntimeDelegatedRun(context.Context, RuntimeCallAgentAuthorization, string) (*RuntimeDelegatedRun, error)
}

func (r *Runtime) ReadRuntimeDelegatedRun(ctx context.Context, authorization RuntimeCallAgentAuthorization, runID string) (*RuntimeDelegatedRun, error) {
	if r == nil || r.client == nil {
		return nil, errors.New("openlinker: runtime client is nil")
	}
	if err := r.client.requireRuntime(); err != nil {
		return nil, err
	}
	if err := validateRuntimeCallAgentAuthorization(authorization); err != nil {
		return nil, err
	}
	if !runtimeUUID(runID) {
		return nil, errors.New("openlinker: invalid delegated Run ID")
	}
	if !runtimeDelegationReadAdvertised(authorization.AgentInvocationToken) {
		return nil, ErrRuntimeDelegationUnsupported
	}
	body, err := json.Marshal(struct {
		RunID string `json:"run_id"`
	}{RunID: runID})
	if err != nil {
		return nil, err
	}
	proof, err := BuildRuntimeInvocationProof(authorization.AgentInvocationToken, RuntimeInvocationProofRequest{
		Method: http.MethodPost, Path: runtimeDelegatedRunReadPath, Body: body,
		Context: authorization.NodeEnvelope, IdempotencyKey: authorization.IdempotencyKey,
	})
	if err != nil {
		return nil, err
	}
	headers := make(http.Header)
	headers.Set("Idempotency-Key", authorization.IdempotencyKey)
	headers.Set(runtimeInvocationHeader, authorization.NodeEnvelope)
	headers.Set(runtimeInvocationProofHeader, proof)
	response, err := r.client.newRequestWithTokenAndHeadersBytes(ctx, http.MethodPost, runtimeDelegatedRunReadPath, nil, body, "application/json", authorization.AgentInvocationToken, headers)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, parseRuntimeError(response)
	}
	var result RuntimeDelegatedRun
	if err := decodeRuntimeResponse(response.Body, &result); err != nil {
		return nil, fmt.Errorf("openlinker: decode delegated Run: %w", err)
	}
	if err := validateRuntimeRunSummary(result.RuntimeRunSummary); err != nil {
		return nil, err
	}
	if result.RunID != runID {
		return nil, fmt.Errorf("%w: delegated Run ID mismatch", ErrRuntimeProtocolMismatch)
	}
	return &result, nil
}

// This is only feature detection on Core's opaque assignment. Core verifies
// both signatures, the new audience, and live Attempt/child ownership on read.
func runtimeDelegationReadAdvertised(token string) bool {
	if !runtimeInvocationCapability(token, "ol_inv_v2.") {
		return false
	}
	payload, err := base64.RawURLEncoding.DecodeString(strings.Split(token, ".")[2])
	if err != nil {
		return false
	}
	var claim struct {
		Audience string `json:"audience"`
	}
	return json.Unmarshal(payload, &claim) == nil && claim.Audience == "openlinker.runtime.v2/delegation"
}

func (runtime RuntimeContext) CanReadDelegatedRuns() bool { return runtime.readDelegatedRun != nil }

func (runtime RuntimeContext) ReadDelegatedRun(ctx context.Context, runID string) (*RuntimeDelegatedRun, error) {
	if runtime.readDelegatedRun == nil {
		return nil, ErrRuntimeDelegationUnsupported
	}
	return runtime.readDelegatedRun(ctx, runID)
}

func (node *RuntimeWorker) readDelegatedRunForAttempt(caller context.Context, attempt *activeRuntimeAttempt, runID string) (*RuntimeDelegatedRun, error) {
	if attempt.finished.Load() || attempt.canceled.Load() || attempt.ctx.Err() != nil {
		return nil, context.Canceled
	}
	client, ok := node.runtimeClient.(runtimeDelegatedRunClient)
	if !ok {
		return nil, ErrRuntimeDelegationUnsupported
	}
	if caller == nil {
		caller = context.Background()
	}
	ctx, cancel := context.WithCancel(caller)
	stop := context.AfterFunc(attempt.ctx, cancel)
	defer func() { stop(); cancel() }()
	return client.ReadRuntimeDelegatedRun(ctx, RuntimeCallAgentAuthorization{
		NodeEnvelope: attempt.payload.NodeEnvelope, AgentInvocationToken: attempt.payload.AgentInvocationToken,
		IdempotencyKey: "read-delegated-" + runID,
	}, runID)
}
