package openlinker

import (
	"context"
	"errors"
	"net/http"
)

// Runtime is the Agent-side OpenLinker runtime client. It uses an Agent Token
// for registration/runtime protocol calls and should not be used for user API
// calls such as listing Agents or starting user-initiated runs.
type Runtime struct {
	client *Client
}

func NewRuntime(baseURL string, opts ...Option) (*Runtime, error) {
	client, err := newClient(baseURL, true, opts...)
	if err != nil {
		return nil, err
	}
	return &Runtime{client: client}, nil
}

func (r *Runtime) HeartbeatAgent(ctx context.Context) (*AgentHeartbeatResponse, error) {
	return r.client.HeartbeatAgent(ctx)
}

func (r *Runtime) ClaimRuntimeRun(ctx context.Context, params ClaimRuntimeRunParams) (*RuntimePullRunResponse, error) {
	return r.client.ClaimRuntimeRun(ctx, params)
}

func (r *Runtime) ClaimRuntimeRunDetailed(ctx context.Context, params ClaimRuntimeRunParams) (*ClaimRuntimeRunResult, error) {
	return r.client.ClaimRuntimeRunDetailed(ctx, params)
}

func (r *Runtime) CompleteRuntimeRun(ctx context.Context, runID string, result RuntimePullResultRequest) (*RunResponse, error) {
	return r.client.CompleteRuntimeRun(ctx, runID, result)
}

func (r *Runtime) CallAgent(ctx context.Context, req CallAgentRequest) (*RunResponse, error) {
	return r.client.CallAgent(ctx, req)
}

func (r *Runtime) CallAgentAt(ctx context.Context, endpoint string, req CallAgentRequest) (*RunResponse, error) {
	return r.client.CallAgentAt(ctx, endpoint, req)
}

func (r *Runtime) runtimeAuthToken() string {
	if r == nil || r.client == nil {
		return ""
	}
	return r.client.runtimeAuthToken()
}

func (r *Runtime) runtimeWebSocketHeaders() http.Header {
	if r == nil || r.client == nil {
		return nil
	}
	return r.client.runtimeWebSocketHeaders()
}

func (r *Runtime) webSocketEndpoint(path string) (string, error) {
	if r == nil || r.client == nil {
		return "", errors.New("openlinker: runtime client is nil")
	}
	return r.client.webSocketEndpoint(path)
}
