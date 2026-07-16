package openlinker

import (
	"context"
	"errors"
	"net/http"
	"net/url"
	"strings"
)

func (c *Client) CreateAgent(ctx context.Context, req CreateAgentRequest) (*AgentResponse, error) {
	var out AgentResponse
	if err := c.do(ctx, http.MethodPost, "/creator/agents", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ListMyAgents(ctx context.Context, params ListMyAgentsParams) (*AgentListResponse, error) {
	query := make(url.Values)
	setQuery(query, "q", params.Query)
	setQuery(query, "status", params.Status)
	setQuery(query, "visibility", params.Visibility)
	setQuery(query, "certification_status", params.CertificationStatus)
	setQuery(query, "sort_by", params.SortBy)
	if len(params.SkillIDs) > 0 {
		query.Set("skill_ids", strings.Join(params.SkillIDs, ","))
	}
	setQueryInt32(query, "limit", params.Limit)
	setQueryInt32(query, "offset", params.Offset)
	var out AgentListResponse
	if err := c.do(ctx, http.MethodGet, "/creator/agents", query, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) GetMyAgent(ctx context.Context, agentID string) (*AgentResponse, error) {
	var out AgentResponse
	if err := c.do(ctx, http.MethodGet, "/creator/agents/"+url.PathEscape(agentID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) GetMyAgentBySlug(ctx context.Context, slug string) (*AgentResponse, error) {
	var out AgentResponse
	if err := c.do(ctx, http.MethodGet, "/creator/agents/by-slug/"+url.PathEscape(slug), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) UpdateAgent(ctx context.Context, agentID string, req UpdateAgentRequest) (*AgentResponse, error) {
	var out AgentResponse
	if err := c.do(ctx, http.MethodPatch, "/creator/agents/"+url.PathEscape(agentID), nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) CreateAgentToken(ctx context.Context, req CreateAgentTokenRequest) (*AgentTokenResponse, error) {
	var out AgentTokenResponse
	if err := c.do(ctx, http.MethodPost, "/creator/agent-tokens", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ListAgentTokens(ctx context.Context, params ListAgentTokensParams) (*AgentTokenListResponse, error) {
	query := make(url.Values)
	setQuery(query, "agent_id", params.AgentID)
	setQuery(query, "sort_by", params.SortBy)
	setQuery(query, "sort_dir", params.SortDir)
	setQueryInt32(query, "limit", params.Limit)
	setQueryInt32(query, "offset", params.Offset)
	var out AgentTokenListResponse
	if err := c.do(ctx, http.MethodGet, "/creator/agent-tokens", query, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) RevokeAgentToken(ctx context.Context, tokenID string) error {
	return c.do(ctx, http.MethodDelete, "/creator/agent-tokens/"+url.PathEscape(tokenID), nil, nil, nil)
}

// RegisterAgentViaToken redeems a pending Agent Token. Registration uses the
// public API origin, not the dedicated mTLS Runtime origin.
func RegisterAgentViaToken(
	ctx context.Context,
	baseURL string,
	agentToken string,
	req RegisterAgentViaTokenRequest,
	opts ...Option,
) (*RegisterAgentViaTokenResponse, error) {
	agentToken = strings.TrimSpace(agentToken)
	if agentToken == "" {
		return nil, errors.New("openlinker: Agent Token is required for registration")
	}
	client, err := newClient(baseURL, false, opts...)
	if err != nil {
		return nil, err
	}
	return client.registerAgentViaToken(ctx, agentToken, req)
}

func (c *Client) registerAgentViaToken(ctx context.Context, agentToken string, req RegisterAgentViaTokenRequest) (*RegisterAgentViaTokenResponse, error) {
	if c == nil {
		return nil, errors.New("openlinker: client is nil")
	}
	agentToken = strings.TrimSpace(agentToken)
	if agentToken == "" {
		return nil, errors.New("openlinker: Agent Token is required for registration")
	}
	if req.Visibility == "" {
		req.Visibility = "private"
	}
	req.ConnectionMode = normalizeRegistrationConnectionMode(req.ConnectionMode)
	var out RegisterAgentViaTokenResponse
	response, err := c.newRequestWithToken(ctx, http.MethodPost, "/agent-registration/agents", nil, req, "application/json", agentToken)
	if err != nil {
		return nil, err
	}
	defer response.Body.Close()
	if response.StatusCode < 200 || response.StatusCode >= 300 {
		return nil, parseError(response)
	}
	if err := decodeJSONResponse(response.Body, &out); err != nil {
		return nil, err
	}
	return &out, nil
}
