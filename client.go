package openlinker

import (
	"bufio"
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
)

const defaultSDKAgent = "openlinker-go/0.0.0"

type Client struct {
	baseURL      *url.URL
	httpClient   *http.Client
	accessToken  string
	runtimeToken string
	sdkAgent     string
	headers      http.Header
}

type Option func(*Client)

func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		if client != nil {
			c.httpClient = client
		}
	}
}

func WithAccessToken(token string) Option {
	return func(c *Client) {
		c.accessToken = strings.TrimSpace(token)
	}
}

func WithRuntimeToken(token string) Option {
	return func(c *Client) {
		c.runtimeToken = strings.TrimSpace(token)
	}
}

func WithSDKAgent(agent string) Option {
	return func(c *Client) {
		if strings.TrimSpace(agent) != "" {
			c.sdkAgent = strings.TrimSpace(agent)
		}
	}
}

func WithHeader(name, value string) Option {
	return func(c *Client) {
		if strings.TrimSpace(name) != "" {
			c.headers.Set(name, value)
		}
	}
}

func NewClient(baseURL string, opts ...Option) (*Client, error) {
	normalized := normalizeBaseURL(baseURL)
	if normalized == "" {
		return nil, errors.New("openlinker: base URL is required")
	}
	parsed, err := url.Parse(normalized)
	if err != nil {
		return nil, fmt.Errorf("openlinker: parse base URL: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, errors.New("openlinker: base URL must include scheme and host")
	}

	client := &Client{
		baseURL:    parsed,
		httpClient: http.DefaultClient,
		sdkAgent:   defaultSDKAgent,
		headers:    make(http.Header),
	}
	for _, opt := range opts {
		opt(client)
	}
	return client, nil
}

func (c *Client) ListAgents(ctx context.Context, params ListAgentsParams) (*MarketListResponse, error) {
	query := make(url.Values)
	setQuery(query, "q", params.Query)
	setQueryInt(query, "page", params.Page)
	setQueryInt(query, "size", params.Size)
	if params.CallableOnly {
		query.Set("callable_only", "true")
	}
	if len(params.Tags) > 0 {
		query.Set("tags", strings.Join(params.Tags, ","))
	}

	var out MarketListResponse
	if err := c.do(ctx, http.MethodGet, "/agents", query, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) GetAgent(ctx context.Context, slug string) (*AgentDetailResponse, error) {
	var out AgentDetailResponse
	if err := c.do(ctx, http.MethodGet, "/agents/"+url.PathEscape(slug), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) GetAgentCard(ctx context.Context, slug string, extended bool) (*AgentCardResponse, error) {
	suffix := "agent-card.json"
	if extended {
		suffix = "agent-card.extended.json"
	}
	var out AgentCardResponse
	path := "/agents/" + url.PathEscape(slug) + "/" + suffix
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) RunAgent(ctx context.Context, req RunAgentRequest) (*RunResponse, error) {
	var out RunResponse
	if err := c.do(ctx, http.MethodPost, "/run", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) StartAgentRun(ctx context.Context, req RunAgentRequest) (*RunResponse, error) {
	var out RunResponse
	if err := c.do(ctx, http.MethodPost, "/runs", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) GetRun(ctx context.Context, runID string) (*RunResponse, error) {
	var out RunResponse
	if err := c.do(ctx, http.MethodGet, "/runs/"+url.PathEscape(runID), nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ListRunEvents(ctx context.Context, runID string, params ListRunEventsParams) (*ListRunEventsResponse, error) {
	query := make(url.Values)
	setQueryInt32(query, "after_sequence", params.AfterSequence)
	setQueryInt32(query, "limit", params.Limit)

	var out ListRunEventsResponse
	path := "/runs/" + url.PathEscape(runID) + "/events"
	if err := c.do(ctx, http.MethodGet, path, query, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ListRunArtifacts(ctx context.Context, runID string) (*ListItemsResponse[RunArtifactResponse], error) {
	var out ListItemsResponse[RunArtifactResponse]
	path := "/runs/" + url.PathEscape(runID) + "/artifacts"
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ListRunMessages(ctx context.Context, runID string) (*ListItemsResponse[RunMessageResponse], error) {
	var out ListItemsResponse[RunMessageResponse]
	path := "/runs/" + url.PathEscape(runID) + "/messages"
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) StreamRunEvents(ctx context.Context, runID string, opts StreamRunEventsOptions, handle func(StreamRunEvent) error) error {
	query := make(url.Values)
	setQueryInt32(query, "after_sequence", opts.AfterSequence)

	resp, err := c.newRequest(ctx, http.MethodGet, "/runs/"+url.PathEscape(runID)+"/stream", query, nil, "text/event-stream")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseError(resp)
	}
	return readSSE(resp.Body, handle)
}

func (c *Client) HeartbeatAgent(ctx context.Context) (*AgentHeartbeatResponse, error) {
	var out AgentHeartbeatResponse
	if err := c.doRuntime(ctx, http.MethodPost, "/agent-runtime/heartbeat", nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ClaimRuntimeRun(ctx context.Context, params ClaimRuntimeRunParams) (*RuntimePullRunResponse, error) {
	query := make(url.Values)
	setQueryInt32(query, "wait", params.WaitSeconds)

	resp, err := c.newRuntimeRequest(ctx, http.MethodGet, "/agent-runtime/runs/claim", query, nil, "application/json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return nil, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, parseError(resp)
	}
	var out RuntimePullRunResponse
	if err := json.NewDecoder(resp.Body).Decode(&out); err != nil {
		return nil, fmt.Errorf("openlinker: decode response: %w", err)
	}
	return &out, nil
}

func (c *Client) CompleteRuntimeRun(ctx context.Context, runID string, result RuntimePullResultRequest) (*RunResponse, error) {
	var out RunResponse
	path := "/agent-runtime/runs/" + url.PathEscape(runID) + "/result"
	if err := c.doRuntime(ctx, http.MethodPost, path, nil, result, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) CallAgent(ctx context.Context, req CallAgentRequest) (*RunResponse, error) {
	var out RunResponse
	if err := c.doRuntime(ctx, http.MethodPost, "/agent-runtime/call-agent", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) do(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	resp, err := c.newRequest(ctx, method, path, query, body, "application/json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseError(resp)
	}
	if resp.StatusCode == http.StatusNoContent || out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("openlinker: decode response: %w", err)
	}
	return nil
}

func (c *Client) doRuntime(ctx context.Context, method, path string, query url.Values, body any, out any) error {
	resp, err := c.newRuntimeRequest(ctx, method, path, query, body, "application/json")
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseError(resp)
	}
	if resp.StatusCode == http.StatusNoContent || out == nil {
		return nil
	}
	if err := json.NewDecoder(resp.Body).Decode(out); err != nil {
		return fmt.Errorf("openlinker: decode response: %w", err)
	}
	return nil
}

func (c *Client) newRequest(ctx context.Context, method, path string, query url.Values, body any, accept string) (*http.Response, error) {
	return c.newRequestWithToken(ctx, method, path, query, body, accept, c.accessToken)
}

func (c *Client) newRuntimeRequest(ctx context.Context, method, path string, query url.Values, body any, accept string) (*http.Response, error) {
	token := c.runtimeToken
	if token == "" {
		token = c.accessToken
	}
	return c.newRequestWithToken(ctx, method, path, query, body, accept, token)
}

func (c *Client) newRequestWithToken(ctx context.Context, method, path string, query url.Values, body any, accept, token string) (*http.Response, error) {
	var bodyReader io.Reader
	if body != nil {
		raw, err := json.Marshal(body)
		if err != nil {
			return nil, fmt.Errorf("openlinker: encode request: %w", err)
		}
		bodyReader = bytes.NewReader(raw)
	}

	req, err := http.NewRequestWithContext(ctx, method, c.endpoint(path, query), bodyReader)
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("X-OpenLinker-SDK", c.sdkAgent)
	if body != nil {
		req.Header.Set("Content-Type", "application/json")
	}
	if token != "" {
		req.Header.Set("Authorization", "Bearer "+token)
	}
	for key, values := range c.headers {
		for _, value := range values {
			req.Header.Add(key, value)
		}
	}
	return c.httpClient.Do(req)
}

func (c *Client) endpoint(path string, query url.Values) string {
	u := *c.baseURL
	u.Path = strings.TrimRight(u.Path, "/") + "/api/v1/" + strings.TrimLeft(path, "/")
	u.RawQuery = query.Encode()
	return u.String()
}

func parseError(resp *http.Response) error {
	raw, _ := io.ReadAll(resp.Body)
	var parsed errorResponse
	_ = json.Unmarshal(raw, &parsed)

	code := parsed.Error.Code
	if code == "" {
		code = fmt.Sprintf("HTTP_%d", resp.StatusCode)
	}
	message := parsed.Error.Message
	if message == "" {
		message = resp.Status
	}
	return &Error{
		StatusCode:   resp.StatusCode,
		Code:         code,
		Message:      message,
		Details:      parsed.Error.Details,
		RequestID:    firstHeader(resp.Header, "X-Request-Id", "X-Correlation-Id"),
		ResponseBody: raw,
	}
}

func normalizeBaseURL(raw string) string {
	normalized := strings.TrimRight(strings.TrimSpace(raw), "/")
	return strings.TrimSuffix(normalized, "/api/v1")
}

func setQuery(query url.Values, key, value string) {
	if strings.TrimSpace(value) != "" {
		query.Set(key, value)
	}
}

func setQueryInt(query url.Values, key string, value int) {
	if value > 0 {
		query.Set(key, strconv.Itoa(value))
	}
}

func setQueryInt32(query url.Values, key string, value int32) {
	if value > 0 {
		query.Set(key, strconv.FormatInt(int64(value), 10))
	}
}

func firstHeader(headers http.Header, names ...string) string {
	for _, name := range names {
		if value := headers.Get(name); value != "" {
			return value
		}
	}
	return ""
}

func readSSE(reader io.Reader, handle func(StreamRunEvent) error) error {
	scanner := bufio.NewScanner(reader)
	scanner.Buffer(make([]byte, 0, 64*1024), 1024*1024)
	event := StreamRunEvent{Event: "message"}
	var data []string

	dispatch := func() error {
		if len(data) == 0 {
			event = StreamRunEvent{Event: "message"}
			return nil
		}
		event.Data = []byte(strings.Join(data, "\n"))
		if handle != nil {
			if err := handle(event); err != nil {
				return err
			}
		}
		event = StreamRunEvent{Event: "message"}
		data = nil
		return nil
	}

	for scanner.Scan() {
		line := strings.TrimSuffix(scanner.Text(), "\r")
		if line == "" {
			if err := dispatch(); err != nil {
				return err
			}
			continue
		}
		if strings.HasPrefix(line, ":") {
			continue
		}
		field, value, found := strings.Cut(line, ":")
		if found {
			value = strings.TrimPrefix(value, " ")
		}
		switch field {
		case "event":
			if value == "" {
				event.Event = "message"
			} else {
				event.Event = value
			}
		case "id":
			event.ID = value
		case "data":
			data = append(data, value)
		}
	}
	if err := scanner.Err(); err != nil {
		return err
	}
	return dispatch()
}
