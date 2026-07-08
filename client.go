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
	"time"
)

const (
	defaultSDKAgent      = "openlinker-go/0.1.3"
	maxResponseBodyBytes = int64(4 << 20)
)

type Client struct {
	baseURL    *url.URL
	httpClient *http.Client
	userToken  string
	agentToken string
	sdkAgent   string
	headers    http.Header
	runtime    bool
}

type Option func(*Client)

func WithHTTPClient(client *http.Client) Option {
	return func(c *Client) {
		if client != nil {
			c.httpClient = client
		}
	}
}

func WithUserToken(token string) Option {
	return func(c *Client) {
		c.userToken = strings.TrimSpace(token)
	}
}

func WithAgentToken(token string) Option {
	return func(c *Client) {
		c.agentToken = strings.TrimSpace(token)
	}
}

func WithRuntimeToken(token string) Option {
	return WithAgentToken(token)
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
	return newClient(baseURL, false, opts...)
}

func newClient(baseURL string, runtime bool, opts ...Option) (*Client, error) {
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
		runtime:    runtime,
	}
	for _, opt := range opts {
		opt(client)
	}
	if !runtime && client.agentToken != "" {
		return nil, errors.New("openlinker: client does not accept agent token; use NewRuntime")
	}
	if runtime {
		if client.userToken != "" {
			return nil, errors.New("openlinker: runtime does not accept user token")
		}
		if client.agentToken == "" {
			return nil, errors.New("openlinker: runtime requires agent token")
		}
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

func (c *Client) RunAgentWithCallbacks(ctx context.Context, req RunAgentRequest, opts PlatformCallbackOptions) (*RunResponse, error) {
	started, err := c.StartAgentRun(ctx, req)
	if err != nil {
		return nil, err
	}
	if _, err := c.streamPlatformCallbacks(ctx, started.RunID, opts, true); err != nil {
		return nil, err
	}
	return c.GetRun(ctx, started.RunID)
}

func (c *Client) StartAgentRun(ctx context.Context, req RunAgentRequest) (*RunResponse, error) {
	var out RunResponse
	if err := c.do(ctx, http.MethodPost, "/runs", nil, req, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) StartAgentRunWithCallbacks(ctx context.Context, req RunAgentRequest, opts PlatformCallbackOptions) (*RunResponse, error) {
	started, err := c.StartAgentRun(ctx, req)
	if err != nil {
		return nil, err
	}
	go func(runID string) {
		if _, streamErr := c.streamPlatformCallbacks(ctx, runID, opts, false); streamErr != nil && opts.OnError != nil {
			opts.OnError(streamErr)
		}
	}(started.RunID)
	return started, nil
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

func (c *Client) ListRunChildren(ctx context.Context, runID string) (*ListRunChildrenResponse, error) {
	var out ListRunChildrenResponse
	path := "/runs/" + url.PathEscape(runID) + "/children"
	if err := c.do(ctx, http.MethodGet, path, nil, nil, &out); err != nil {
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

func (c *Client) streamPlatformCallbacks(ctx context.Context, runID string, opts PlatformCallbackOptions, untilTerminal bool) (*StreamRunEvent, error) {
	streamCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var terminal *StreamRunEvent
	err := c.StreamRunEvents(streamCtx, runID, StreamRunEventsOptions{AfterSequence: opts.AfterSequence}, func(event StreamRunEvent) error {
		if matchesPlatformCallbackEvent(opts.EventTypes, event.Event) && opts.OnEvent != nil {
			if err := opts.OnEvent(event); err != nil {
				return err
			}
		}
		if isTerminalRunEvent(event.Event) {
			copyEvent := event
			terminal = &copyEvent
			if opts.OnTerminal != nil {
				if err := opts.OnTerminal(event); err != nil {
					return err
				}
			}
			if untilTerminal {
				cancel()
			}
		}
		return nil
	})
	if err != nil && !(untilTerminal && terminal != nil && errors.Is(err, context.Canceled)) {
		return terminal, err
	}
	if opts.OnClose != nil {
		if err := opts.OnClose(); err != nil {
			return terminal, err
		}
	}
	return terminal, nil
}

func (c *Client) HeartbeatAgent(ctx context.Context) (*AgentHeartbeatResponse, error) {
	var out AgentHeartbeatResponse
	if err := c.doRuntime(ctx, http.MethodPost, "/agent-runtime/heartbeat", nil, nil, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *Client) ClaimRuntimeRun(ctx context.Context, params ClaimRuntimeRunParams) (*RuntimePullRunResponse, error) {
	result, err := c.ClaimRuntimeRunDetailed(ctx, params)
	if err != nil || result == nil {
		return nil, err
	}
	return result.Run, nil
}

type ClaimRuntimeRunResult struct {
	Run                 *RuntimePullRunResponse
	RetryAfter          time.Duration
	MaxClaimWaitSeconds int32
}

func (c *Client) ClaimRuntimeRunDetailed(ctx context.Context, params ClaimRuntimeRunParams) (*ClaimRuntimeRunResult, error) {
	query := make(url.Values)
	setQueryInt32(query, "wait", params.WaitSeconds)

	resp, err := c.newRuntimeRequest(ctx, http.MethodGet, "/agent-runtime/runs/claim", query, nil, "application/json")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode == http.StatusNoContent {
		return &ClaimRuntimeRunResult{
			RetryAfter:          retryAfter(resp.Header),
			MaxClaimWaitSeconds: headerInt32(resp.Header, "X-OpenLinker-Max-Claim-Wait-Seconds"),
		}, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, parseError(resp)
	}
	var out RuntimePullRunResponse
	if err := decodeJSONResponse(resp.Body, &out); err != nil {
		return nil, fmt.Errorf("openlinker: decode response: %w", err)
	}
	return &ClaimRuntimeRunResult{Run: &out}, nil
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
	return c.CallAgentAt(ctx, "", req)
}

func (c *Client) CallAgentAt(ctx context.Context, endpoint string, req CallAgentRequest) (*RunResponse, error) {
	var out RunResponse
	if strings.TrimSpace(endpoint) == "" {
		endpoint = "/agent-runtime/call-agent"
	}
	if err := c.doRuntime(ctx, http.MethodPost, endpoint, nil, req, &out); err != nil {
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
	if err := decodeJSONResponse(resp.Body, out); err != nil {
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
	if err := decodeJSONResponse(resp.Body, out); err != nil {
		return fmt.Errorf("openlinker: decode response: %w", err)
	}
	return nil
}

func (c *Client) newRequest(ctx context.Context, method, path string, query url.Values, body any, accept string) (*http.Response, error) {
	return c.newRequestWithToken(ctx, method, path, query, body, accept, c.userToken)
}

func (c *Client) newRuntimeRequest(ctx context.Context, method, path string, query url.Values, body any, accept string) (*http.Response, error) {
	if err := c.requireRuntime(); err != nil {
		return nil, err
	}
	return c.newRequestWithToken(ctx, method, path, query, body, accept, c.agentToken)
}

func (c *Client) requireRuntime() error {
	if c == nil {
		return errors.New("openlinker: runtime client is nil")
	}
	if !c.runtime {
		return errors.New("openlinker: client cannot call agent runtime endpoints; use NewRuntime")
	}
	if c.agentToken == "" {
		return errors.New("openlinker: runtime requires agent token")
	}
	return nil
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
	if isAbsoluteURL(path) {
		u, err := url.Parse(path)
		if err == nil {
			u.RawQuery = query.Encode()
			return u.String()
		}
	}
	u := *c.baseURL
	normalizedPath := strings.TrimLeft(path, "/")
	normalizedPath = strings.TrimPrefix(normalizedPath, "api/v1/")
	u.Path = strings.TrimRight(u.Path, "/") + "/api/v1/" + normalizedPath
	u.RawQuery = query.Encode()
	return u.String()
}

func parseError(resp *http.Response) error {
	raw := readLimitedErrorResponseBody(resp.Body)
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
		RetryAfter:   retryAfter(resp.Header),
		ResponseBody: raw,
	}
}

func decodeJSONResponse(body io.Reader, out any) error {
	raw, err := readLimitedResponseBody(body)
	if err != nil {
		return err
	}
	if len(raw) == 0 {
		return nil
	}
	if target, ok := out.(*json.RawMessage); ok {
		*target = append((*target)[:0], raw...)
		return nil
	}
	return json.Unmarshal(raw, out)
}

func readLimitedResponseBody(body io.Reader) ([]byte, error) {
	raw, err := io.ReadAll(io.LimitReader(body, maxResponseBodyBytes+1))
	if err != nil {
		return nil, err
	}
	if int64(len(raw)) > maxResponseBodyBytes {
		return nil, fmt.Errorf("openlinker: response body exceeds %d bytes", maxResponseBodyBytes)
	}
	return raw, nil
}

func readLimitedErrorResponseBody(body io.Reader) []byte {
	raw, err := io.ReadAll(io.LimitReader(body, maxResponseBodyBytes+1))
	if err != nil {
		return nil
	}
	if int64(len(raw)) > maxResponseBodyBytes {
		return raw[:maxResponseBodyBytes]
	}
	return raw
}

func isAbsoluteURL(raw string) bool {
	return strings.HasPrefix(raw, "http://") || strings.HasPrefix(raw, "https://")
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

func retryAfter(headers http.Header) time.Duration {
	value := headers.Get("Retry-After")
	if value == "" {
		return 0
	}
	if seconds, err := strconv.Atoi(value); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	if retryAt, err := http.ParseTime(value); err == nil {
		if d := time.Until(retryAt); d > 0 {
			return d
		}
	}
	return 0
}

func headerInt32(headers http.Header, name string) int32 {
	value := headers.Get(name)
	if value == "" {
		return 0
	}
	parsed, err := strconv.ParseInt(value, 10, 32)
	if err != nil || parsed <= 0 {
		return 0
	}
	return int32(parsed)
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

func matchesPlatformCallbackEvent(eventTypes []string, eventType string) bool {
	if len(eventTypes) == 0 {
		return true
	}
	for _, allowed := range eventTypes {
		if allowed == eventType {
			return true
		}
	}
	return false
}

func isTerminalRunEvent(eventType string) bool {
	return eventType == "run.completed" || eventType == "run.failed" || eventType == "run.canceled"
}
