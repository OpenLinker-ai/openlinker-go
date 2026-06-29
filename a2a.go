package openlinker

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

const (
	A2AMethodMessageSend                = "message/send"
	A2AMethodMessageStream              = "message/stream"
	A2AMethodTasksGet                   = "tasks/get"
	A2AMethodTasksList                  = "tasks/list"
	A2AMethodTasksCancel                = "tasks/cancel"
	A2AMethodTasksResubscribe           = "tasks/resubscribe"
	A2AMethodTaskPushNotificationSet    = "tasks/pushNotificationConfig/set"
	A2AMethodTaskPushNotificationGet    = "tasks/pushNotificationConfig/get"
	A2AMethodTaskPushNotificationList   = "tasks/pushNotificationConfig/list"
	A2AMethodTaskPushNotificationDelete = "tasks/pushNotificationConfig/delete"
	A2AMethodAgentGetExtendedCard       = "agent/getExtendedCard"
	defaultA2AProtocolVersion           = "1.0"
	defaultA2AJSONRPCIDPrefix           = "openlinker-a2a"
)

type A2AClient struct {
	Endpoint        string
	Token           string
	Headers         http.Header
	HTTPClient      *http.Client
	ProtocolVersion string
	SDKAgent        string
}

type A2AClientOption func(*A2AClient)

func NewA2AClient(endpoint string, opts ...A2AClientOption) (*A2AClient, error) {
	endpoint = strings.TrimSpace(endpoint)
	if endpoint == "" {
		return nil, fmt.Errorf("openlinker: A2A endpoint is required")
	}
	parsed, err := url.Parse(endpoint)
	if err != nil {
		return nil, fmt.Errorf("openlinker: parse A2A endpoint: %w", err)
	}
	if parsed.Scheme == "" || parsed.Host == "" {
		return nil, fmt.Errorf("openlinker: A2A endpoint must include scheme and host")
	}
	client := &A2AClient{
		Endpoint:        endpoint,
		Headers:         make(http.Header),
		HTTPClient:      http.DefaultClient,
		ProtocolVersion: defaultA2AProtocolVersion,
		SDKAgent:        defaultSDKAgent,
	}
	for _, opt := range opts {
		opt(client)
	}
	if client.HTTPClient == nil {
		client.HTTPClient = http.DefaultClient
	}
	if client.Headers == nil {
		client.Headers = make(http.Header)
	}
	return client, nil
}

func WithA2AHTTPClient(httpClient *http.Client) A2AClientOption {
	return func(c *A2AClient) {
		if httpClient != nil {
			c.HTTPClient = httpClient
		}
	}
}

func WithA2AToken(token string) A2AClientOption {
	return func(c *A2AClient) {
		c.Token = strings.TrimSpace(token)
	}
}

func WithA2AHeader(name, value string) A2AClientOption {
	return func(c *A2AClient) {
		if strings.TrimSpace(name) != "" {
			c.Headers.Set(name, value)
		}
	}
}

func WithA2AHeaders(headers map[string]string) A2AClientOption {
	return func(c *A2AClient) {
		for key, value := range headers {
			if strings.TrimSpace(key) != "" {
				c.Headers.Set(key, value)
			}
		}
	}
}

func WithA2AProtocolVersion(version string) A2AClientOption {
	return func(c *A2AClient) {
		c.ProtocolVersion = strings.TrimSpace(version)
	}
}

func WithA2ASDKAgent(agent string) A2AClientOption {
	return func(c *A2AClient) {
		if strings.TrimSpace(agent) != "" {
			c.SDKAgent = strings.TrimSpace(agent)
		}
	}
}

func (c *Client) A2AAgent(slug string) (*A2AClient, error) {
	endpoint := c.endpoint("/a2a/agents/"+url.PathEscape(slug), nil)
	headers := map[string]string{}
	for key, values := range c.headers {
		if len(values) > 0 {
			headers[key] = values[len(values)-1]
		}
	}
	return NewA2AClient(
		endpoint,
		WithA2AHTTPClient(c.httpClient),
		WithA2AToken(c.accessToken),
		WithA2AHeaders(headers),
		WithA2ASDKAgent(c.sdkAgent),
	)
}

type A2AJSONRPCRequest struct {
	JSONRPC string `json:"jsonrpc"`
	ID      any    `json:"id,omitempty"`
	Method  string `json:"method"`
	Params  any    `json:"params,omitempty"`
}

type A2AJSONRPCResponse struct {
	JSONRPC string           `json:"jsonrpc,omitempty"`
	ID      any              `json:"id,omitempty"`
	Result  json.RawMessage  `json:"result,omitempty"`
	Error   *A2AJSONRPCError `json:"error,omitempty"`
}

type A2AJSONRPCError struct {
	Code    any    `json:"code"`
	Message string `json:"message"`
	Data    any    `json:"data,omitempty"`
}

func (e *A2AJSONRPCError) Error() string {
	if e == nil {
		return "openlinker: A2A JSON-RPC error"
	}
	if e.Message == "" {
		return fmt.Sprintf("openlinker: A2A JSON-RPC error: %v", e.Code)
	}
	return fmt.Sprintf("openlinker: A2A JSON-RPC error %v: %s", e.Code, e.Message)
}

type A2AMessageSendParams struct {
	Message       A2AMessage            `json:"message"`
	Configuration *A2ASendConfiguration `json:"configuration,omitempty"`
	Metadata      map[string]any        `json:"metadata,omitempty"`
}

type A2ASendConfiguration struct {
	AcceptedOutputModes        []string                       `json:"acceptedOutputModes,omitempty"`
	Blocking                   *bool                          `json:"blocking,omitempty"`
	PushNotificationConfig     *A2APushNotificationConfig     `json:"pushNotificationConfig,omitempty"`
	TaskPushNotificationConfig *A2ATaskPushNotificationConfig `json:"taskPushNotificationConfig,omitempty"`
	HistoryLength              *int                           `json:"historyLength,omitempty"`
}

type A2APushNotificationConfig struct {
	ID              string                     `json:"id,omitempty"`
	URL             string                     `json:"url,omitempty"`
	Token           string                     `json:"token,omitempty"`
	Secret          string                     `json:"secret,omitempty"`
	Authentication  *A2APushAuthenticationInfo `json:"authentication,omitempty"`
	Metadata        map[string]any             `json:"metadata,omitempty"`
	EventTypes      []string                   `json:"eventTypes,omitempty"`
	EventTypesAlias []string                   `json:"event_types,omitempty"`
}

type A2APushAuthenticationInfo struct {
	Scheme      string `json:"scheme,omitempty"`
	Credentials string `json:"credentials,omitempty"`
}

type A2ATaskPushNotificationConfig struct {
	TaskID                 string                    `json:"taskId"`
	PushNotificationConfig A2APushNotificationConfig `json:"pushNotificationConfig"`
}

type A2ATaskPushConfigParams struct {
	ID                       string                    `json:"id,omitempty"`
	TaskID                   string                    `json:"taskId,omitempty"`
	PushNotificationConfigID string                    `json:"pushNotificationConfigId,omitempty"`
	PushNotificationConfig   A2APushNotificationConfig `json:"pushNotificationConfig,omitempty"`
}

type A2ATaskPushConfigList struct {
	Items []A2ATaskPushNotificationConfig `json:"items"`
}

type A2ATaskQueryParams struct {
	ID            string `json:"id"`
	HistoryLength *int   `json:"historyLength,omitempty"`
}

type A2ATaskListParams struct {
	ContextID            string `json:"contextId,omitempty"`
	Status               string `json:"status,omitempty"`
	PageSize             *int   `json:"pageSize,omitempty"`
	PageToken            string `json:"pageToken,omitempty"`
	HistoryLength        *int   `json:"historyLength,omitempty"`
	StatusTimestampAfter string `json:"statusTimestampAfter,omitempty"`
	IncludeArtifacts     *bool  `json:"includeArtifacts,omitempty"`
}

type A2ATaskListResponse struct {
	Tasks         []A2ATask `json:"tasks"`
	NextPageToken string    `json:"nextPageToken,omitempty"`
	PageSize      int32     `json:"pageSize,omitempty"`
	TotalSize     int32     `json:"totalSize,omitempty"`
}

type A2AMessage struct {
	Kind      string           `json:"kind,omitempty"`
	MessageID string           `json:"messageId,omitempty"`
	ContextID string           `json:"contextId,omitempty"`
	TaskID    string           `json:"taskId,omitempty"`
	Role      string           `json:"role,omitempty"`
	Parts     []map[string]any `json:"parts,omitempty"`
	Metadata  map[string]any   `json:"metadata,omitempty"`
}

type A2ATask struct {
	Kind      string         `json:"kind,omitempty"`
	ID        string         `json:"id"`
	ContextID string         `json:"contextId,omitempty"`
	Status    A2ATaskStatus  `json:"status"`
	Artifacts []A2AArtifact  `json:"artifacts,omitempty"`
	History   []A2AMessage   `json:"history,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type A2ATaskStatus struct {
	State     string      `json:"state"`
	Timestamp string      `json:"timestamp,omitempty"`
	Message   *A2AMessage `json:"message,omitempty"`
}

type A2AArtifact struct {
	ArtifactID string           `json:"artifactId,omitempty"`
	Name       string           `json:"name,omitempty"`
	Parts      []map[string]any `json:"parts,omitempty"`
	Metadata   map[string]any   `json:"metadata,omitempty"`
}

type A2ATaskStatusUpdateEvent struct {
	Kind      string         `json:"kind,omitempty"`
	TaskID    string         `json:"taskId,omitempty"`
	ContextID string         `json:"contextId,omitempty"`
	Status    A2ATaskStatus  `json:"status"`
	Final     bool           `json:"final,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type A2ATaskArtifactUpdateEvent struct {
	Kind      string         `json:"kind,omitempty"`
	TaskID    string         `json:"taskId,omitempty"`
	ContextID string         `json:"contextId,omitempty"`
	Artifact  A2AArtifact    `json:"artifact"`
	Append    bool           `json:"append,omitempty"`
	LastChunk bool           `json:"lastChunk,omitempty"`
	Metadata  map[string]any `json:"metadata,omitempty"`
}

type A2AStreamResponse struct {
	Task           *A2ATask                    `json:"task,omitempty"`
	Message        *A2AMessage                 `json:"message,omitempty"`
	StatusUpdate   *A2ATaskStatusUpdateEvent   `json:"statusUpdate,omitempty"`
	ArtifactUpdate *A2ATaskArtifactUpdateEvent `json:"artifactUpdate,omitempty"`
}

type A2AStreamEvent struct {
	ID     string
	Event  string
	Raw    []byte
	Result A2AStreamResponse
}

func (c *A2AClient) Call(ctx context.Context, method string, params any) (any, error) {
	var result any
	if err := c.CallInto(ctx, method, params, &result); err != nil {
		return nil, err
	}
	return result, nil
}

func (c *A2AClient) CallInto(ctx context.Context, method string, params any, out any) error {
	response, err := c.doJSONRPC(ctx, method, params, "application/json")
	if err != nil {
		return err
	}
	if response.Error != nil {
		return response.Error
	}
	if out == nil || len(response.Result) == 0 || string(response.Result) == "null" {
		return nil
	}
	if err := json.Unmarshal(response.Result, out); err != nil {
		return fmt.Errorf("openlinker: decode A2A JSON-RPC result: %w", err)
	}
	return nil
}

func (c *A2AClient) SendMessage(ctx context.Context, params A2AMessageSendParams) (*A2ATask, error) {
	var out A2ATask
	if err := c.CallInto(ctx, A2AMethodMessageSend, params, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *A2AClient) StreamMessage(ctx context.Context, params A2AMessageSendParams, handle func(A2AStreamEvent) error) error {
	return c.stream(ctx, A2AMethodMessageStream, params, handle)
}

func (c *A2AClient) GetTask(ctx context.Context, params A2ATaskQueryParams) (*A2ATask, error) {
	var out A2ATask
	if err := c.CallInto(ctx, A2AMethodTasksGet, params, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *A2AClient) ListTasks(ctx context.Context, params A2ATaskListParams) (*A2ATaskListResponse, error) {
	var out A2ATaskListResponse
	if err := c.CallInto(ctx, A2AMethodTasksList, params, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *A2AClient) CancelTask(ctx context.Context, params A2ATaskQueryParams) (*A2ATask, error) {
	var out A2ATask
	if err := c.CallInto(ctx, A2AMethodTasksCancel, params, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *A2AClient) ResubscribeTask(ctx context.Context, params A2ATaskQueryParams, handle func(A2AStreamEvent) error) error {
	return c.stream(ctx, A2AMethodTasksResubscribe, params, handle)
}

func (c *A2AClient) SetTaskPushNotificationConfig(ctx context.Context, params A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error) {
	var out A2ATaskPushNotificationConfig
	if err := c.CallInto(ctx, A2AMethodTaskPushNotificationSet, params, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *A2AClient) GetTaskPushNotificationConfig(ctx context.Context, params A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error) {
	var out A2ATaskPushNotificationConfig
	if err := c.CallInto(ctx, A2AMethodTaskPushNotificationGet, params, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *A2AClient) ListTaskPushNotificationConfigs(ctx context.Context, params A2ATaskPushConfigParams) (*A2ATaskPushConfigList, error) {
	var out A2ATaskPushConfigList
	if err := c.CallInto(ctx, A2AMethodTaskPushNotificationList, params, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *A2AClient) DeleteTaskPushNotificationConfig(ctx context.Context, params A2ATaskPushConfigParams) error {
	return c.CallInto(ctx, A2AMethodTaskPushNotificationDelete, params, nil)
}

func (c *A2AClient) GetExtendedAgentCard(ctx context.Context) (*AgentCardResponse, error) {
	var out AgentCardResponse
	if err := c.CallInto(ctx, A2AMethodAgentGetExtendedCard, map[string]any{}, &out); err != nil {
		return nil, err
	}
	return &out, nil
}

func (c *A2AClient) doJSONRPC(ctx context.Context, method string, params any, accept string) (*A2AJSONRPCResponse, error) {
	body := A2AJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      fmt.Sprintf("%s-%d", defaultA2AJSONRPCIDPrefix, time.Now().UnixNano()),
		Method:  NormalizeA2AJSONRPCMethod(method),
		Params:  params,
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return nil, fmt.Errorf("openlinker: encode A2A JSON-RPC request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint, bytes.NewReader(encoded))
	if err != nil {
		return nil, err
	}
	req.Header.Set("Accept", accept)
	req.Header.Set("Content-Type", "application/json")
	if c.ProtocolVersion != "" {
		req.Header.Set("a2a-version", c.ProtocolVersion)
	}
	if c.SDKAgent != "" {
		req.Header.Set("X-OpenLinker-SDK", c.SDKAgent)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	for key, values := range c.Headers {
		for _, value := range values {
			req.Header.Set(key, value)
		}
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return nil, parseA2AHTTPError(resp)
	}
	var response A2AJSONRPCResponse
	if err := json.NewDecoder(resp.Body).Decode(&response); err != nil {
		return nil, fmt.Errorf("openlinker: decode A2A JSON-RPC response: %w", err)
	}
	return &response, nil
}

func (c *A2AClient) stream(ctx context.Context, method string, params any, handle func(A2AStreamEvent) error) error {
	body := A2AJSONRPCRequest{
		JSONRPC: "2.0",
		ID:      fmt.Sprintf("%s-%d", defaultA2AJSONRPCIDPrefix, time.Now().UnixNano()),
		Method:  NormalizeA2AJSONRPCMethod(method),
		Params:  params,
	}
	encoded, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("openlinker: encode A2A JSON-RPC request: %w", err)
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodPost, c.Endpoint, bytes.NewReader(encoded))
	if err != nil {
		return err
	}
	req.Header.Set("Accept", "text/event-stream")
	req.Header.Set("Content-Type", "application/json")
	if c.ProtocolVersion != "" {
		req.Header.Set("a2a-version", c.ProtocolVersion)
	}
	if c.SDKAgent != "" {
		req.Header.Set("X-OpenLinker-SDK", c.SDKAgent)
	}
	if c.Token != "" {
		req.Header.Set("Authorization", "Bearer "+c.Token)
	}
	for key, values := range c.Headers {
		for _, value := range values {
			req.Header.Set(key, value)
		}
	}
	resp, err := c.HTTPClient.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		return parseA2AHTTPError(resp)
	}
	return readSSE(resp.Body, func(event StreamRunEvent) error {
		streamEvent, err := a2aStreamEventFromSSE(event)
		if err != nil {
			return err
		}
		if handle != nil {
			return handle(streamEvent)
		}
		return nil
	})
}

func parseA2AHTTPError(resp *http.Response) error {
	raw, _ := io.ReadAll(resp.Body)
	if len(raw) > 0 {
		var rpc A2AJSONRPCResponse
		if err := json.Unmarshal(raw, &rpc); err == nil && rpc.Error != nil {
			return rpc.Error
		}
	}
	return &Error{
		StatusCode:   resp.StatusCode,
		Code:         fmt.Sprintf("HTTP_%d", resp.StatusCode),
		Message:      resp.Status,
		RequestID:    firstHeader(resp.Header, "X-Request-Id", "X-Correlation-Id"),
		RetryAfter:   retryAfter(resp.Header),
		ResponseBody: raw,
	}
}

func a2aStreamEventFromSSE(event StreamRunEvent) (A2AStreamEvent, error) {
	streamEvent := A2AStreamEvent{ID: event.ID, Event: event.Event, Raw: event.Data}
	if len(event.Data) == 0 {
		return streamEvent, nil
	}
	var rpc A2AJSONRPCResponse
	if err := json.Unmarshal(event.Data, &rpc); err == nil && (rpc.JSONRPC != "" || rpc.Result != nil || rpc.Error != nil) {
		if rpc.Error != nil {
			return streamEvent, rpc.Error
		}
		if len(rpc.Result) == 0 {
			return streamEvent, nil
		}
		if err := json.Unmarshal(rpc.Result, &streamEvent.Result); err != nil {
			return streamEvent, fmt.Errorf("openlinker: decode A2A stream result: %w", err)
		}
		return streamEvent, nil
	}
	if err := json.Unmarshal(event.Data, &streamEvent.Result); err != nil {
		return streamEvent, fmt.Errorf("openlinker: decode A2A stream event: %w", err)
	}
	return streamEvent, nil
}

func NewA2ATextMessageParams(messageID, text string, acceptedOutputModes []string) A2AMessageSendParams {
	blocking := true
	if strings.TrimSpace(messageID) == "" {
		messageID = fmt.Sprintf("msg-%d", time.Now().UnixNano())
	}
	if len(acceptedOutputModes) == 0 {
		acceptedOutputModes = []string{"application/json", "text/plain", "text/markdown"}
	}
	return A2AMessageSendParams{
		Message: A2AMessage{
			Kind:      "message",
			MessageID: messageID,
			Role:      "user",
			Parts: []map[string]any{{
				"kind": "text",
				"text": text,
			}},
		},
		Configuration: &A2ASendConfiguration{
			Blocking:            &blocking,
			AcceptedOutputModes: acceptedOutputModes,
		},
	}
}

func NormalizeA2AJSONRPCMethod(method string) string {
	switch strings.TrimSpace(method) {
	case "message/send", "message:send", "SendMessage":
		return A2AMethodMessageSend
	case "message/stream", "message:stream", "SendStreamingMessage":
		return A2AMethodMessageStream
	case "tasks/get", "GetTask":
		return A2AMethodTasksGet
	case "tasks/list", "ListTasks":
		return A2AMethodTasksList
	case "tasks/cancel", "CancelTask":
		return A2AMethodTasksCancel
	case "tasks/resubscribe", "SubscribeToTask":
		return A2AMethodTasksResubscribe
	case "tasks/pushNotificationConfig/set", "SetTaskPushNotificationConfig", "CreateTaskPushNotificationConfig":
		return A2AMethodTaskPushNotificationSet
	case "tasks/pushNotificationConfig/get", "GetTaskPushNotificationConfig":
		return A2AMethodTaskPushNotificationGet
	case "tasks/pushNotificationConfig/list", "ListTaskPushNotificationConfigs", "ListTaskPushNotificationConfig":
		return A2AMethodTaskPushNotificationList
	case "tasks/pushNotificationConfig/delete", "DeleteTaskPushNotificationConfig":
		return A2AMethodTaskPushNotificationDelete
	case "agent/getExtendedCard", "GetExtendedAgentCard":
		return A2AMethodAgentGetExtendedCard
	default:
		return strings.TrimSpace(method)
	}
}

func NormalizeA2ATaskState(state string) string {
	normalized := strings.ToLower(strings.TrimSpace(state))
	normalized = strings.TrimPrefix(normalized, "task_state_")
	normalized = strings.ReplaceAll(normalized, "-", "_")
	switch normalized {
	case "submitted":
		return "submitted"
	case "working":
		return "working"
	case "completed", "complete", "success", "succeeded":
		return "completed"
	case "canceled", "cancelled":
		return "canceled"
	case "failed", "failure", "error":
		return "failed"
	case "rejected":
		return "rejected"
	case "input_required":
		return "input_required"
	case "auth_required":
		return "auth_required"
	case "unknown", "unspecified":
		return "unknown"
	default:
		return normalized
	}
}

func A2ATaskStateRunStatus(state string) string {
	switch NormalizeA2ATaskState(state) {
	case "", "completed":
		return "success"
	case "failed", "canceled", "rejected", "input_required", "auth_required":
		return "failed"
	default:
		return "success"
	}
}

func A2ATaskStateFailed(state string) bool {
	return A2ATaskStateRunStatus(state) == "failed"
}

func ExtractA2AText(value any) string {
	parts := make([]string, 0)
	collectA2AText(value, &parts)
	return strings.Join(parts, "\n")
}

func collectA2AText(value any, parts *[]string) {
	switch typed := value.(type) {
	case A2ATask:
		collectA2AText(typed.Status.Message, parts)
		collectA2AText(typed.Artifacts, parts)
		collectA2AText(typed.History, parts)
	case *A2ATask:
		if typed != nil {
			collectA2AText(*typed, parts)
		}
	case A2AMessage:
		collectA2AText(typed.Parts, parts)
	case *A2AMessage:
		if typed != nil {
			collectA2AText(*typed, parts)
		}
	case A2AArtifact:
		collectA2AText(typed.Parts, parts)
	case []A2AArtifact:
		for _, item := range typed {
			collectA2AText(item, parts)
		}
	case []A2AMessage:
		for _, item := range typed {
			collectA2AText(item, parts)
		}
	case []map[string]any:
		for _, item := range typed {
			collectA2AText(item, parts)
		}
	case []any:
		for _, item := range typed {
			collectA2AText(item, parts)
		}
	case map[string]any:
		if text, ok := typed["text"].(string); ok && strings.TrimSpace(text) != "" {
			*parts = append(*parts, strings.TrimSpace(text))
		}
		for _, key := range []string{"parts", "artifacts", "history", "message", "messages", "result", "status"} {
			collectA2AText(typed[key], parts)
		}
	case JSON:
		collectA2AText(map[string]any(typed), parts)
	}
}
