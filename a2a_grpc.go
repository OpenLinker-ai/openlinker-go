package openlinker

import (
	"context"
	"crypto/tls"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"io"
	"math"
	"net"
	"net/url"
	"strings"
	"time"

	"github.com/OpenLinker-ai/openlinker-go/a2apb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/types/known/structpb"
	"google.golang.org/protobuf/types/known/timestamppb"
)

type A2AGRPCClient struct {
	Endpoint   string
	Target     string
	Tenant     string
	Token      string
	Headers    metadata.MD
	SDKAgent   string
	conn       *grpc.ClientConn
	client     a2apb.A2AServiceClient
	dialOption []grpc.DialOption
}

type A2AGRPCClientOption func(*A2AGRPCClient)

func NewA2AGRPCClient(endpoint, tenant string, opts ...A2AGRPCClientOption) (*A2AGRPCClient, error) {
	endpoint = strings.TrimSpace(endpoint)
	tenant = strings.TrimSpace(tenant)
	if endpoint == "" {
		return nil, fmt.Errorf("openlinker: A2A gRPC endpoint is required")
	}
	if tenant == "" {
		return nil, fmt.Errorf("openlinker: A2A gRPC tenant is required")
	}
	target, transport, err := a2aGRPCTarget(endpoint)
	if err != nil {
		return nil, err
	}
	client := &A2AGRPCClient{
		Endpoint: endpoint,
		Target:   target,
		Tenant:   tenant,
		Headers:  metadata.MD{},
		SDKAgent: defaultSDKAgent,
		dialOption: []grpc.DialOption{
			grpc.WithTransportCredentials(transport),
		},
	}
	for _, opt := range opts {
		opt(client)
	}
	conn, err := grpc.NewClient(client.Target, client.dialOption...)
	if err != nil {
		return nil, fmt.Errorf("openlinker: create A2A gRPC client: %w", err)
	}
	client.conn = conn
	client.client = a2apb.NewA2AServiceClient(conn)
	return client, nil
}

func WithA2AGRPCToken(token string) A2AGRPCClientOption {
	return func(c *A2AGRPCClient) {
		c.Token = strings.TrimSpace(token)
	}
}

func WithA2AGRPCHeader(name, value string) A2AGRPCClientOption {
	return func(c *A2AGRPCClient) {
		name = strings.TrimSpace(name)
		if name != "" {
			c.Headers.Set(name, value)
		}
	}
}

func WithA2AGRPCHeaders(headers map[string]string) A2AGRPCClientOption {
	return func(c *A2AGRPCClient) {
		for key, value := range headers {
			key = strings.TrimSpace(key)
			if key != "" {
				c.Headers.Set(key, value)
			}
		}
	}
}

func WithA2AGRPCDialOptions(opts ...grpc.DialOption) A2AGRPCClientOption {
	return func(c *A2AGRPCClient) {
		c.dialOption = append(c.dialOption, opts...)
	}
}

func WithA2AGRPCTransportCredentials(creds credentials.TransportCredentials) A2AGRPCClientOption {
	return func(c *A2AGRPCClient) {
		if creds == nil {
			return
		}
		if len(c.dialOption) == 0 {
			c.dialOption = append(c.dialOption, grpc.WithTransportCredentials(creds))
			return
		}
		c.dialOption[0] = grpc.WithTransportCredentials(creds)
	}
}

func WithA2AGRPCSDKAgent(agent string) A2AGRPCClientOption {
	return func(c *A2AGRPCClient) {
		if strings.TrimSpace(agent) != "" {
			c.SDKAgent = strings.TrimSpace(agent)
		}
	}
}

func (c *A2AGRPCClient) Close() error {
	if c == nil || c.conn == nil {
		return nil
	}
	return c.conn.Close()
}

func (c *A2AGRPCClient) SendMessage(ctx context.Context, params A2AMessageSendParams) (*A2ATask, error) {
	resp, err := c.SendMessageResponse(ctx, params)
	if err != nil {
		return nil, err
	}
	if resp.Task != nil {
		return resp.Task, nil
	}
	if resp.Message != nil {
		return nil, fmt.Errorf("openlinker: A2A gRPC SendMessage returned a message; use SendMessageResponse to handle message payloads")
	}
	return nil, fmt.Errorf("openlinker: A2A gRPC SendMessage returned an empty response")
}

func (c *A2AGRPCClient) SendMessageResponse(ctx context.Context, params A2AMessageSendParams) (*A2ASendMessageResponse, error) {
	req, err := c.sendMessageRequest(params)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.SendMessage(c.outgoingContext(ctx), req)
	if err != nil {
		return nil, err
	}
	return sendMessageResponseFromProto(resp), nil
}

func (c *A2AGRPCClient) StreamMessage(ctx context.Context, params A2AMessageSendParams, handle func(A2AStreamEvent) error) error {
	req, err := c.sendMessageRequest(params)
	if err != nil {
		return err
	}
	stream, err := c.client.SendStreamingMessage(c.outgoingContext(ctx), req)
	if err != nil {
		return err
	}
	return readA2AGRPCStream(stream, handle)
}

func (c *A2AGRPCClient) GetTask(ctx context.Context, params A2ATaskQueryParams) (*A2ATask, error) {
	req := &a2apb.GetTaskRequest{Tenant: c.Tenant, Id: params.ID}
	historyLength, err := a2aOptionalInt32("historyLength", params.HistoryLength)
	if err != nil {
		return nil, err
	}
	req.HistoryLength = historyLength
	task, err := c.client.GetTask(c.outgoingContext(ctx), req)
	if err != nil {
		return nil, err
	}
	return taskFromProto(task), nil
}

func (c *A2AGRPCClient) ListTasks(ctx context.Context, params A2ATaskListParams) (*A2ATaskListResponse, error) {
	req := &a2apb.ListTasksRequest{
		Tenant:    c.Tenant,
		ContextId: params.ContextID,
		Status:    taskStateToProto(params.Status),
		PageToken: params.PageToken,
	}
	pageSize, err := a2aOptionalInt32("pageSize", params.PageSize)
	if err != nil {
		return nil, err
	}
	historyLength, err := a2aOptionalInt32("historyLength", params.HistoryLength)
	if err != nil {
		return nil, err
	}
	req.PageSize = pageSize
	req.HistoryLength = historyLength
	if params.IncludeArtifacts != nil {
		req.IncludeArtifacts = params.IncludeArtifacts
	}
	if ts := timestampFromString(params.StatusTimestampAfter); ts != nil {
		req.StatusTimestampAfter = ts
	}
	resp, err := c.client.ListTasks(c.outgoingContext(ctx), req)
	if err != nil {
		return nil, err
	}
	tasks := make([]A2ATask, 0, len(resp.GetTasks()))
	for _, task := range resp.GetTasks() {
		if converted := taskFromProto(task); converted != nil {
			tasks = append(tasks, *converted)
		}
	}
	return &A2ATaskListResponse{
		Tasks:         tasks,
		NextPageToken: resp.GetNextPageToken(),
		PageSize:      resp.GetPageSize(),
		TotalSize:     resp.GetTotalSize(),
	}, nil
}

func (c *A2AGRPCClient) CancelTask(ctx context.Context, params A2ATaskQueryParams) (*A2ATask, error) {
	task, err := c.client.CancelTask(c.outgoingContext(ctx), &a2apb.CancelTaskRequest{Tenant: c.Tenant, Id: params.ID})
	if err != nil {
		return nil, err
	}
	return taskFromProto(task), nil
}

func (c *A2AGRPCClient) ResubscribeTask(ctx context.Context, params A2ATaskQueryParams, handle func(A2AStreamEvent) error) error {
	stream, err := c.client.SubscribeToTask(c.outgoingContext(ctx), &a2apb.SubscribeToTaskRequest{Tenant: c.Tenant, Id: params.ID})
	if err != nil {
		return err
	}
	return readA2AGRPCStream(stream, handle)
}

func (c *A2AGRPCClient) SetTaskPushNotificationConfig(ctx context.Context, params A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error) {
	resp, err := c.client.CreateTaskPushNotificationConfig(c.outgoingContext(ctx), taskPushConfigToProto(c.Tenant, params))
	if err != nil {
		return nil, err
	}
	return taskPushConfigFromProto(resp), nil
}

func (c *A2AGRPCClient) GetTaskPushNotificationConfig(ctx context.Context, params A2ATaskPushConfigParams) (*A2ATaskPushNotificationConfig, error) {
	resp, err := c.client.GetTaskPushNotificationConfig(c.outgoingContext(ctx), &a2apb.GetTaskPushNotificationConfigRequest{
		Tenant: c.Tenant,
		TaskId: a2aTaskIDFromPushParams(params),
		Id:     a2aPushConfigID(params),
	})
	if err != nil {
		return nil, err
	}
	return taskPushConfigFromProto(resp), nil
}

func (c *A2AGRPCClient) ListTaskPushNotificationConfigs(ctx context.Context, params A2ATaskPushConfigParams) (*A2ATaskPushConfigList, error) {
	pageSize, err := a2aOptionalInt32Value("pageSize", params.PageSize)
	if err != nil {
		return nil, err
	}
	resp, err := c.client.ListTaskPushNotificationConfigs(c.outgoingContext(ctx), &a2apb.ListTaskPushNotificationConfigsRequest{
		Tenant:    c.Tenant,
		TaskId:    a2aTaskIDFromPushParams(params),
		PageSize:  pageSize,
		PageToken: params.PageToken,
	})
	if err != nil {
		return nil, err
	}
	configs := make([]A2ATaskPushNotificationConfig, 0, len(resp.GetConfigs()))
	for _, cfg := range resp.GetConfigs() {
		if converted := taskPushConfigFromProto(cfg); converted != nil {
			configs = append(configs, *converted)
		}
	}
	return &A2ATaskPushConfigList{Configs: configs, Items: configs, NextPageToken: resp.GetNextPageToken()}, nil
}

func (c *A2AGRPCClient) DeleteTaskPushNotificationConfig(ctx context.Context, params A2ATaskPushConfigParams) error {
	_, err := c.client.DeleteTaskPushNotificationConfig(c.outgoingContext(ctx), &a2apb.DeleteTaskPushNotificationConfigRequest{
		Tenant: c.Tenant,
		TaskId: a2aTaskIDFromPushParams(params),
		Id:     a2aPushConfigID(params),
	})
	return err
}

func (c *A2AGRPCClient) GetExtendedAgentCard(ctx context.Context) (*AgentCardResponse, error) {
	card, err := c.client.GetExtendedAgentCard(c.outgoingContext(ctx), &a2apb.GetExtendedAgentCardRequest{Tenant: c.Tenant})
	if err != nil {
		return nil, err
	}
	raw, err := protojson.MarshalOptions{UseProtoNames: false}.Marshal(card)
	if err != nil {
		return nil, fmt.Errorf("openlinker: marshal A2A gRPC Agent Card: %w", err)
	}
	raw, err = normalizeA2AGRPCAgentCardJSON(raw)
	if err != nil {
		return nil, err
	}
	var out AgentCardResponse
	if err := json.Unmarshal(raw, &out); err != nil {
		return nil, fmt.Errorf("openlinker: decode A2A gRPC Agent Card: %w", err)
	}
	return &out, nil
}

func normalizeA2AGRPCAgentCardJSON(raw []byte) ([]byte, error) {
	var doc map[string]any
	if err := json.Unmarshal(raw, &doc); err != nil {
		return nil, fmt.Errorf("openlinker: decode A2A gRPC Agent Card JSON: %w", err)
	}
	normalizeA2AGRPCSecurityRequirements(doc, "securityRequirements")
	normalizeA2AGRPCSecurityRequirements(doc, "security")
	out, err := json.Marshal(doc)
	if err != nil {
		return nil, fmt.Errorf("openlinker: normalize A2A gRPC Agent Card JSON: %w", err)
	}
	return out, nil
}

func normalizeA2AGRPCSecurityRequirements(doc map[string]any, key string) {
	raw, ok := doc[key].([]any)
	if !ok {
		return
	}
	for i, item := range raw {
		requirement, ok := item.(map[string]any)
		if !ok {
			continue
		}
		schemes, ok := requirement["schemes"].(map[string]any)
		if !ok {
			continue
		}
		flattened := map[string]any{}
		for name, value := range schemes {
			if scoped, ok := value.(map[string]any); ok {
				if list, ok := scoped["list"].([]any); ok {
					flattened[name] = list
					continue
				}
			}
			flattened[name] = value
		}
		raw[i] = flattened
	}
	doc[key] = raw
}

func (c *A2AGRPCClient) sendMessageRequest(params A2AMessageSendParams) (*a2apb.SendMessageRequest, error) {
	cfg, err := sendConfigurationToProto(params.Configuration)
	if err != nil {
		return nil, err
	}
	return &a2apb.SendMessageRequest{
		Tenant:        c.Tenant,
		Message:       messageToProto(params.Message),
		Configuration: cfg,
		Metadata:      mapToStruct(params.Metadata),
	}, nil
}

func (c *A2AGRPCClient) outgoingContext(ctx context.Context) context.Context {
	md := metadata.MD{}
	for key, values := range c.Headers {
		for _, value := range values {
			md.Append(key, value)
		}
	}
	if c.Token != "" {
		md.Set("authorization", "Bearer "+c.Token)
	}
	if c.SDKAgent != "" {
		md.Set("x-openlinker-sdk-agent", c.SDKAgent)
	}
	return metadata.NewOutgoingContext(ctx, md)
}

func readA2AGRPCStream(stream grpc.ServerStreamingClient[a2apb.StreamResponse], handle func(A2AStreamEvent) error) error {
	for {
		resp, err := stream.Recv()
		if err == io.EOF {
			return nil
		}
		if err != nil {
			return err
		}
		if handle != nil {
			if err := handle(A2AStreamEvent{Result: streamResponseFromProto(resp)}); err != nil {
				return err
			}
		}
	}
}

func a2aGRPCTarget(endpoint string) (string, credentials.TransportCredentials, error) {
	parsed, err := url.Parse(endpoint)
	if err == nil && parsed.Scheme != "" && parsed.Host != "" {
		target := parsed.Host
		switch strings.ToLower(parsed.Scheme) {
		case "http", "grpc":
			return target, insecure.NewCredentials(), nil
		case "https", "grpcs":
			return target, credentials.NewTLS(&tls.Config{ServerName: parsed.Hostname(), MinVersion: tls.VersionTLS12}), nil
		default:
			return endpoint, credentials.NewTLS(&tls.Config{MinVersion: tls.VersionTLS12}), nil
		}
	}
	if isLocalGRPCTarget(endpoint) {
		return endpoint, insecure.NewCredentials(), nil
	}
	host := endpoint
	if h, _, err := net.SplitHostPort(endpoint); err == nil {
		host = h
	}
	return endpoint, credentials.NewTLS(&tls.Config{ServerName: host, MinVersion: tls.VersionTLS12}), nil
}

func isLocalGRPCTarget(target string) bool {
	host := target
	if h, _, err := net.SplitHostPort(target); err == nil {
		host = h
	}
	host = strings.Trim(host, "[]")
	return host == "localhost" || host == "127.0.0.1" || host == "::1"
}

func sendConfigurationToProto(cfg *A2ASendConfiguration) (*a2apb.SendMessageConfiguration, error) {
	if cfg == nil {
		return nil, nil
	}
	out := &a2apb.SendMessageConfiguration{
		AcceptedOutputModes: append([]string{}, cfg.AcceptedOutputModes...),
	}
	if cfg.ReturnImmediately != nil {
		out.ReturnImmediately = *cfg.ReturnImmediately
	}
	historyLength, err := a2aOptionalInt32("historyLength", cfg.HistoryLength)
	if err != nil {
		return nil, err
	}
	out.HistoryLength = historyLength
	if cfg.TaskPushNotificationConfig != nil {
		out.TaskPushNotificationConfig = taskPushNotificationConfigToProto(*cfg.TaskPushNotificationConfig)
	} else if cfg.PushNotificationConfig != nil {
		out.TaskPushNotificationConfig = taskPushNotificationConfigToProto(A2ATaskPushNotificationConfig{
			PushNotificationConfig: *cfg.PushNotificationConfig,
		})
	}
	return out, nil
}

func a2aOptionalInt32(name string, value *int) (*int32, error) {
	if value == nil {
		return nil, nil
	}
	if *value < 0 || *value > math.MaxInt32 {
		return nil, fmt.Errorf("openlinker: A2A %s must be between 0 and %d", name, math.MaxInt32)
	}
	// #nosec G115 -- value is checked against int32 bounds above.
	converted := int32(*value)
	return &converted, nil
}

func a2aOptionalInt32Value(name string, value *int) (int32, error) {
	converted, err := a2aOptionalInt32(name, value)
	if err != nil || converted == nil {
		return 0, err
	}
	return *converted, nil
}

func messageToProto(msg A2AMessage) *a2apb.Message {
	parts := make([]*a2apb.Part, 0, len(msg.Parts))
	for _, part := range msg.Parts {
		parts = append(parts, partToProto(part))
	}
	return &a2apb.Message{
		MessageId:        msg.MessageID,
		ContextId:        msg.ContextID,
		TaskId:           msg.TaskID,
		Role:             roleToProto(msg.Role),
		Parts:            parts,
		Metadata:         mapToStruct(msg.Metadata),
		Extensions:       append([]string{}, msg.Extensions...),
		ReferenceTaskIds: append([]string{}, msg.ReferenceTaskIDs...),
	}
}

func partToProto(part map[string]any) *a2apb.Part {
	out := &a2apb.Part{Metadata: mapToStruct(nestedMap(part, "metadata"))}
	if filename := firstA2APartString(part, "filename", "fileName", "name"); filename != "" {
		out.Filename = filename
	}
	if mediaType := firstA2APartString(part, "mediaType", "mimeType"); mediaType != "" {
		out.MediaType = mediaType
	}
	switch a2aPartKind(part) {
	case "text":
		out.Content = &a2apb.Part_Text{Text: fmt.Sprint(part["text"])}
	case "data":
		out.Content = &a2apb.Part_Data{Data: valueToProto(part["data"])}
	case "file":
		source := part
		if file, ok := part["file"].(map[string]any); ok {
			source = file
		}
		if out.Filename == "" {
			out.Filename = firstA2APartString(source, "filename", "fileName", "name")
		}
		if out.MediaType == "" {
			out.MediaType = firstA2APartString(source, "mediaType", "mimeType")
		}
		if uri := firstA2APartString(source, "url", "uri"); uri != "" {
			out.Content = &a2apb.Part_Url{Url: uri}
			return out
		}
		if raw := bytesFromA2AFilePart(source); len(raw) > 0 {
			out.Content = &a2apb.Part_Raw{Raw: raw}
			return out
		}
		out.Content = &a2apb.Part_Data{Data: valueToProto(source)}
	default:
		out.Content = &a2apb.Part_Data{Data: valueToProto(part)}
	}
	return out
}

func taskPushConfigToProto(tenant string, params A2ATaskPushConfigParams) *a2apb.TaskPushNotificationConfig {
	return taskPushNotificationConfigToProto(A2ATaskPushNotificationConfig{
		Tenant:                 tenant,
		ID:                     params.PushNotificationConfigID,
		TaskID:                 a2aTaskIDFromPushParams(params),
		URL:                    params.URL,
		Token:                  params.Token,
		Secret:                 params.Secret,
		Authentication:         params.Authentication,
		Metadata:               params.Metadata,
		EventTypes:             params.EventTypes,
		EventTypesAlias:        params.EventTypesAlias,
		PushNotificationConfig: params.PushNotificationConfig,
	})
}

func taskPushNotificationConfigToProto(cfg A2ATaskPushNotificationConfig) *a2apb.TaskPushNotificationConfig {
	push := pushConfigFromA2ATaskPushNotificationConfig(cfg)
	out := &a2apb.TaskPushNotificationConfig{
		Tenant: cfg.Tenant,
		Id:     cfg.ID,
		TaskId: cfg.TaskID,
		Url:    push.URL,
		Token:  push.Token,
	}
	if push.Authentication != nil {
		out.Authentication = &a2apb.AuthenticationInfo{
			Scheme:      push.Authentication.Scheme,
			Credentials: push.Authentication.Credentials,
		}
	}
	return out
}

func sendMessageResponseFromProto(resp *a2apb.SendMessageResponse) *A2ASendMessageResponse {
	if resp == nil {
		return &A2ASendMessageResponse{}
	}
	if task := resp.GetTask(); task != nil {
		return &A2ASendMessageResponse{Task: taskFromProto(task)}
	}
	if msg := resp.GetMessage(); msg != nil {
		converted := messageFromProto(msg)
		return &A2ASendMessageResponse{Message: &converted}
	}
	return &A2ASendMessageResponse{}
}

func streamResponseFromProto(resp *a2apb.StreamResponse) A2AStreamResponse {
	if resp == nil {
		return A2AStreamResponse{}
	}
	if task := resp.GetTask(); task != nil {
		return A2AStreamResponse{Task: taskFromProto(task)}
	}
	if msg := resp.GetMessage(); msg != nil {
		converted := messageFromProto(msg)
		return A2AStreamResponse{Message: &converted}
	}
	if event := resp.GetStatusUpdate(); event != nil {
		return A2AStreamResponse{StatusUpdate: statusUpdateFromProto(event)}
	}
	if event := resp.GetArtifactUpdate(); event != nil {
		return A2AStreamResponse{ArtifactUpdate: artifactUpdateFromProto(event)}
	}
	return A2AStreamResponse{}
}

func taskFromProto(task *a2apb.Task) *A2ATask {
	if task == nil {
		return nil
	}
	artifacts := make([]A2AArtifact, 0, len(task.GetArtifacts()))
	for _, artifact := range task.GetArtifacts() {
		artifacts = append(artifacts, artifactFromProto(artifact))
	}
	history := make([]A2AMessage, 0, len(task.GetHistory()))
	for _, msg := range task.GetHistory() {
		history = append(history, messageFromProto(msg))
	}
	return &A2ATask{
		ID:        task.GetId(),
		ContextID: task.GetContextId(),
		Status:    taskStatusFromProto(task.GetStatus()),
		Artifacts: artifacts,
		History:   history,
		Metadata:  structToMap(task.GetMetadata()),
	}
}

func taskStatusFromProto(status *a2apb.TaskStatus) A2ATaskStatus {
	if status == nil {
		return A2ATaskStatus{}
	}
	return A2ATaskStatus{
		State:     taskStateFromProto(status.GetState()),
		Timestamp: timestampToString(status.GetTimestamp()),
		Message:   messagePtrFromProto(status.GetMessage()),
	}
}

func statusUpdateFromProto(event *a2apb.TaskStatusUpdateEvent) *A2ATaskStatusUpdateEvent {
	if event == nil {
		return nil
	}
	return &A2ATaskStatusUpdateEvent{
		TaskID:    event.GetTaskId(),
		ContextID: event.GetContextId(),
		Status:    taskStatusFromProto(event.GetStatus()),
		Metadata:  structToMap(event.GetMetadata()),
	}
}

func artifactUpdateFromProto(event *a2apb.TaskArtifactUpdateEvent) *A2ATaskArtifactUpdateEvent {
	if event == nil {
		return nil
	}
	return &A2ATaskArtifactUpdateEvent{
		TaskID:    event.GetTaskId(),
		ContextID: event.GetContextId(),
		Artifact:  artifactFromProto(event.GetArtifact()),
		Append:    event.GetAppend(),
		LastChunk: event.GetLastChunk(),
		Metadata:  structToMap(event.GetMetadata()),
	}
}

func messagePtrFromProto(msg *a2apb.Message) *A2AMessage {
	if msg == nil {
		return nil
	}
	converted := messageFromProto(msg)
	return &converted
}

func messageFromProto(msg *a2apb.Message) A2AMessage {
	if msg == nil {
		return A2AMessage{}
	}
	parts := make([]map[string]any, 0, len(msg.GetParts()))
	for _, part := range msg.GetParts() {
		parts = append(parts, partFromProto(part))
	}
	return A2AMessage{
		MessageID:        msg.GetMessageId(),
		ContextID:        msg.GetContextId(),
		TaskID:           msg.GetTaskId(),
		Role:             roleFromProto(msg.GetRole()),
		Parts:            parts,
		Metadata:         structToMap(msg.GetMetadata()),
		Extensions:       append([]string{}, msg.GetExtensions()...),
		ReferenceTaskIDs: append([]string{}, msg.GetReferenceTaskIds()...),
	}
}

func partFromProto(part *a2apb.Part) map[string]any {
	if part == nil {
		return map[string]any{}
	}
	out := map[string]any{}
	switch content := part.GetContent().(type) {
	case *a2apb.Part_Text:
		out["kind"] = "text"
		out["text"] = content.Text
	case *a2apb.Part_Data:
		out["kind"] = "data"
		out["data"] = valueToInterface(content.Data)
	case *a2apb.Part_Url:
		out["kind"] = "file"
		out["file"] = map[string]any{"url": content.Url, "uri": content.Url}
	case *a2apb.Part_Raw:
		out["kind"] = "file"
		out["file"] = map[string]any{"bytes": base64.StdEncoding.EncodeToString(content.Raw)}
	default:
		out["kind"] = "data"
	}
	if filename := part.GetFilename(); filename != "" {
		out["filename"] = filename
	}
	if mediaType := part.GetMediaType(); mediaType != "" {
		out["mediaType"] = mediaType
	}
	if metadata := structToMap(part.GetMetadata()); len(metadata) > 0 {
		out["metadata"] = metadata
	}
	return out
}

func artifactFromProto(artifact *a2apb.Artifact) A2AArtifact {
	if artifact == nil {
		return A2AArtifact{}
	}
	parts := make([]map[string]any, 0, len(artifact.GetParts()))
	for _, part := range artifact.GetParts() {
		parts = append(parts, partFromProto(part))
	}
	return A2AArtifact{
		ArtifactID: artifact.GetArtifactId(),
		Name:       artifact.GetName(),
		Extensions: append([]string{}, artifact.GetExtensions()...),
		Parts:      parts,
		Metadata:   structToMap(artifact.GetMetadata()),
	}
}

func taskPushConfigFromProto(cfg *a2apb.TaskPushNotificationConfig) *A2ATaskPushNotificationConfig {
	if cfg == nil {
		return nil
	}
	out := &A2ATaskPushNotificationConfig{
		Tenant: cfg.GetTenant(),
		ID:     cfg.GetId(),
		TaskID: cfg.GetTaskId(),
		URL:    cfg.GetUrl(),
		Token:  cfg.GetToken(),
	}
	if auth := cfg.GetAuthentication(); auth != nil {
		out.Authentication = &A2APushAuthenticationInfo{Scheme: auth.GetScheme(), Credentials: auth.GetCredentials()}
	}
	out.PushNotificationConfig = pushConfigFromA2ATaskPushNotificationConfig(*out)
	return out
}

func roleToProto(role string) a2apb.Role {
	switch strings.ToLower(strings.TrimSpace(role)) {
	case "agent", "assistant", "server", "ROLE_AGENT":
		return a2apb.Role_ROLE_AGENT
	case "user", "client", "", "ROLE_USER":
		return a2apb.Role_ROLE_USER
	default:
		return a2apb.Role_ROLE_USER
	}
}

func roleFromProto(role a2apb.Role) string {
	switch role {
	case a2apb.Role_ROLE_AGENT:
		return "agent"
	case a2apb.Role_ROLE_USER, a2apb.Role_ROLE_UNSPECIFIED:
		return "user"
	default:
		return "user"
	}
}

func taskStateToProto(state string) a2apb.TaskState {
	switch NormalizeA2ATaskState(state) {
	case "submitted":
		return a2apb.TaskState_TASK_STATE_SUBMITTED
	case "working":
		return a2apb.TaskState_TASK_STATE_WORKING
	case "completed":
		return a2apb.TaskState_TASK_STATE_COMPLETED
	case "failed":
		return a2apb.TaskState_TASK_STATE_FAILED
	case "canceled":
		return a2apb.TaskState_TASK_STATE_CANCELED
	case "input_required":
		return a2apb.TaskState_TASK_STATE_INPUT_REQUIRED
	case "auth_required":
		return a2apb.TaskState_TASK_STATE_AUTH_REQUIRED
	case "rejected":
		return a2apb.TaskState_TASK_STATE_REJECTED
	default:
		return a2apb.TaskState_TASK_STATE_UNSPECIFIED
	}
}

func taskStateFromProto(state a2apb.TaskState) string {
	switch state {
	case a2apb.TaskState_TASK_STATE_SUBMITTED:
		return "submitted"
	case a2apb.TaskState_TASK_STATE_WORKING:
		return "working"
	case a2apb.TaskState_TASK_STATE_COMPLETED:
		return "completed"
	case a2apb.TaskState_TASK_STATE_FAILED:
		return "failed"
	case a2apb.TaskState_TASK_STATE_CANCELED:
		return "canceled"
	case a2apb.TaskState_TASK_STATE_INPUT_REQUIRED:
		return "input_required"
	case a2apb.TaskState_TASK_STATE_AUTH_REQUIRED:
		return "auth_required"
	case a2apb.TaskState_TASK_STATE_REJECTED:
		return "rejected"
	default:
		return ""
	}
}

func nestedMap(source map[string]any, key string) map[string]any {
	if source == nil {
		return nil
	}
	value, _ := source[key].(map[string]any)
	return value
}

func bytesFromA2AFilePart(source map[string]any) []byte {
	for _, key := range []string{"raw", "bytes", "fileWithBytes"} {
		switch value := source[key].(type) {
		case []byte:
			return value
		case string:
			if decoded, err := base64.StdEncoding.DecodeString(value); err == nil {
				return decoded
			}
			if decoded, err := base64.RawStdEncoding.DecodeString(value); err == nil {
				return decoded
			}
			return []byte(value)
		}
	}
	return nil
}

func mapToStruct(value map[string]any) *structpb.Struct {
	if len(value) == 0 {
		return nil
	}
	sanitized, ok := sanitizeProtoValue(value).(map[string]any)
	if !ok {
		return nil
	}
	out, err := structpb.NewStruct(sanitized)
	if err != nil {
		return nil
	}
	return out
}

func structToMap(value *structpb.Struct) map[string]any {
	if value == nil {
		return nil
	}
	out, _ := valueToInterface(value).(map[string]any)
	return out
}

func valueToProto(value any) *structpb.Value {
	out, err := structpb.NewValue(sanitizeProtoValue(value))
	if err != nil {
		return structpb.NewNullValue()
	}
	return out
}

func valueToInterface(value any) any {
	switch typed := value.(type) {
	case nil:
		return nil
	case *structpb.Struct:
		return typed.AsMap()
	case *structpb.Value:
		return typed.AsInterface()
	default:
		return typed
	}
}

func sanitizeProtoValue(value any) any {
	raw, err := json.Marshal(value)
	if err != nil {
		return fmt.Sprint(value)
	}
	var out any
	if err := json.Unmarshal(raw, &out); err != nil {
		return fmt.Sprint(value)
	}
	return out
}

func timestampFromString(raw string) *timestamppb.Timestamp {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return nil
	}
	for _, layout := range []string{time.RFC3339Nano, time.RFC3339} {
		if parsed, err := time.Parse(layout, raw); err == nil {
			return timestamppb.New(parsed)
		}
	}
	return nil
}

func timestampToString(ts *timestamppb.Timestamp) string {
	if ts == nil {
		return ""
	}
	return ts.AsTime().UTC().Format(time.RFC3339Nano)
}
