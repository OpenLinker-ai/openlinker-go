package openlinker

import (
	"context"
	"encoding/json"
	"net"
	"testing"

	"github.com/OpenLinker-ai/openlinker-go/a2apb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
	"google.golang.org/protobuf/types/known/emptypb"
)

const a2aGRPCTestBufferSize = 1024 * 1024

type fakeA2AGRPCServer struct {
	a2apb.UnimplementedA2AServiceServer
	lastAuthorization string
	lastTenant        string
	lastTaskID        string
}

func (s *fakeA2AGRPCServer) SendMessage(ctx context.Context, req *a2apb.SendMessageRequest) (*a2apb.SendMessageResponse, error) {
	s.record(ctx, req.GetTenant())
	return &a2apb.SendMessageResponse{Payload: &a2apb.SendMessageResponse_Task{Task: &a2apb.Task{
		Id:        "task-grpc",
		ContextId: req.GetMessage().GetContextId(),
		Status:    &a2apb.TaskStatus{State: a2apb.TaskState_TASK_STATE_COMPLETED},
	}}}, nil
}

func (s *fakeA2AGRPCServer) SendStreamingMessage(req *a2apb.SendMessageRequest, stream grpc.ServerStreamingServer[a2apb.StreamResponse]) error {
	s.record(stream.Context(), req.GetTenant())
	if err := stream.Send(&a2apb.StreamResponse{Payload: &a2apb.StreamResponse_Task{Task: &a2apb.Task{
		Id:     "task-stream",
		Status: &a2apb.TaskStatus{State: a2apb.TaskState_TASK_STATE_WORKING},
	}}}); err != nil {
		return err
	}
	return stream.Send(&a2apb.StreamResponse{Payload: &a2apb.StreamResponse_StatusUpdate{StatusUpdate: &a2apb.TaskStatusUpdateEvent{
		TaskId: "task-stream",
		Status: &a2apb.TaskStatus{State: a2apb.TaskState_TASK_STATE_COMPLETED},
	}}})
}

func (s *fakeA2AGRPCServer) GetTask(ctx context.Context, req *a2apb.GetTaskRequest) (*a2apb.Task, error) {
	s.record(ctx, req.GetTenant())
	s.lastTaskID = req.GetId()
	return &a2apb.Task{Id: req.GetId(), Status: &a2apb.TaskStatus{State: a2apb.TaskState_TASK_STATE_WORKING}}, nil
}

func (s *fakeA2AGRPCServer) ListTasks(ctx context.Context, req *a2apb.ListTasksRequest) (*a2apb.ListTasksResponse, error) {
	s.record(ctx, req.GetTenant())
	return &a2apb.ListTasksResponse{Tasks: []*a2apb.Task{{Id: "task-list"}}, PageSize: req.GetPageSize(), TotalSize: 1}, nil
}

func (s *fakeA2AGRPCServer) CancelTask(ctx context.Context, req *a2apb.CancelTaskRequest) (*a2apb.Task, error) {
	s.record(ctx, req.GetTenant())
	return &a2apb.Task{Id: req.GetId(), Status: &a2apb.TaskStatus{State: a2apb.TaskState_TASK_STATE_CANCELED}}, nil
}

func (s *fakeA2AGRPCServer) SubscribeToTask(req *a2apb.SubscribeToTaskRequest, stream grpc.ServerStreamingServer[a2apb.StreamResponse]) error {
	s.record(stream.Context(), req.GetTenant())
	return stream.Send(&a2apb.StreamResponse{Payload: &a2apb.StreamResponse_StatusUpdate{StatusUpdate: &a2apb.TaskStatusUpdateEvent{
		TaskId: req.GetId(),
		Status: &a2apb.TaskStatus{State: a2apb.TaskState_TASK_STATE_COMPLETED},
	}}})
}

func (s *fakeA2AGRPCServer) CreateTaskPushNotificationConfig(ctx context.Context, req *a2apb.TaskPushNotificationConfig) (*a2apb.TaskPushNotificationConfig, error) {
	s.record(ctx, req.GetTenant())
	return &a2apb.TaskPushNotificationConfig{Tenant: req.GetTenant(), Id: "cfg-1", TaskId: req.GetTaskId(), Url: req.GetUrl(), Token: req.GetToken()}, nil
}

func (s *fakeA2AGRPCServer) ListTaskPushNotificationConfigs(ctx context.Context, req *a2apb.ListTaskPushNotificationConfigsRequest) (*a2apb.ListTaskPushNotificationConfigsResponse, error) {
	s.record(ctx, req.GetTenant())
	return &a2apb.ListTaskPushNotificationConfigsResponse{Configs: []*a2apb.TaskPushNotificationConfig{{Tenant: req.GetTenant(), Id: "cfg-1", TaskId: req.GetTaskId()}}}, nil
}

func (s *fakeA2AGRPCServer) DeleteTaskPushNotificationConfig(ctx context.Context, req *a2apb.DeleteTaskPushNotificationConfigRequest) (*emptypb.Empty, error) {
	s.record(ctx, req.GetTenant())
	return &emptypb.Empty{}, nil
}

func (s *fakeA2AGRPCServer) record(ctx context.Context, tenant string) {
	s.lastTenant = tenant
	if md, ok := metadata.FromIncomingContext(ctx); ok {
		if values := md.Get("authorization"); len(values) > 0 {
			s.lastAuthorization = values[0]
		}
	}
}

func TestA2AGRPCClientMethods(t *testing.T) {
	server := &fakeA2AGRPCServer{}
	listener := bufconn.Listen(a2aGRPCTestBufferSize)
	grpcServer := grpc.NewServer()
	a2apb.RegisterA2AServiceServer(grpcServer, server)
	go func() {
		_ = grpcServer.Serve(listener)
	}()
	defer grpcServer.Stop()

	client, err := NewA2AGRPCClient(
		"passthrough:///bufnet",
		"agent-slug",
		WithA2AGRPCToken("ol_test"),
		WithA2AGRPCTransportCredentials(insecure.NewCredentials()),
		WithA2AGRPCDialOptions(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) {
			return listener.Dial()
		})),
	)
	if err != nil {
		t.Fatalf("NewA2AGRPCClient: %v", err)
	}
	defer client.Close()

	task, err := client.SendMessage(context.Background(), NewA2ATextMessageParams("msg-1", "hello", nil))
	if err != nil || task.ID != "task-grpc" {
		t.Fatalf("SendMessage = %#v, %v", task, err)
	}
	if server.lastTenant != "agent-slug" || server.lastAuthorization != "Bearer ol_test" {
		t.Fatalf("metadata tenant/auth = %q / %q", server.lastTenant, server.lastAuthorization)
	}

	if _, err := client.GetTask(context.Background(), A2ATaskQueryParams{ID: "task-grpc"}); err != nil {
		t.Fatalf("GetTask: %v", err)
	}
	pageSize := 5
	list, err := client.ListTasks(context.Background(), A2ATaskListParams{PageSize: &pageSize, Status: "completed"})
	if err != nil || len(list.Tasks) != 1 || list.PageSize != int32(pageSize) {
		t.Fatalf("ListTasks = %#v, %v", list, err)
	}
	canceled, err := client.CancelTask(context.Background(), A2ATaskQueryParams{ID: "task-grpc"})
	if err != nil || NormalizeA2ATaskState(canceled.Status.State) != "canceled" {
		t.Fatalf("CancelTask = %#v, %v", canceled, err)
	}

	var events []A2AStreamEvent
	if err := client.StreamMessage(context.Background(), NewA2ATextMessageParams("msg-stream", "hello", nil), func(event A2AStreamEvent) error {
		events = append(events, event)
		return nil
	}); err != nil {
		t.Fatalf("StreamMessage: %v", err)
	}
	if len(events) != 2 || events[1].Result.StatusUpdate == nil {
		t.Fatalf("stream events = %#v", events)
	}

	push, err := client.SetTaskPushNotificationConfig(context.Background(), A2ATaskPushConfigParams{
		ID: "task-grpc",
		PushNotificationConfig: A2APushNotificationConfig{
			URL:   "https://callback.example/a2a",
			Token: "push-token",
		},
	})
	if err != nil || push.ID != "cfg-1" || push.URL != "https://callback.example/a2a" {
		t.Fatalf("SetTaskPushNotificationConfig = %#v, %v", push, err)
	}
	configs, err := client.ListTaskPushNotificationConfigs(context.Background(), A2ATaskPushConfigParams{ID: "task-grpc"})
	if err != nil || len(configs.Configs) != 1 {
		t.Fatalf("ListTaskPushNotificationConfigs = %#v, %v", configs, err)
	}
}

func TestNormalizeA2AGRPCAgentCardJSON(t *testing.T) {
	raw := []byte(`{
		"name":"Agent",
		"description":"desc",
		"url":"https://example.com",
		"version":"v1",
		"supportedInterfaces":[{"url":"https://grpc.example.com","protocolBinding":"GRPC","protocolVersion":"1.0","tenant":"agent"}],
		"provider":{},
		"capabilities":{},
		"defaultInputModes":["text/plain"],
		"defaultOutputModes":["text/plain"],
		"skills":[],
		"securityRequirements":[{"schemes":{"openlinker_bearer":{"list":["agents:run","runs:read"]}}}]
	}`)
	normalized, err := normalizeA2AGRPCAgentCardJSON(raw)
	if err != nil {
		t.Fatal(err)
	}
	var card AgentCardResponse
	if err := json.Unmarshal(normalized, &card); err != nil {
		t.Fatal(err)
	}
	got := card.SecurityRequirements[0]["openlinker_bearer"]
	if len(got) != 2 || got[0] != "agents:run" || got[1] != "runs:read" {
		t.Fatalf("security requirements = %#v", card.SecurityRequirements)
	}
}
