package main

import (
	"bytes"
	"context"
	"net"
	"strings"
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/a2apb"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/test/bufconn"
)

type testA2AServer struct {
	a2apb.UnimplementedA2AServiceServer
	authorization string
	tenant        string
}

func (server *testA2AServer) SendMessage(ctx context.Context, request *a2apb.SendMessageRequest) (*a2apb.SendMessageResponse, error) {
	server.tenant = request.GetTenant()
	if values := metadata.ValueFromIncomingContext(ctx, "authorization"); len(values) > 0 {
		server.authorization = values[0]
	}
	return &a2apb.SendMessageResponse{Payload: &a2apb.SendMessageResponse_Task{Task: &a2apb.Task{
		Id: "task-grpc-example", ContextId: request.GetMessage().GetContextId(), Status: &a2apb.TaskStatus{State: a2apb.TaskState_TASK_STATE_COMPLETED},
	}}}, nil
}

func TestRunUsesGRPCBinding(t *testing.T) {
	listener := bufconn.Listen(1024 * 1024)
	service := &testA2AServer{}
	grpcServer := grpc.NewServer()
	a2apb.RegisterA2AServiceServer(grpcServer, service)
	go func() { _ = grpcServer.Serve(listener) }()
	defer grpcServer.Stop()
	cfg := config{Endpoint: "passthrough:///bufnet", Tenant: "agent-tenant", Token: "a2a-token", Input: "hello", MessageID: "message-1", Options: []openlinker.A2AGRPCClientOption{
		openlinker.WithA2AGRPCTransportCredentials(insecure.NewCredentials()),
		openlinker.WithA2AGRPCDialOptions(grpc.WithContextDialer(func(context.Context, string) (net.Conn, error) { return listener.Dial() })),
	}}
	var output bytes.Buffer
	if err := run(context.Background(), cfg, &output); err != nil {
		t.Fatal(err)
	}
	if service.tenant != "agent-tenant" || service.authorization != "Bearer a2a-token" || !strings.Contains(output.String(), "task-grpc-example") {
		t.Fatalf("tenant=%s auth=%s output=%s", service.tenant, service.authorization, output.String())
	}
}
