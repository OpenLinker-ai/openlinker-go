package main

import (
	"context"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"os"
	"strings"
	"sync"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"google.golang.org/genproto/googleapis/rpc/errdetails"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/status"
)

type manifest struct {
	APIRoot          string `json:"api_root"`
	GRPCEndpoint     string `json:"grpc_endpoint"`
	Token            string `json:"token"`
	Slug             string `json:"slug"`
	CancelableSlug   string `json:"cancelable_slug"`
	WebhookURL       string `json:"webhook_url"`
	JSONRPCTaskID    string `json:"jsonrpc_task_id"`
	JSONRPCContextID string `json:"jsonrpc_context_id"`
	Suffix           string `json:"suffix"`
}

type result struct {
	OK                          bool     `json:"ok"`
	GRPCEndpoint                string   `json:"grpc_endpoint"`
	Slug                        string   `json:"slug"`
	JSONRPCTaskID               string   `json:"jsonrpc_task_id"`
	GRPCTaskID                  string   `json:"grpc_task_id"`
	GRPCStreamEventsObserved    int      `json:"grpc_stream_events_observed"`
	GRPCCanceledTaskID          string   `json:"grpc_canceled_task_id"`
	GRPCSubscribeStates         []string `json:"grpc_subscribe_states"`
	PushConfigID                string   `json:"push_config_id"`
	PushConfigCredentialsHidden bool     `json:"push_config_credentials_hidden"`
	ErrorCode                   string   `json:"error_code"`
	ErrorReason                 string   `json:"error_reason"`
}

func main() {
	manifestPath := flag.String("manifest", "", "path to the A2A gRPC conformance manifest")
	flag.Parse()
	if strings.TrimSpace(*manifestPath) == "" {
		fatalf("-manifest is required")
	}
	var cfg manifest
	raw, err := os.ReadFile(*manifestPath)
	if err != nil {
		fatalf("read manifest: %v", err)
	}
	if err := json.Unmarshal(raw, &cfg); err != nil {
		fatalf("decode manifest: %v", err)
	}
	if cfg.GRPCEndpoint == "" || cfg.Token == "" || cfg.Slug == "" || cfg.CancelableSlug == "" || cfg.JSONRPCTaskID == "" {
		fatalf("manifest missing required gRPC conformance fields: %+v", cfg)
	}

	ctx, cancel := context.WithTimeout(context.Background(), 75*time.Second)
	defer cancel()

	client := newClient(ctx, cfg.GRPCEndpoint, cfg.Slug, cfg.Token)
	defer client.Close()

	card, err := client.GetExtendedAgentCard(ctx)
	must(err, "GetExtendedAgentCard")
	assert(card.Name == "A2A Protocol Conformance Agent", "unexpected extended card name: %q", card.Name)
	assert(hasGRPCInterface(card.SupportedInterfaces, cfg.Slug), "extended card missing GRPC interface for tenant %q", cfg.Slug)

	jsonRPCTask, err := client.GetTask(ctx, openlinker.A2ATaskQueryParams{ID: cfg.JSONRPCTaskID})
	must(err, "GetTask for JSON-RPC-created task")
	assert(jsonRPCTask.Status.State == "completed", "JSON-RPC-created task via gRPC state = %q", jsonRPCTask.Status.State)

	pageSize := 5
	list, err := client.ListTasks(ctx, openlinker.A2ATaskListParams{
		ContextID: cfg.JSONRPCContextID,
		PageSize:  &pageSize,
	})
	must(err, "ListTasks for JSON-RPC context")
	assert(taskListContains(list.Tasks, cfg.JSONRPCTaskID), "ListTasks did not include JSON-RPC-created task %s", cfg.JSONRPCTaskID)

	grpcTask, err := client.SendMessage(ctx, messageParams(cfg.Suffix, "grpc-send", "用 gRPC SendMessage 触发 A2A 调用", false))
	must(err, "SendMessage")
	assert(grpcTask.ID != "", "gRPC SendMessage returned empty task id")
	assert(grpcTask.Status.State == "completed", "gRPC SendMessage task state = %q", grpcTask.Status.State)

	var streamEvents int
	var streamCompleted bool
	err = client.StreamMessage(ctx, messageParams(cfg.Suffix, "grpc-stream", "用 gRPC SendStreamingMessage 验证 stream", false), func(event openlinker.A2AStreamEvent) error {
		streamEvents++
		if task := event.Result.Task; task != nil && task.Status.State == "completed" {
			streamCompleted = true
		}
		if update := event.Result.StatusUpdate; update != nil && update.Status.State == "completed" {
			streamCompleted = true
		}
		return nil
	})
	must(err, "SendStreamingMessage")
	assert(streamEvents > 0, "gRPC stream produced no events")
	assert(streamCompleted, "gRPC stream did not observe completed state")

	push, err := client.SetTaskPushNotificationConfig(ctx, openlinker.A2ATaskPushConfigParams{
		TaskID: grpcTask.ID,
		PushNotificationConfig: openlinker.A2APushNotificationConfig{
			URL: cfg.WebhookURL,
			Authentication: &openlinker.A2APushAuthenticationInfo{
				Scheme:      "Bearer",
				Credentials: "grpc-push-secret",
			},
			Metadata: map[string]any{
				"client": "a2a-grpc-conformance",
			},
			EventTypes: []string{"run.completed"},
		},
	})
	must(err, "CreateTaskPushNotificationConfig")
	assert(push.ID != "", "gRPC push config id is empty")
	assert(push.TaskID == grpcTask.ID, "gRPC push config task id = %q, want %q", push.TaskID, grpcTask.ID)
	assert(push.URL == cfg.WebhookURL, "gRPC push config URL = %q, want %q", push.URL, cfg.WebhookURL)
	credentialsHidden := push.Authentication == nil || push.Authentication.Credentials == ""
	assert(credentialsHidden, "gRPC push config must not echo credentials")

	gotPush, err := client.GetTaskPushNotificationConfig(ctx, openlinker.A2ATaskPushConfigParams{
		TaskID:                   grpcTask.ID,
		PushNotificationConfigID: push.ID,
	})
	must(err, "GetTaskPushNotificationConfig")
	assert(gotPush.ID == push.ID, "GetTaskPushNotificationConfig id = %q, want %q", gotPush.ID, push.ID)

	pushList, err := client.ListTaskPushNotificationConfigs(ctx, openlinker.A2ATaskPushConfigParams{TaskID: grpcTask.ID})
	must(err, "ListTaskPushNotificationConfigs")
	assert(pushConfigListContains(pushList.Configs, push.ID), "ListTaskPushNotificationConfigs missing %s", push.ID)

	err = client.DeleteTaskPushNotificationConfig(ctx, openlinker.A2ATaskPushConfigParams{
		TaskID:                   grpcTask.ID,
		PushNotificationConfigID: push.ID,
	})
	must(err, "DeleteTaskPushNotificationConfig")
	pushListAfterDelete, err := client.ListTaskPushNotificationConfigs(ctx, openlinker.A2ATaskPushConfigParams{TaskID: grpcTask.ID})
	must(err, "ListTaskPushNotificationConfigs after delete")
	assert(!pushConfigListContains(pushListAfterDelete.Configs, push.ID), "deleted push config %s still listed", push.ID)

	cancelClient := newClient(ctx, cfg.GRPCEndpoint, cfg.CancelableSlug, cfg.Token)
	defer cancelClient.Close()
	cancelableTask, err := cancelClient.SendMessage(ctx, messageParams(cfg.Suffix, "grpc-cancel", "用 gRPC 启动私有 Agent 任务并取消", true))
	must(err, "SendMessage cancelable task")
	assert(cancelableTask.ID != "", "gRPC cancelable task id is empty")
	assert(cancelableTask.Status.State == "working", "gRPC cancelable task initial state = %q", cancelableTask.Status.State)

	states, streamDone := subscribeTask(cancelClient, cancelableTask.ID)
	time.Sleep(600 * time.Millisecond)
	canceled, err := cancelClient.CancelTask(ctx, openlinker.A2ATaskQueryParams{ID: cancelableTask.ID})
	must(err, "CancelTask")
	assert(canceled.Status.State == "canceled", "gRPC canceled state = %q", canceled.Status.State)
	if err := waitStream(streamDone); err != nil {
		fatalf("SubscribeToTask: %v", err)
	}
	assert(containsState(states.snapshot(), "canceled"), "gRPC SubscribeToTask did not observe canceled state: %v", states.snapshot())

	_, err = client.GetTask(ctx, openlinker.A2ATaskQueryParams{ID: "00000000-0000-0000-0000-000000000000"})
	assert(err != nil, "GetTask for missing task unexpectedly succeeded")
	st, ok := status.FromError(err)
	assert(ok, "missing task error was not a gRPC status: %v", err)
	assert(st.Code() == codes.NotFound, "missing task gRPC code = %s, want NotFound", st.Code())
	reason := errorInfoReason(st)
	assert(reason == "TASK_NOT_FOUND", "missing task ErrorInfo reason = %q", reason)

	out := result{
		OK:                          true,
		GRPCEndpoint:                cfg.GRPCEndpoint,
		Slug:                        cfg.Slug,
		JSONRPCTaskID:               cfg.JSONRPCTaskID,
		GRPCTaskID:                  grpcTask.ID,
		GRPCStreamEventsObserved:    streamEvents,
		GRPCCanceledTaskID:          canceled.ID,
		GRPCSubscribeStates:         states.snapshot(),
		PushConfigID:                push.ID,
		PushConfigCredentialsHidden: credentialsHidden,
		ErrorCode:                   st.Code().String(),
		ErrorReason:                 reason,
	}
	encoded, err := json.MarshalIndent(out, "", "  ")
	must(err, "encode result")
	fmt.Println(string(encoded))
}

type stateRecorder struct {
	mu     sync.Mutex
	states []string
}

func (r *stateRecorder) add(state string) {
	state = strings.TrimSpace(state)
	if state == "" {
		return
	}
	r.mu.Lock()
	defer r.mu.Unlock()
	r.states = append(r.states, state)
}

func (r *stateRecorder) snapshot() []string {
	r.mu.Lock()
	defer r.mu.Unlock()
	return append([]string{}, r.states...)
}

func newClient(ctx context.Context, endpoint, tenant, token string) *openlinker.A2AGRPCClient {
	client, err := openlinker.NewA2AGRPCClient(
		endpoint,
		tenant,
		openlinker.WithA2AGRPCToken(token),
		openlinker.WithA2AGRPCSDKAgent("openlinker-go/a2a-grpc-conformance"),
	)
	must(err, "NewA2AGRPCClient")
	connectCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
	defer cancel()
	if _, err := client.GetExtendedAgentCard(connectCtx); err != nil {
		_ = client.Close()
		fatalf("connect to gRPC tenant %s: %v", tenant, err)
	}
	return client
}

func messageParams(suffix, label, text string, returnImmediately bool) openlinker.A2AMessageSendParams {
	return openlinker.A2AMessageSendParams{
		Message: openlinker.A2AMessage{
			MessageID: fmt.Sprintf("msg-%s-%s", label, suffix),
			ContextID: fmt.Sprintf("ctx-%s-%s", label, suffix),
			Role:      "user",
			Parts: []map[string]any{
				{"kind": "text", "text": text},
			},
			Metadata: map[string]any{
				"client": "a2a-grpc-conformance",
			},
		},
		Configuration: &openlinker.A2ASendConfiguration{
			AcceptedOutputModes: []string{"application/json", "text/plain"},
			ReturnImmediately:   &returnImmediately,
		},
		Metadata: map[string]any{
			"trace_id": fmt.Sprintf("a2a-grpc-%s-%s", label, suffix),
		},
	}
}

func subscribeTask(client *openlinker.A2AGRPCClient, taskID string) (*stateRecorder, <-chan error) {
	recorder := &stateRecorder{}
	done := make(chan error, 1)
	go func() {
		ctx, cancel := context.WithTimeout(context.Background(), 18*time.Second)
		defer cancel()
		done <- client.ResubscribeTask(ctx, openlinker.A2ATaskQueryParams{ID: taskID}, func(event openlinker.A2AStreamEvent) error {
			if task := event.Result.Task; task != nil {
				recorder.add(task.Status.State)
			}
			if update := event.Result.StatusUpdate; update != nil {
				recorder.add(update.Status.State)
			}
			return nil
		})
	}()
	return recorder, done
}

func waitStream(done <-chan error) error {
	select {
	case err := <-done:
		return err
	case <-time.After(20 * time.Second):
		return errors.New("timed out waiting for gRPC stream to finish")
	}
}

func hasGRPCInterface(items []openlinker.JSON, tenant string) bool {
	for _, item := range items {
		binding, _ := item["protocolBinding"].(string)
		if binding == "" {
			binding, _ = item["protocol_binding"].(string)
		}
		itemTenant, _ := item["tenant"].(string)
		if strings.EqualFold(binding, "GRPC") && itemTenant == tenant {
			return true
		}
	}
	return false
}

func taskListContains(tasks []openlinker.A2ATask, id string) bool {
	for _, task := range tasks {
		if task.ID == id {
			return true
		}
	}
	return false
}

func pushConfigListContains(configs []openlinker.A2ATaskPushNotificationConfig, id string) bool {
	for _, cfg := range configs {
		if cfg.ID == id {
			return true
		}
	}
	return false
}

func containsState(states []string, expected string) bool {
	for _, state := range states {
		if state == expected {
			return true
		}
	}
	return false
}

func errorInfoReason(st *status.Status) string {
	for _, detail := range st.Details() {
		if info, ok := detail.(*errdetails.ErrorInfo); ok {
			return info.Reason
		}
	}
	return ""
}

func assert(ok bool, format string, args ...any) {
	if !ok {
		fatalf(format, args...)
	}
}

func must(err error, label string) {
	if err != nil {
		fatalf("%s: %v", label, err)
	}
}

func fatalf(format string, args ...any) {
	fmt.Fprintf(os.Stderr, "A2A gRPC conformance failed: "+format+"\n", args...)
	os.Exit(1)
}
