package openlinker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestNativeRunExposesLayoutContextAndValidatedEvents(t *testing.T) {
	t.Parallel()
	var mu sync.Mutex
	events := make([]RuntimeEvent, 0, 3)
	runtime := RuntimeContext{
		RunID: testRunID, AgentID: testAgentID,
		AttemptIdentity:   RuntimeAttemptIdentity{RunID: testRunID, AttemptID: testAttemptID, AgentID: testAgentID},
		AttemptDeadlineAt: time.Now().Add(time.Minute).UTC(), RunDeadlineAt: time.Now().Add(2 * time.Minute).UTC(),
		Input: RuntimeJSONMap{"text": "hello"}, Metadata: RuntimeJSONMap{"session_key": "layout-session"},
	}
	runtime.emit = func(eventType string, payload any) error {
		mu.Lock()
		events = append(events, RuntimeEvent{EventType: eventType, Payload: payload})
		mu.Unlock()
		return nil
	}
	run := NativeRun{runtime: runtime, Assignment: RuntimeAssignment{
		AttemptIdentity: runtime.AttemptIdentity, Input: map[string]any{"text": "hello"},
		Metadata: map[string]any(runtime.Metadata), AttemptDeadlineAt: runtime.AttemptDeadlineAt, RunDeadlineAt: runtime.RunDeadlineAt,
	}}
	if run.Text() != "hello" || run.RunID() != testRunID || run.AttemptID() != testAttemptID || run.AgentID() != testAgentID {
		t.Fatalf("run = %#v", run)
	}
	if run.Metadata()["session_key"] != "layout-session" {
		t.Fatalf("metadata = %#v", run.Metadata())
	}
	deadline, ok := run.Deadline()
	if !ok || !deadline.Equal(runtime.AttemptDeadlineAt) {
		t.Fatalf("deadline = %v, %v", deadline, ok)
	}
	if err := run.Emit(context.Background(), "layout.trace.completed", struct {
		Steps int `json:"steps"`
	}{Steps: 5}); err != nil {
		t.Fatal(err)
	}
	if err := run.Progress(context.Background(), 50, "halfway"); err != nil {
		t.Fatal(err)
	}
	if err := run.MessageDelta(context.Background(), "done"); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(events) != 3 || events[0].EventType != "layout.trace.completed" ||
		events[1].EventType != AgentEventTypeRunProgressChanged || events[2].EventType != AgentEventTypeRunMessageDelta {
		t.Fatalf("events = %#v", events)
	}
}

func TestNativeRunRejectsInvalidEventsBeforeSpooling(t *testing.T) {
	t.Parallel()
	var calls atomic.Int32
	runtime := RuntimeContext{}
	runtime.emit = func(string, any) error { calls.Add(1); return nil }
	run := NativeRun{runtime: runtime}
	invalid := []func() error{
		func() error { return run.Emit(context.Background(), "Invalid Event", map[string]any{"ok": true}) },
		func() error { return run.Emit(context.Background(), "run.completed", map[string]any{"ok": true}) },
		func() error {
			return run.Emit(context.Background(), "layout.trace", map[string]any{"bad": make(chan int)})
		},
		func() error { return run.MessageDelta(context.Background(), "  ") },
		func() error { return run.Progress(context.Background(), 101, "invalid") },
	}
	for index, test := range invalid {
		if err := test(); err == nil {
			t.Fatalf("invalid event %d was accepted", index)
		}
	}
	if calls.Load() != 0 {
		t.Fatalf("invalid events reached RuntimeContext: %d", calls.Load())
	}
}

func TestNativeRunDelegationUsesStableKey(t *testing.T) {
	t.Parallel()
	const idempotencyKey = "layout-delegation-1"
	runtime := RuntimeContext{}
	runtime.callAgent = func(_ context.Context, target string, input any, options RuntimeCallOptions) (any, error) {
		if target != testTargetAgentID || options.IdempotencyKey != idempotencyKey || options.Reason != "tool delegation" {
			t.Fatalf("target=%q options=%#v", target, options)
		}
		return RuntimeJSONMap{"run_id": testRunID, "status": RuntimeRunRunning, "dispatch_state": RuntimeDispatchPending}, nil
	}
	run := NativeRun{runtime: runtime}
	summary, err := run.CallAgentWithRequest(context.Background(), RuntimeCallAgentRequest{
		TargetAgentID: testTargetAgentID, Input: map[string]any{"task": "inspect"}, Reason: "tool delegation",
	}, idempotencyKey)
	if err != nil || summary.RunID != testRunID {
		t.Fatalf("summary=%#v err=%v", summary, err)
	}
}

func TestNativeResultHelpersAndHandlerNormalization(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name      string
		handler   func(context.Context, NativeRun) (any, error)
		status    string
		errorCode string
	}{
		{name: "empty success", handler: func(context.Context, NativeRun) (any, error) { return nil, nil }, status: "success"},
		{name: "returned error", handler: func(context.Context, NativeRun) (any, error) { return nil, errors.New("failed") }, status: "failed", errorCode: "AGENT_RUNTIME_ERROR"},
		{name: "panic", handler: func(context.Context, NativeRun) (any, error) { panic("boom") }, status: "failed", errorCode: "AGENT_RUNTIME_PANIC"},
		{name: "invalid failed result", handler: func(context.Context, NativeRun) (any, error) { return NativeResult{Status: "failed"}, nil }, status: "failed", errorCode: "AGENT_RUNTIME_INVALID_RESULT"},
		{name: "invalid status", handler: func(context.Context, NativeRun) (any, error) { return NativeResult{Status: "pending"}, nil }, status: "failed", errorCode: "AGENT_RUNTIME_INVALID_RESULT"},
	}
	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			result := invokeNativeHandler(context.Background(), test.handler, NativeRun{})
			if result.Status != test.status || (test.errorCode != "" && (result.Error == nil || result.Error.Code != test.errorCode)) {
				t.Fatalf("result = %#v", result)
			}
		})
	}
	if result := RetryableFailure("LAYOUT_RETRY", errors.New("retry")); result.Error == nil || !result.Error.Retryable {
		t.Fatalf("retryable result = %#v", result)
	}
}

func TestRuntimeHighLevelRunnerUsesCanonicalWorker(t *testing.T) {
	t.Parallel()
	client := newFakeRuntimeClient()
	var claimed atomic.Bool
	client.claimFn = func(ctx context.Context, _ int, request RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		if claimed.CompareAndSwap(false, true) {
			return assignedRunForHello(client.helloSnapshot()), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	var resultStatus atomic.Value
	client.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		resultStatus.Store(request.Status)
		return successfulResultACK(request.ResultID), nil
	}
	store, err := OpenFileRuntimeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	runner := WithFunc(func(context.Context, string) (string, error) { return "done", nil }).
		WithNodeID(testNodeID).WithAgentID(testAgentID).WithStore(store).WithMaxRuns(1)
	runner.runtimeClient = client
	if err = runner.Run(context.Background()); err != nil {
		t.Fatal(err)
	}
	if value := resultStatus.Load(); value != "success" {
		t.Fatalf("result status = %#v", value)
	}
}

func TestRuntimeDoesNotCreateAgentFromUserTokenAlone(t *testing.T) {
	t.Chdir(t.TempDir())
	clearRuntimeConfigEnv(t)
	t.Setenv("OPENLINKER_USER_TOKEN", "ol_user_secret")
	runner := Native(func(context.Context, NativeRun) (any, error) { return Success(nil), nil })
	if _, err := runner.buildWorker(context.Background()); err == nil {
		t.Fatal("missing Runtime identity unexpectedly succeeded")
	}
}
