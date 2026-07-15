package openlinker

import (
	"context"
	"errors"
	"testing"
)

type apiModeTextAgent struct{}

func (apiModeTextAgent) Run(context.Context, string) (string, error) { return "done", nil }

type apiModeLayoutAgent struct{}

func (apiModeLayoutAgent) Handle(context.Context, NativeRun) (NativeResult, error) {
	return Success(map[string]any{"layout": true}), nil
}

func TestRuntimeAPIModesCompile(t *testing.T) {
	t.Parallel()
	minimal := WithAgent(apiModeTextAgent{}).WithTransportMode(TransportAuto)
	registered := WithAgent(apiModeTextAgent{}).WithRegistration(AgentSpec{
		Slug: "api-mode-agent", Name: "API Mode Agent", Visibility: "private",
	})
	native := Native(func(ctx context.Context, run NativeRun) (any, error) {
		_ = run.MessageDelta(ctx, "working")
		_ = run.Emit(ctx, "example.trace", map[string]any{"ok": true})
		_ = run.Progress(ctx, 50, "halfway")
		return Success(map[string]any{"text": run.Text()}), nil
	}).WithRegistration(AgentSpec{Slug: "native-agent", Name: "Native Agent"})
	layoutNative := Native(apiModeLayoutAgent{}.Handle)

	store, err := OpenFileRuntimeStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = store.Close() })
	managed, err := NewRuntimeWorker(RuntimeWorkerConfig{
		RuntimeURL: "https://runtime.example.test", NodeID: testNodeID, AgentID: testAgentID,
		AgentToken: "ol_agent_test", Store: store, Transport: RuntimeTransportAuto,
		MTLS: RuntimeMTLSConfig{CertFile: "node.crt", KeyFile: "node.key", CAFile: "ca.crt"},
		Handler: RuntimeHandlerFunc(func(context.Context, RuntimeContext) (RuntimeResult, error) {
			return RuntimeResult{Status: "success"}, nil
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	if minimal == nil || registered == nil || native == nil || layoutNative == nil || managed == nil {
		t.Fatal("runtime API mode builder returned nil")
	}
	if Failure("FAILED", errors.New("failed")).Error == nil ||
		!RetryableFailure("RETRY", errors.New("retry")).Error.Retryable {
		t.Fatal("Native result helpers are not usable")
	}
}

func TestRuntimeCompatibilityAliasesCompile(t *testing.T) {
	t.Parallel()
	worker := Native(func(context.Context, NativeRun) (any, error) { return nil, nil })
	worker.WithConnector(string(RuntimeTransportWebSocket)).WithConnector(string(RuntimeTransportPull))
	if RuntimeConnectorWebSocket == "" || RuntimeConnectorPull == "" {
		t.Fatal("Runtime transport compatibility aliases are empty")
	}
}
