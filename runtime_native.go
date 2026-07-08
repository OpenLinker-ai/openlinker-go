package openlinker

import (
	"context"
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"
)

const (
	RuntimeConnectorPull          = "runtime_pull"
	RuntimeConnectorWebSocket     = "runtime_ws"
	AgentEventTypeRunMessageDelta = "run.message.delta"

	defaultNativeAPIBase  = "https://api.openlinker.ai"
	defaultNativeSDKAgent = "openlinker-go/native"
)

type NativeHandler func(context.Context, NativeRun) (any, error)

type NativeRun struct {
	Assignment RuntimeAssignment
	reporter   NativeReporter
}

func (r NativeRun) Text(keys ...string) string {
	if len(keys) == 0 {
		keys = []string{"text", "query", "task", "prompt"}
	}
	return nativeInputText(r.Assignment.Input, keys)
}

func (r NativeRun) SendEvent(ctx context.Context, event AgentEvent) error {
	return r.reporter.SendEvent(ctx, event)
}

func (r NativeRun) MessageDelta(ctx context.Context, text string) error {
	return r.SendEvent(ctx, AgentEvent{
		EventType: AgentEventTypeRunMessageDelta,
		Payload:   JSON{"text": text},
	})
}

func (r NativeRun) SupportsLiveEvents() bool {
	return r.reporter.SupportsLiveEvents()
}

type NativeReporter struct {
	connector RuntimeConnector
	runID     string
}

func (r NativeReporter) SupportsLiveEvents() bool {
	return r.connector != nil && r.connector.SupportsLiveEvents()
}

func (r NativeReporter) SendEvent(ctx context.Context, event AgentEvent) error {
	if !r.SupportsLiveEvents() {
		return nil
	}
	return r.connector.SendRunEvent(ctx, r.runID, event)
}

type NativeResult struct {
	Status string
	Output any
	Events []AgentEvent
	Error  *AgentError
}

type NativeRunner struct {
	Handler      NativeHandler
	Runtime      *Runtime
	Client       *Client
	APIBase      string
	RuntimeToken string
	Connector    string
	PullWait     time.Duration
	MaxRuns      int
	SDKAgent     string
	OnReady      func(RuntimeWSServerMessage)
	OnError      func(error)
}

func Native(handler NativeHandler) *NativeRunner {
	return &NativeRunner{Handler: handler}
}

func (r *NativeRunner) WithClient(client *Client) *NativeRunner {
	r.Client = client
	return r
}

func (r *NativeRunner) WithRuntime(runtime *Runtime) *NativeRunner {
	r.Runtime = runtime
	return r
}

func (r *NativeRunner) WithAPIBase(apiBase string) *NativeRunner {
	r.APIBase = strings.TrimSpace(apiBase)
	return r
}

func (r *NativeRunner) WithRuntimeToken(token string) *NativeRunner {
	r.RuntimeToken = strings.TrimSpace(token)
	return r
}

func (r *NativeRunner) WithConnector(connector string) *NativeRunner {
	r.Connector = strings.TrimSpace(connector)
	return r
}

func (r *NativeRunner) WithPullWait(wait time.Duration) *NativeRunner {
	r.PullWait = wait
	return r
}

func (r *NativeRunner) WithMaxRuns(maxRuns int) *NativeRunner {
	r.MaxRuns = maxRuns
	return r
}

func (r *NativeRunner) WithSDKAgent(agent string) *NativeRunner {
	r.SDKAgent = strings.TrimSpace(agent)
	return r
}

func (r *NativeRunner) WithReadyHandler(fn func(RuntimeWSServerMessage)) *NativeRunner {
	r.OnReady = fn
	return r
}

func (r *NativeRunner) WithErrorHandler(fn func(error)) *NativeRunner {
	r.OnError = fn
	return r
}

func (r *NativeRunner) Run(ctx context.Context) error {
	if r == nil {
		return errors.New("openlinker: native runner is nil")
	}
	if r.Handler == nil {
		return errors.New("openlinker: native handler is required")
	}
	if ctx == nil {
		ctx = context.Background()
	}
	client, err := r.runtimeClient()
	if err != nil {
		return err
	}
	maxRuns := r.effectiveMaxRuns()
	connector, err := r.runtimeConnector(client, maxRuns)
	if err != nil {
		return err
	}

	runCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	var completed atomic.Int64
	var stopAfterMax atomic.Bool
	errCh := make(chan error, 1)
	if err := connector.Start(runCtx, RuntimeHandlers{
		OnReady: r.OnReady,
		OnAssigned: func(assignment RuntimeAssignment) {
			go r.handleAssignment(runCtx, connector, assignment, maxRuns, &completed, &stopAfterMax, cancel, errCh)
		},
		OnError: func(err error) {
			if r.OnError != nil {
				r.OnError(err)
			}
		},
	}); err != nil {
		return err
	}

	select {
	case <-runCtx.Done():
	case err := <-errCh:
		cancel()
		stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer stopCancel()
		_ = connector.Stop(stopCtx)
		return err
	}

	stopCtx, stopCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer stopCancel()
	if err := connector.Stop(stopCtx); err != nil && !stopAfterMax.Load() {
		return err
	}
	if stopAfterMax.Load() {
		return nil
	}
	return ctx.Err()
}

func (r *NativeRunner) runtimeClient() (*Runtime, error) {
	if r.Runtime != nil {
		return r.Runtime, nil
	}
	if r.Client != nil {
		return &Runtime{client: r.Client}, nil
	}
	apiBase := firstNativeNonEmpty(r.APIBase, os.Getenv("OPENLINKER_API_BASE"), defaultNativeAPIBase)
	token := firstNativeNonEmpty(r.RuntimeToken, os.Getenv("OPENLINKER_RUNTIME_TOKEN"), os.Getenv("OPENLINKER_AGENT_TOKEN"))
	if token == "" {
		return nil, errors.New("openlinker: OPENLINKER_RUNTIME_TOKEN is required")
	}
	sdkAgent := firstNativeNonEmpty(r.SDKAgent, defaultNativeSDKAgent)
	return NewRuntime(apiBase, WithRuntimeToken(token), WithSDKAgent(sdkAgent))
}

func (r *NativeRunner) effectiveMaxRuns() int {
	return firstNativeInt(r.MaxRuns, "OPENLINKER_WORKER_MAX_RUNS", 0)
}

func (r *NativeRunner) runtimeConnector(runtime *Runtime, maxRuns int) (RuntimeConnector, error) {
	mode := firstNativeNonEmpty(r.Connector, os.Getenv("OPENLINKER_WORKER_CONNECTOR"), RuntimeConnectorPull)
	switch mode {
	case RuntimeConnectorPull:
		conn := NewRuntimePullConnector(runtime)
		conn.Wait = firstNativeDuration(r.PullWait, "OPENLINKER_WORKER_PULL_WAIT", 25*time.Second)
		conn.MaxRuns = maxRuns
		return conn, nil
	case RuntimeConnectorWebSocket:
		return NewRuntimeWSConnector(runtime), nil
	default:
		return nil, fmt.Errorf("openlinker: runtime connector must be %q or %q", RuntimeConnectorPull, RuntimeConnectorWebSocket)
	}
}

func (r *NativeRunner) handleAssignment(
	ctx context.Context,
	connector RuntimeConnector,
	assignment RuntimeAssignment,
	maxRuns int,
	completed *atomic.Int64,
	stopAfterMax *atomic.Bool,
	cancel context.CancelFunc,
	errCh chan<- error,
) {
	started := time.Now()
	result := r.invokeHandler(ctx, connector, assignment)
	result.DurationMS = nativeDurationMS(started)

	completeCtx, completeCancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer completeCancel()
	if err := connector.CompleteRun(completeCtx, assignment.RunID, result); err != nil {
		select {
		case errCh <- err:
		default:
		}
		return
	}

	if maxRuns > 0 && completed.Add(1) >= int64(maxRuns) {
		stopAfterMax.Store(true)
		cancel()
	}
}

func (r *NativeRunner) invokeHandler(ctx context.Context, connector RuntimeConnector, assignment RuntimeAssignment) (result RuntimePullResultRequest) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = nativeRuntimePanicResult(recovered)
		}
	}()
	output, err := r.Handler(ctx, NativeRun{
		Assignment: assignment,
		reporter: NativeReporter{
			connector: connector,
			runID:     assignment.RunID,
		},
	})
	return nativeRuntimeResult(output, err)
}

func nativeRuntimeResult(output any, err error) RuntimePullResultRequest {
	if err != nil {
		return RuntimePullResultRequest{
			Status: "failed",
			Error: &AgentError{
				Code:    "AGENT_RUNTIME_ERROR",
				Message: err.Error(),
			},
		}
	}
	var result NativeResult
	switch value := output.(type) {
	case NativeResult:
		result = value
	case *NativeResult:
		if value != nil {
			result = *value
		}
	default:
		result = NativeResult{Output: output}
	}
	if result.Status == "" {
		result.Status = "success"
	}
	if result.Error != nil && result.Status == "success" {
		result.Status = "failed"
	}
	return RuntimePullResultRequest{
		Status: result.Status,
		Output: result.Output,
		Events: result.Events,
		Error:  result.Error,
	}
}

func nativeRuntimePanicResult(recovered any) RuntimePullResultRequest {
	return RuntimePullResultRequest{
		Status: "failed",
		Error: &AgentError{
			Code:    "AGENT_RUNTIME_PANIC",
			Message: fmt.Sprintf("agent panic: %v", recovered),
		},
	}
}

func nativeInputText(input any, keys []string) string {
	switch value := input.(type) {
	case nil:
		return ""
	case string:
		return strings.TrimSpace(value)
	case map[string]any:
		for _, key := range keys {
			if text := strings.TrimSpace(fmt.Sprint(value[key])); text != "" && text != "<nil>" {
				return text
			}
		}
	case JSON:
		return nativeInputText(map[string]any(value), keys)
	}
	return strings.TrimSpace(fmt.Sprint(input))
}

func nativeDurationMS(started time.Time) int32 {
	ms := time.Since(started).Milliseconds()
	if ms < 1 {
		return 1
	}
	if ms > int64(^uint32(0)>>1) {
		return int32(^uint32(0) >> 1)
	}
	return int32(ms)
}

func firstNativeNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}

func firstNativeDuration(value time.Duration, envKey string, fallback time.Duration) time.Duration {
	if value > 0 {
		return value
	}
	raw := strings.TrimSpace(os.Getenv(envKey))
	if raw == "" {
		return fallback
	}
	if parsed, err := time.ParseDuration(raw); err == nil && parsed > 0 {
		return parsed
	}
	if seconds, err := strconv.Atoi(raw); err == nil && seconds > 0 {
		return time.Duration(seconds) * time.Second
	}
	return fallback
}

func firstNativeInt(value int, envKey string, fallback int) int {
	if value > 0 {
		return value
	}
	raw := strings.TrimSpace(os.Getenv(envKey))
	if raw == "" {
		return fallback
	}
	parsed, err := strconv.Atoi(raw)
	if err != nil || parsed < 0 {
		return fallback
	}
	return parsed
}
