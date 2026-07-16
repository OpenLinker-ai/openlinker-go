package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"net/http"
	"os"
	"strings"
	"sync/atomic"
	"time"
	"unicode/utf8"
)

const (
	AgentEventTypeRunMessageDelta    = "run.message.delta"
	AgentEventTypeRunProgressChanged = "run.progress.changed"
)

type AgentEvent = RuntimeEvent
type AgentError = RuntimeHandlerError
type NativeResult = RuntimeResult

// RuntimeAssignment is the framework-facing assignment view. Invocation
// credentials remain private inside RuntimeWorker.
type RuntimeAssignment struct {
	AttemptIdentity   RuntimeAttemptIdentity
	Input             map[string]any
	Metadata          map[string]any
	AttemptDeadlineAt time.Time
	RunDeadlineAt     time.Time
}

// NativeRun exposes the confirmed assignment and safe attempt-scoped helpers.
type NativeRun struct {
	Assignment RuntimeAssignment
	runtime    RuntimeContext
}

func (run NativeRun) Text(keys ...string) string {
	if len(keys) == 0 {
		keys = []string{"text", "query", "task", "prompt"}
	}
	for _, key := range keys {
		if value, ok := run.Assignment.Input[key]; ok {
			if text := strings.TrimSpace(fmt.Sprint(value)); text != "" && text != "<nil>" {
				return text
			}
		}
	}
	return ""
}

func (run NativeRun) Identity() RuntimeAttemptIdentity { return run.Assignment.AttemptIdentity }
func (run NativeRun) RunID() string                    { return run.Assignment.AttemptIdentity.RunID }
func (run NativeRun) AttemptID() string                { return run.Assignment.AttemptIdentity.AttemptID }
func (run NativeRun) AgentID() string                  { return run.Assignment.AttemptIdentity.AgentID }
func (run NativeRun) Metadata() map[string]any         { return run.Assignment.Metadata }
func (run NativeRun) Deadline() (time.Time, bool)      { return run.runtime.Deadline() }
func (run NativeRun) SupportsLiveEvents() bool         { return run.runtime.emit != nil }

// SendEvent durably journals an event through the canonical RuntimeStore.
func (run NativeRun) SendEvent(ctx context.Context, event AgentEvent) error {
	if ctx != nil && ctx.Err() != nil {
		return ctx.Err()
	}
	event, err := normalizeNativeEvent(event)
	if err != nil {
		return err
	}
	return run.runtime.Emit(event.EventType, event.Payload)
}

// Emit sends a custom Runtime event whose payload encodes to a JSON object.
func (run NativeRun) Emit(ctx context.Context, eventType string, payload any) error {
	object, err := nativeEventPayload(payload)
	if err != nil {
		return err
	}
	return run.SendEvent(ctx, AgentEvent{EventType: eventType, Payload: object})
}

func (run NativeRun) Message(ctx context.Context, text string) error {
	return run.MessageDelta(ctx, text)
}

func (run NativeRun) MessageDelta(ctx context.Context, text string) error {
	if !utf8.ValidString(text) || strings.TrimSpace(text) == "" {
		return errors.New("openlinker: Native message text is required")
	}
	return run.Emit(ctx, AgentEventTypeRunMessageDelta, map[string]any{"text": text})
}

func (run NativeRun) Progress(ctx context.Context, percent int, message string) error {
	if percent < 0 || percent > 100 {
		return errors.New("openlinker: progress percent must be between 0 and 100")
	}
	return run.Emit(ctx, AgentEventTypeRunProgressChanged, map[string]any{"percent": percent, "message": message})
}

func (run NativeRun) CallAgent(ctx context.Context, targetAgentID string, input map[string]any, reason string) (*RuntimeRunSummary, error) {
	key, err := newRuntimeUUID()
	if err != nil {
		return nil, err
	}
	return run.CallAgentWithRequest(ctx, RuntimeCallAgentRequest{
		TargetAgentID: targetAgentID, Input: input, Reason: reason,
	}, key)
}

func (run NativeRun) CallAgentWithRequest(ctx context.Context, request RuntimeCallAgentRequest, idempotencyKey string) (*RuntimeRunSummary, error) {
	value, err := run.runtime.CallAgent(ctx, request.TargetAgentID, request.Input, RuntimeCallOptions{
		IdempotencyKey: idempotencyKey, Reason: request.Reason, Metadata: request.Metadata,
	})
	if err != nil {
		return nil, err
	}
	switch typed := value.(type) {
	case *RuntimeRunSummary:
		return typed, nil
	case RuntimeRunSummary:
		return &typed, nil
	case RuntimeJSONMap:
		return runtimeRunSummaryFromMap(map[string]any(typed))
	case map[string]any:
		return runtimeRunSummaryFromMap(typed)
	default:
		return nil, errors.New("openlinker: delegated Agent response has an unexpected shape")
	}
}

func runtimeRunSummaryFromMap(value map[string]any) (*RuntimeRunSummary, error) {
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, err
	}
	var summary RuntimeRunSummary
	if err = json.Unmarshal(raw, &summary); err != nil || summary.RunID == "" {
		return nil, errors.New("openlinker: delegated Agent response has an unexpected shape")
	}
	return &summary, nil
}

type nativeRunContextKey struct{}

func ContextWithNativeRun(ctx context.Context, run NativeRun) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, nativeRunContextKey{}, run)
}

func NativeRunFromContext(ctx context.Context) (NativeRun, bool) {
	if ctx == nil {
		return NativeRun{}, false
	}
	run, ok := ctx.Value(nativeRunContextKey{}).(NativeRun)
	return run, ok
}

type NativeTextAgent interface {
	Run(context.Context, string) (string, error)
}

type NativeTextAgentFunc func(context.Context, string) (string, error)

func (function NativeTextAgentFunc) Run(ctx context.Context, input string) (string, error) {
	if function == nil {
		return "", errors.New("openlinker: native text function is nil")
	}
	return function(ctx, input)
}

// NativeAgentRunner is the high-level facade over the canonical RuntimeWorker.
type NativeAgentRunner struct {
	handler       func(context.Context, NativeRun) (any, error)
	agent         NativeTextAgent
	model         string
	fallbackInput string

	config        RuntimeWorkerConfig
	runtime       *Runtime
	runtimeClient RuntimeClient
	httpClient    *http.Client
	userToken     string
	registration  any
	registerOpts  []RegistrationOption
	maxRuns       int
	onReady       func(RuntimeReadyPayload)
	onError       func(error)
}

func WithAgent(agent NativeTextAgent) *NativeAgentRunner {
	return &NativeAgentRunner{agent: agent, fallbackInput: "hello"}
}

func WithFunc(function func(context.Context, string) (string, error)) *NativeAgentRunner {
	return WithAgent(NativeTextAgentFunc(function))
}

func Native[T any](handler func(context.Context, NativeRun) (T, error)) *NativeAgentRunner {
	runner := &NativeAgentRunner{fallbackInput: "hello"}
	if handler != nil {
		runner.handler = func(ctx context.Context, run NativeRun) (any, error) { return handler(ctx, run) }
	}
	return runner
}

func (runner *NativeAgentRunner) WithModel(value string) *NativeAgentRunner {
	runner.model = strings.TrimSpace(value)
	return runner
}
func (runner *NativeAgentRunner) WithSDKAgent(value string) *NativeAgentRunner {
	runner.config.NodeVersion = strings.TrimSpace(value)
	return runner
}
func (runner *NativeAgentRunner) WithFallbackInput(value string) *NativeAgentRunner {
	runner.fallbackInput = strings.TrimSpace(value)
	return runner
}
func (runner *NativeAgentRunner) WithRuntime(value *Runtime) *NativeAgentRunner {
	runner.runtime = value
	return runner
}
func (runner *NativeAgentRunner) WithStore(value RuntimeStore) *NativeAgentRunner {
	runner.config.Store = value
	return runner
}
func (runner *NativeAgentRunner) WithHTTPClient(value *http.Client) *NativeAgentRunner {
	runner.httpClient = value
	return runner
}
func (runner *NativeAgentRunner) WithAPIBase(value string) *NativeAgentRunner {
	runner.config.PlatformURL = strings.TrimSpace(value)
	return runner
}
func (runner *NativeAgentRunner) WithRuntimeBase(value string) *NativeAgentRunner {
	runner.config.RuntimeURL = strings.TrimSpace(value)
	return runner
}
func (runner *NativeAgentRunner) WithUserToken(value string) *NativeAgentRunner {
	runner.userToken = strings.TrimSpace(value)
	return runner
}
func (runner *NativeAgentRunner) WithAgentToken(value string) *NativeAgentRunner {
	runner.config.AgentToken = strings.TrimSpace(value)
	return runner
}
func (runner *NativeAgentRunner) WithAgentID(value string) *NativeAgentRunner {
	runner.config.AgentID = strings.TrimSpace(value)
	return runner
}
func (runner *NativeAgentRunner) WithNodeID(value string) *NativeAgentRunner {
	runner.config.NodeID = strings.TrimSpace(value)
	return runner
}
func (runner *NativeAgentRunner) WithDataDir(value string) *NativeAgentRunner {
	runner.config.DataDir = strings.TrimSpace(value)
	return runner
}

// WithStatePath is retained for the pre-sync facade. The reliable store now
// owns a directory, so a .json suffix is converted to a sibling directory.
func (runner *NativeAgentRunner) WithStatePath(value string) *NativeAgentRunner {
	runner.config.DataDir = legacyRuntimeDataDir(strings.TrimSpace(value))
	return runner
}
func (runner *NativeAgentRunner) WithTransportMode(value TransportMode) *NativeAgentRunner {
	runner.config.Transport = value
	return runner
}

// Deprecated: use WithTransportMode.
func (runner *NativeAgentRunner) WithConnector(value string) *NativeAgentRunner {
	if mode, ok := normalizeFacadeTransport(value); ok {
		runner.config.Transport = mode
	} else {
		runner.config.Transport = RuntimeTransportMode(value)
	}
	return runner
}
func (runner *NativeAgentRunner) WithCapacity(value int64) *NativeAgentRunner {
	runner.config.Capacity = value
	return runner
}
func (runner *NativeAgentRunner) WithMaxRuns(value int) *NativeAgentRunner {
	runner.maxRuns = value
	return runner
}
func (runner *NativeAgentRunner) WithLogger(value *log.Logger) *NativeAgentRunner {
	runner.config.Logger = value
	return runner
}
func (runner *NativeAgentRunner) WithReadyHandler(value func(RuntimeReadyPayload)) *NativeAgentRunner {
	runner.onReady = value
	return runner
}
func (runner *NativeAgentRunner) WithErrorHandler(value func(error)) *NativeAgentRunner {
	runner.onError = value
	return runner
}
func (runner *NativeAgentRunner) WithRegistration(spec AgentSpec, options ...RegistrationOption) *NativeAgentRunner {
	runner.registration = spec
	runner.registerOpts = append([]RegistrationOption(nil), options...)
	return runner
}

func (runner *NativeAgentRunner) Register(ctx context.Context, input any, options ...RegistrationOption) (*AgentRegistration, error) {
	if runner == nil {
		return nil, errors.New("openlinker: native Agent runner is nil")
	}
	request, err := resolveEnsureAgentRequest(input, options...)
	if err != nil {
		return nil, err
	}
	request.APIBase = firstNonEmpty(request.APIBase, runner.config.PlatformURL, os.Getenv(EnvAPIBase))
	request.UserToken = firstNonEmpty(request.UserToken, runner.userToken, os.Getenv("OPENLINKER_USER_TOKEN"))
	request.AgentToken = firstNonEmpty(request.AgentToken, runner.config.AgentToken, os.Getenv(EnvAgentToken))
	registration, err := EnsureAgent(ctx, request)
	if err != nil {
		return nil, err
	}
	runner.config.PlatformURL = registration.APIBase
	runner.config.AgentID = registration.AgentID
	runner.config.AgentToken = registration.AgentToken
	return registration, nil
}

func (runner *NativeAgentRunner) RunOrRegister(ctx context.Context, input any, options ...RegistrationOption) error {
	if _, err := runner.Register(ctx, input, options...); err != nil {
		return err
	}
	return runner.Run(ctx)
}

func (runner *NativeAgentRunner) Run(ctx context.Context) error {
	worker, err := runner.buildWorker(ctx)
	if err != nil {
		return err
	}
	err = worker.Start(ctx)
	if err != nil && runner.onError != nil {
		runner.onError(err)
	}
	return err
}

func (runner *NativeAgentRunner) buildWorker(ctx context.Context) (*RuntimeWorker, error) {
	if runner == nil || (runner.handler == nil && runner.agent == nil) {
		return nil, errors.New("openlinker: native Agent handler is required")
	}
	if runner.registration != nil {
		if _, err := runner.Register(ctx, runner.registration, runner.registerOpts...); err != nil {
			return nil, err
		}
	}
	environment, err := LoadRuntimeWorkerConfig()
	if err != nil {
		return nil, err
	}
	config := mergeRuntimeWorkerConfig(environment, runner.config)
	if config.AgentID == "" || config.AgentToken == "" || config.PlatformURL == "" {
		stored, loadErr := NewEnvRegistrationStore(DefaultRegistrationEnvPath).LoadAgentRegistration()
		if loadErr != nil {
			return nil, loadErr
		}
		if stored != nil {
			config.AgentID = firstNonEmpty(config.AgentID, stored.AgentID)
			config.AgentToken = firstNonEmpty(config.AgentToken, stored.AgentToken)
			config.PlatformURL = firstNonEmpty(config.PlatformURL, stored.APIBase)
		}
	}
	if config.DataDir == "" && config.Store == nil && config.AgentID != "" {
		config.DataDir = defaultRuntimeDataDir(config.AgentID)
	}
	config.Handler = RuntimeHandlerFunc(runner.handleRuntime)
	config.OnReady = runner.onReady

	runtimeClient := runner.runtimeClient
	var dialer RuntimeTransportDialer
	if runner.runtime != nil {
		runtimeClient = runner.runtime
		dialer = &sdkRuntimeTransportDialer{runtime: runner.runtime}
	} else if runtimeClient == nil && runner.httpClient != nil {
		if config.RuntimeURL == "" {
			return nil, errors.New("openlinker: Runtime URL is required with an injected HTTP client")
		}
		runtime, runtimeErr := NewRuntime(config.RuntimeURL,
			WithAgentToken(config.AgentToken), WithHTTPClient(runner.httpClient),
			WithSDKAgent(firstNonEmpty(config.NodeVersion, runtimeWorkerSDKAgent)))
		if runtimeErr != nil {
			return nil, runtimeErr
		}
		runtimeClient = runtime
		dialer = &sdkRuntimeTransportDialer{runtime: runtime}
	}
	if err = config.Validate(runtimeClient == nil); err != nil {
		return nil, err
	}
	worker, err := newRuntimeWorker(config, runtimeClient, dialer)
	if err != nil {
		return nil, err
	}
	if runner.maxRuns > 0 {
		var completed atomic.Int64
		base := worker.Handler
		worker.Handler = RuntimeHandlerFunc(func(handlerCtx context.Context, runtime RuntimeContext) (RuntimeResult, error) {
			result, handlerErr := base.Handle(handlerCtx, runtime)
			if completed.Add(1) >= int64(runner.maxRuns) {
				go func() { _ = worker.Stop(context.Background()) }()
			}
			return result, handlerErr
		})
	}
	return worker, nil
}

func (runner *NativeAgentRunner) handleRuntime(ctx context.Context, runtime RuntimeContext) (RuntimeResult, error) {
	run := NativeRun{runtime: runtime, Assignment: RuntimeAssignment{
		AttemptIdentity: runtime.AttemptIdentity,
		Input:           runtimeInputMap(runtime.Input), Metadata: map[string]any(runtime.Metadata),
		AttemptDeadlineAt: runtime.AttemptDeadlineAt, RunDeadlineAt: runtime.RunDeadlineAt,
	}}
	ctx = ContextWithNativeRun(ctx, run)
	if runner.agent != nil {
		input := run.Text()
		if input == "" {
			input = runner.fallbackInput
		}
		answer, err := runner.agent.Run(ctx, input)
		if err != nil {
			return Failure("AGENT_WORKER_ERROR", err), nil
		}
		answer = strings.TrimSpace(answer)
		if answer == "" {
			return Failure("AGENT_WORKER_ERROR", errors.New("agent returned empty response")), nil
		}
		output := map[string]any{"text": answer, "input": map[string]any{"text": input, "raw": run.Assignment.Input}}
		if runner.model != "" {
			output["llm"] = map[string]any{"text": answer, "run_id": run.RunID(), "agent_id": run.AgentID(), "model": runner.model}
		}
		return NativeResult{Status: "success", Output: output,
			Events: []RuntimeEvent{{EventType: AgentEventTypeRunMessageDelta, Payload: map[string]any{"text": answer}}}}, nil
	}
	return invokeNativeHandler(ctx, runner.handler, run), nil
}

func invokeNativeHandler(ctx context.Context, handler func(context.Context, NativeRun) (any, error), run NativeRun) (result NativeResult) {
	defer func() {
		if recovered := recover(); recovered != nil {
			result = Failure("AGENT_RUNTIME_PANIC", fmt.Errorf("agent panic: %v", recovered))
		}
	}()
	value, err := handler(ctx, run)
	if err != nil {
		return Failure("AGENT_RUNTIME_ERROR", err)
	}
	switch typed := value.(type) {
	case NativeResult:
		result = typed
	case *NativeResult:
		if typed != nil {
			result = *typed
		}
	default:
		result = Success(value)
	}
	return normalizeNativeResult(result)
}

func Success(output any) NativeResult {
	return NativeResult{Status: "success", Output: output}
}

func Failure(code string, err error) NativeResult {
	message := "Agent failed"
	if err != nil {
		message = err.Error()
	}
	return NativeResult{Status: "failed", Error: &AgentError{Code: code, Message: message}}
}

func RetryableFailure(code string, err error) NativeResult {
	result := Failure(code, err)
	result.Error.Retryable = true
	return result
}

func normalizeNativeResult(result NativeResult) NativeResult {
	if result.Error != nil {
		result.Status = "failed"
		return result
	}
	switch result.Status {
	case "", "success":
		result.Status = "success"
		return result
	case "failed":
		return Failure("AGENT_RUNTIME_INVALID_RESULT", errors.New("handler returned failed status without an AgentError"))
	default:
		return Failure("AGENT_RUNTIME_INVALID_RESULT", fmt.Errorf("handler returned unsupported status %q", result.Status))
	}
}

func normalizeNativeEvent(event AgentEvent) (AgentEvent, error) {
	event.EventType = strings.TrimSpace(event.EventType)
	if !runtimeEventTypePattern.MatchString(event.EventType) {
		return AgentEvent{}, errors.New("openlinker: Native event type must use dotted lowercase segments")
	}
	switch event.EventType {
	case "run.completed", "run.failed", "run.canceled", "run.stream.gap":
		return AgentEvent{}, errors.New("openlinker: Native event type is reserved by Core")
	}
	payload, err := nativeEventPayload(event.Payload)
	if err != nil {
		return AgentEvent{}, err
	}
	event.Payload = payload
	return event, nil
}

func nativeEventPayload(value any) (map[string]any, error) {
	if value == nil {
		return map[string]any{}, nil
	}
	raw, err := json.Marshal(value)
	if err != nil {
		return nil, fmt.Errorf("openlinker: encode Native event payload: %w", err)
	}
	var object map[string]any
	if err = json.Unmarshal(raw, &object); err != nil || object == nil {
		return nil, errors.New("openlinker: Native event payload must encode to a JSON object")
	}
	return object, nil
}

func runtimeInputMap(value any) map[string]any {
	switch typed := value.(type) {
	case RuntimeJSONMap:
		return map[string]any(typed)
	case map[string]any:
		return typed
	default:
		return map[string]any{"value": value}
	}
}

func mergeRuntimeWorkerConfig(environment, explicit RuntimeWorkerConfig) RuntimeWorkerConfig {
	merged := environment
	if explicit.PlatformURL != "" {
		merged.PlatformURL = explicit.PlatformURL
	}
	if explicit.RuntimeURL != "" {
		merged.RuntimeURL = explicit.RuntimeURL
	}
	if explicit.Transport != "" {
		merged.Transport = explicit.Transport
	}
	if explicit.NodeID != "" {
		merged.NodeID = explicit.NodeID
	}
	if explicit.NodeVersion != "" {
		merged.NodeVersion = explicit.NodeVersion
	}
	if explicit.AgentID != "" {
		merged.AgentID = explicit.AgentID
	}
	if explicit.AgentToken != "" {
		merged.AgentToken = explicit.AgentToken
	}
	if explicit.DataDir != "" {
		merged.DataDir = explicit.DataDir
	}
	if explicit.Store != nil {
		merged.Store = explicit.Store
	}
	if explicit.Capacity != 0 {
		merged.Capacity = explicit.Capacity
	}
	if explicit.MTLS.CertFile != "" {
		merged.MTLS.CertFile = explicit.MTLS.CertFile
	}
	if explicit.MTLS.KeyFile != "" {
		merged.MTLS.KeyFile = explicit.MTLS.KeyFile
	}
	if explicit.MTLS.CAFile != "" {
		merged.MTLS.CAFile = explicit.MTLS.CAFile
	}
	if explicit.MTLS.ServerName != "" {
		merged.MTLS.ServerName = explicit.MTLS.ServerName
	}
	if explicit.Logger != nil {
		merged.Logger = explicit.Logger
	}
	return merged
}

const (
	RuntimeConnectorWebSocket = "runtime_ws"
	RuntimeConnectorPull      = "runtime_pull"
	RuntimeTransportHTTP      = "pull"
)
