package openlinker

import (
	"context"
	"encoding/json"
	"log"
	"time"
)

// RuntimeWorkerConfig configures a reliable Runtime Worker. RuntimeURL is an
// explicit override; otherwise PlatformURL is used only for credential-free
// discovery of the dedicated Runtime origin.
type RuntimeWorkerConfig struct {
	PlatformURL string
	RuntimeURL  string
	Transport   RuntimeTransportMode
	NodeID      string
	NodeVersion string
	AgentID     string
	AgentToken  string
	MTLS        RuntimeMTLSConfig
	Store       RuntimeStore
	DataDir     string
	Handler     RuntimeHandler
	Capacity    int64

	ClaimWait         time.Duration
	CommandWait       time.Duration
	HeartbeatInterval time.Duration
	RetryMinimum      time.Duration
	RetryMaximum      time.Duration
	Logger            *log.Logger
	OnReady           func(RuntimeReadyPayload)
}

// NewRuntimeWorker validates config and returns a worker ready to Start.
func NewRuntimeWorker(config RuntimeWorkerConfig) (*RuntimeWorker, error) {
	return newRuntimeWorker(config, nil, nil)
}

func newRuntimeWorker(config RuntimeWorkerConfig, client RuntimeClient, dialer RuntimeTransportDialer) (*RuntimeWorker, error) {
	worker := &RuntimeWorker{
		PlatformURL:       config.PlatformURL,
		RuntimeURL:        config.RuntimeURL,
		Transport:         config.Transport,
		NodeID:            config.NodeID,
		NodeVersion:       config.NodeVersion,
		AgentID:           config.AgentID,
		AgentToken:        config.AgentToken,
		MTLS:              config.MTLS,
		Store:             config.Store,
		DataDir:           config.DataDir,
		Handler:           config.Handler,
		Capacity:          config.Capacity,
		ClaimWait:         config.ClaimWait,
		CommandWait:       config.CommandWait,
		HeartbeatInterval: config.HeartbeatInterval,
		RetryMinimum:      config.RetryMinimum,
		RetryMaximum:      config.RetryMaximum,
		Logger:            config.Logger,
		OnReady:           config.OnReady,
		runtimeClient:     client,
		runtimeDialer:     dialer,
	}
	if runtimeClient, ok := client.(*Runtime); ok && runtimeClient != nil && runtimeClient.client != nil {
		worker.httpClient = runtimeClient.client.httpClient
		if worker.AgentToken == "" {
			worker.AgentToken = runtimeClient.client.agentToken
		}
	}
	if err := worker.applyDefaultsAndValidate(); err != nil {
		return nil, err
	}
	return worker, nil
}

// RuntimeJSONMap is the JSON object shape used by Runtime assignments,
// events, results, and delegated Agent calls.
type RuntimeJSONMap map[string]any

// RuntimeEvent is an application event emitted before the final result.
type RuntimeEvent struct {
	EventType string `json:"event_type"`
	Payload   any    `json:"payload,omitempty"`
}

// RuntimeHandlerError is a stable, bounded application failure returned to
// Core as part of a Runtime result.
type RuntimeHandlerError struct {
	Code      string `json:"code"`
	Message   string `json:"message"`
	Retryable bool   `json:"retryable,omitempty"`
}

// RuntimeResult is the terminal result returned by a RuntimeHandler.
type RuntimeResult struct {
	Status     string               `json:"status"`
	Output     any                  `json:"output,omitempty"`
	Events     []RuntimeEvent       `json:"events,omitempty"`
	Error      *RuntimeHandlerError `json:"error,omitempty"`
	DurationMS int64                `json:"duration_ms,omitempty"`
}

// RuntimeCallOptions controls an assignment-scoped delegated Agent call.
type RuntimeCallOptions struct {
	IdempotencyKey string
	Reason         string
	Metadata       any
}

// RuntimeContext is the durable, confirmed assignment passed to a handler.
// Emit and CallAgent remain scoped to the active Attempt and stop accepting
// work after cancellation or terminal completion.
type RuntimeContext struct {
	RunID             string
	AgentID           string
	AttemptIdentity   RuntimeAttemptIdentity
	AttemptDeadlineAt time.Time
	RunDeadlineAt     time.Time
	Input             any
	Metadata          RuntimeJSONMap

	emit      func(eventType string, payload any) error
	callAgent func(context.Context, string, any, RuntimeCallOptions) (any, error)
}

// Deadline returns the earlier of the Attempt and Run deadlines.
func (runtime RuntimeContext) Deadline() (time.Time, bool) {
	deadline := runtime.AttemptDeadlineAt
	if deadline.IsZero() || (!runtime.RunDeadlineAt.IsZero() && runtime.RunDeadlineAt.Before(deadline)) {
		deadline = runtime.RunDeadlineAt
	}
	return deadline, !deadline.IsZero()
}

// Emit durably journals an event before scheduling it for upload.
func (runtime RuntimeContext) Emit(eventType string, payload any) error {
	if runtime.emit == nil {
		return context.Canceled
	}
	return runtime.emit(eventType, payload)
}

// CallAgent makes an assignment-scoped delegated Agent call. An explicit
// idempotency key is required so reconnects cannot duplicate the child Run.
func (runtime RuntimeContext) CallAgent(
	ctx context.Context,
	targetAgentID string,
	input any,
	options RuntimeCallOptions,
) (any, error) {
	if runtime.callAgent == nil {
		return nil, context.Canceled
	}
	return runtime.callAgent(ctx, targetAgentID, input, options)
}

// RuntimeHandler executes only confirmed assignments. Cancellation and
// deadlines are delivered through ctx.
type RuntimeHandler interface {
	Handle(ctx context.Context, assignment RuntimeContext) (RuntimeResult, error)
}

// RuntimeHandlerFunc adapts a function to RuntimeHandler.
type RuntimeHandlerFunc func(context.Context, RuntimeContext) (RuntimeResult, error)

func (handler RuntimeHandlerFunc) Handle(ctx context.Context, assignment RuntimeContext) (RuntimeResult, error) {
	return handler(ctx, assignment)
}

// RuntimeStore owns the stable worker identity, assignment journal, encrypted
// event/result spool, and session epoch used by RuntimeWorker. Implementations
// must make every mutating method durable before returning success.
type RuntimeStore interface {
	Identity() RuntimeIdentity
	AcceptsNewRuns() bool
	CreateAssignment(AssignmentJournalRecord) error
	AdvanceAssignment(string, AssignmentState) (AssignmentJournalRecord, error)
	Assignment(string) (AssignmentJournalRecord, error)
	Assignments() ([]AssignmentJournalRecord, error)
	DeleteAssignment(string) error
	StoreAssignmentPayload(DurableAssignmentPayload) error
	AssignmentPayload(string) (DurableAssignmentPayload, error)
	AppendEvent(AttemptIdentity, string, json.RawMessage) (EventSpoolRecord, error)
	PendingEvents(string) ([]EventSpoolRecord, error)
	EventsInRanges(string, []RuntimeEventRange) ([]EventSpoolRecord, error)
	AckEvent(string, string, int64) error
	StoreResult(ResultSpoolRecord) error
	PendingResult(string) (ResultSpoolRecord, error)
	AckResult(string, string) error
	ClearTerminalEvents(string) error
	Close() error
}

var _ RuntimeStore = (*FileRuntimeStore)(nil)
