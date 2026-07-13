package openlinker

import (
	"context"
	"sync"
	"time"
)

type testRuntimeHandlerFunc func(context.Context, any, RuntimeContext) (any, error)

func (handler testRuntimeHandlerFunc) Handle(ctx context.Context, assignment RuntimeContext) (RuntimeResult, error) {
	raw, err := handler(ctx, assignment.Input, assignment)
	if err != nil {
		return RuntimeResult{}, err
	}
	if result, ok := raw.(RuntimeResult); ok {
		return result, nil
	}
	return RuntimeResult{Status: "success", Output: raw}, nil
}

const (
	testNodeID         = "11111111-1111-4111-8111-111111111111"
	testAgentID        = "22222222-2222-4222-8222-222222222222"
	testCoreInstanceID = "33333333-3333-4333-8333-333333333333"
	testRunID          = "44444444-4444-4444-8444-444444444444"
	testAttemptID      = "55555555-5555-4555-8555-555555555555"
	testLeaseID        = "66666666-6666-4666-8666-666666666666"
	testTargetAgentID  = "77777777-7777-4777-8777-777777777777"
	testCancellationID = "88888888-8888-4888-8888-888888888888"
	testAttachmentID   = "99999999-9999-4999-8999-999999999998"
)

type fakeRuntimeClient struct {
	mu sync.Mutex

	hello      RuntimeHelloPayload
	heartbeats []RuntimeHelloPayload
	closes     []RuntimeSessionCloseRequest
	readyOnce  sync.Once
	ready      chan struct{}

	createFn    func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error)
	heartbeatFn func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error)
	closeFn     func(context.Context, RuntimeSessionCloseRequest) error
	claimFn     func(context.Context, int, RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error)
	ackFn       func(context.Context, RuntimeAssignmentAckPayload) (*RuntimeAssignmentConfirmedPayload, error)
	rejectFn    func(context.Context, RuntimeAssignmentRejectPayload) (*RuntimeAssignmentRejectedPayload, error)
	renewFn     func(context.Context, RuntimeLeaseRenewPayload) (*RuntimeLeaseRenewedPayload, error)
	eventFn     func(context.Context, RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error)
	resultFn    func(context.Context, RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error)
	resumeFn    func(context.Context, RuntimeResumePayload) (*RuntimeResumeResponse, error)
	commandsFn  func(context.Context, string, int) (*RuntimeCommandsResponse, error)
	cancelAckFn func(context.Context, RuntimeRunCancelAckPayload) (*RuntimeRunCancellationState, error)
	callAgentFn func(context.Context, RuntimeCallAgentAuthorization, RuntimeCallAgentRequest) (*RuntimeRunSummary, error)
}

func newFakeRuntimeClient() *fakeRuntimeClient {
	return &fakeRuntimeClient{ready: make(chan struct{})}
}

func (client *fakeRuntimeClient) readyPayload() *RuntimeReadyPayload {
	return &RuntimeReadyPayload{
		CoreInstanceID:  testCoreInstanceID,
		AttachmentID:    testAttachmentID,
		Features:        RuntimeRequiredFeatures(),
		OfferTTLSeconds: 30,
		LeaseTTLSeconds: 60,
		DatabaseTime:    time.Now().UTC(),
	}
}

func (client *fakeRuntimeClient) CreateRuntimeSession(ctx context.Context, request RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
	client.mu.Lock()
	client.hello = request
	client.mu.Unlock()
	client.readyOnce.Do(func() { close(client.ready) })
	if client.createFn != nil {
		return client.createFn(ctx, request)
	}
	return client.readyPayload(), nil
}

func (client *fakeRuntimeClient) HeartbeatRuntimeSession(ctx context.Context, request RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
	client.mu.Lock()
	client.heartbeats = append(client.heartbeats, request)
	client.mu.Unlock()
	if client.heartbeatFn != nil {
		return client.heartbeatFn(ctx, request)
	}
	return client.readyPayload(), nil
}

func (client *fakeRuntimeClient) CloseRuntimeSession(ctx context.Context, request RuntimeSessionCloseRequest) error {
	client.mu.Lock()
	client.closes = append(client.closes, request)
	client.mu.Unlock()
	if client.closeFn != nil {
		return client.closeFn(ctx, request)
	}
	return nil
}

func (client *fakeRuntimeClient) ClaimRuntimeRun(ctx context.Context, wait int, request RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
	if client.claimFn != nil {
		return client.claimFn(ctx, wait, request)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (client *fakeRuntimeClient) AckRuntimeAssignment(ctx context.Context, request RuntimeAssignmentAckPayload) (*RuntimeAssignmentConfirmedPayload, error) {
	if client.ackFn != nil {
		return client.ackFn(ctx, request)
	}
	return &RuntimeAssignmentConfirmedPayload{
		AttemptIdentity: request.AttemptIdentity,
		AttemptNo:       1,
		LeaseExpiresAt:  time.Now().Add(time.Minute).UTC(),
	}, nil
}

func (client *fakeRuntimeClient) RejectRuntimeAssignment(ctx context.Context, request RuntimeAssignmentRejectPayload) (*RuntimeAssignmentRejectedPayload, error) {
	if client.rejectFn != nil {
		return client.rejectFn(ctx, request)
	}
	return &RuntimeAssignmentRejectedPayload{
		AttemptIdentity: request.AttemptIdentity,
		Outcome:         RuntimeOfferRejected,
		DispatchState:   RuntimeDispatchPending,
	}, nil
}

func (client *fakeRuntimeClient) RenewRuntimeLease(ctx context.Context, request RuntimeLeaseRenewPayload) (*RuntimeLeaseRenewedPayload, error) {
	if client.renewFn != nil {
		return client.renewFn(ctx, request)
	}
	return &RuntimeLeaseRenewedPayload{
		AttemptIdentity: request.AttemptIdentity,
		LeaseExpiresAt:  time.Now().Add(time.Minute).UTC(),
	}, nil
}

func (client *fakeRuntimeClient) AppendRuntimeEvent(ctx context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
	if client.eventFn != nil {
		return client.eventFn(ctx, request)
	}
	return &RuntimeRunEventAckPayload{
		ClientEventID:  request.ClientEventID,
		ClientEventSeq: request.ClientEventSeq,
		Sequence:       request.ClientEventSeq,
	}, nil
}

func (client *fakeRuntimeClient) FinalizeRuntimeResult(ctx context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
	if client.resultFn != nil {
		return client.resultFn(ctx, request)
	}
	return successfulResultACK(request.ResultID), nil
}

func (client *fakeRuntimeClient) ResumeRuntimeRuns(ctx context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
	if client.resumeFn != nil {
		return client.resumeFn(ctx, request)
	}
	return &RuntimeResumeResponse{Decisions: []RuntimeResumeAcceptedPayload{}}, nil
}

func (client *fakeRuntimeClient) PollRuntimeCommands(ctx context.Context, runtimeSessionID string, wait int) (*RuntimeCommandsResponse, error) {
	if client.commandsFn != nil {
		return client.commandsFn(ctx, runtimeSessionID, wait)
	}
	<-ctx.Done()
	return nil, ctx.Err()
}

func (client *fakeRuntimeClient) AckRuntimeCancel(ctx context.Context, request RuntimeRunCancelAckPayload) (*RuntimeRunCancellationState, error) {
	if client.cancelAckFn != nil {
		return client.cancelAckFn(ctx, request)
	}
	return &RuntimeRunCancellationState{
		CancellationID: request.CancellationID,
		CancelState:    request.CancelState,
		UpdatedAt:      time.Now().UTC(),
		ErrorCode:      request.ErrorCode,
	}, nil
}

func (client *fakeRuntimeClient) CallRuntimeAgent(ctx context.Context, authorization RuntimeCallAgentAuthorization, request RuntimeCallAgentRequest) (*RuntimeRunSummary, error) {
	if client.callAgentFn != nil {
		return client.callAgentFn(ctx, authorization, request)
	}
	return &RuntimeRunSummary{
		RunID:         "99999999-9999-4999-8999-999999999999",
		Status:        RuntimeRunRunning,
		DispatchState: RuntimeDispatchPending,
	}, nil
}

func (client *fakeRuntimeClient) helloSnapshot() RuntimeHelloPayload {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.hello
}

func successfulResultACK(resultID string) *RuntimeRunResultAckPayload {
	return &RuntimeRunResultAckPayload{
		ResultID:       resultID,
		Classification: RuntimeResultSuccess,
		RunStatus:      RuntimeRunSuccess,
		DispatchState:  RuntimeDispatchTerminal,
	}
}

func assignedRunForHello(hello RuntimeHelloPayload) *RuntimeRunAssignedPayload {
	return &RuntimeRunAssignedPayload{
		AttemptIdentity: RuntimeAttemptIdentity{
			RunID:            testRunID,
			AttemptID:        testAttemptID,
			LeaseID:          testLeaseID,
			FencingToken:     1,
			NodeID:           hello.NodeID,
			AgentID:          hello.AgentID,
			WorkerID:         hello.WorkerID,
			RuntimeSessionID: hello.RuntimeSessionID,
		},
		OfferNo:              1,
		OfferExpiresAt:       time.Now().Add(time.Minute).UTC(),
		AttemptDeadlineAt:    time.Now().Add(2 * time.Minute).UTC(),
		RunDeadlineAt:        time.Now().Add(3 * time.Minute).UTC(),
		Input:                map[string]any{"task": "test reliable runtime"},
		Metadata:             map[string]any{"source": "test"},
		NodeEnvelope:         "ol_ctx_v2.header.payload.signature",
		AgentInvocationToken: "ol_inv_v2.header.payload.signature",
	}
}

func newRuntimeWorkerForTest(dataDir string, client RuntimeClient, adapter RuntimeHandler) *RuntimeWorker {
	return &RuntimeWorker{
		RuntimeURL:        "https://core.example.test",
		NodeID:            testNodeID,
		AgentID:           testAgentID,
		DataDir:           dataDir,
		Capacity:          1,
		ClaimWait:         time.Second,
		CommandWait:       time.Second,
		HeartbeatInterval: time.Hour,
		RetryMinimum:      5 * time.Millisecond,
		RetryMaximum:      20 * time.Millisecond,
		Handler:           adapter,
		runtimeClient:     client,
		jitter:            func(value time.Duration) time.Duration { return value },
	}
}
