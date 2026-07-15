package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

type runtimeChaosPoint string
type runtimeChaosStage string

const (
	chaosCreateSession runtimeChaosPoint = "session.create"
	chaosHeartbeat     runtimeChaosPoint = "session.heartbeat"
	chaosClose         runtimeChaosPoint = "session.close"
	chaosClaim         runtimeChaosPoint = "run.claim"
	chaosAssignmentACK runtimeChaosPoint = "assignment.ack"
	chaosReject        runtimeChaosPoint = "assignment.reject"
	chaosLease         runtimeChaosPoint = "lease.renew"
	chaosEvent         runtimeChaosPoint = "event.append"
	chaosResult        runtimeChaosPoint = "result.finalize"
	chaosResume        runtimeChaosPoint = "runtime.resume"
	chaosCommands      runtimeChaosPoint = "commands.poll"
	chaosCancelACK     runtimeChaosPoint = "cancel.ack"
	chaosCallAgent     runtimeChaosPoint = "call-agent"

	chaosBefore runtimeChaosStage = "before"
	chaosAfter  runtimeChaosStage = "after"
)

type runtimeChaosFault struct {
	point runtimeChaosPoint
	stage runtimeChaosStage
	err   error
}

// chaosRuntimeClient is the common protocol fault injector used by worker
// regression tests. An after fault simulates a response/ACK being lost after
// Core already applied the operation.
type chaosRuntimeClient struct {
	RuntimeClient
	mu     sync.Mutex
	faults []runtimeChaosFault
}

func newChaosRuntimeClient(base RuntimeClient, faults ...runtimeChaosFault) *chaosRuntimeClient {
	return &chaosRuntimeClient{RuntimeClient: base, faults: append([]runtimeChaosFault(nil), faults...)}
}

func (client *chaosRuntimeClient) add(point runtimeChaosPoint, stage runtimeChaosStage, err error) {
	client.mu.Lock()
	client.faults = append(client.faults, runtimeChaosFault{point: point, stage: stage, err: err})
	client.mu.Unlock()
}

func (client *chaosRuntimeClient) inject(point runtimeChaosPoint, stage runtimeChaosStage) error {
	client.mu.Lock()
	defer client.mu.Unlock()
	for index, fault := range client.faults {
		if fault.point == point && fault.stage == stage {
			client.faults = append(client.faults[:index], client.faults[index+1:]...)
			return fault.err
		}
	}
	return nil
}

func chaosCall[T any](client *chaosRuntimeClient, point runtimeChaosPoint, call func() (T, error)) (T, error) {
	var zero T
	if err := client.inject(point, chaosBefore); err != nil {
		return zero, err
	}
	value, err := call()
	if err != nil {
		return value, err
	}
	if err = client.inject(point, chaosAfter); err != nil {
		return zero, err
	}
	return value, nil
}

func (client *chaosRuntimeClient) CreateRuntimeSession(ctx context.Context, request RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
	return chaosCall(client, chaosCreateSession, func() (*RuntimeReadyPayload, error) {
		return client.RuntimeClient.CreateRuntimeSession(ctx, request)
	})
}
func (client *chaosRuntimeClient) HeartbeatRuntimeSession(ctx context.Context, request RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
	return chaosCall(client, chaosHeartbeat, func() (*RuntimeReadyPayload, error) {
		return client.RuntimeClient.HeartbeatRuntimeSession(ctx, request)
	})
}
func (client *chaosRuntimeClient) CloseRuntimeSession(ctx context.Context, request RuntimeSessionCloseRequest) error {
	if err := client.inject(chaosClose, chaosBefore); err != nil {
		return err
	}
	if err := client.RuntimeClient.CloseRuntimeSession(ctx, request); err != nil {
		return err
	}
	return client.inject(chaosClose, chaosAfter)
}
func (client *chaosRuntimeClient) ClaimRuntimeRun(ctx context.Context, wait int, request RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
	return chaosCall(client, chaosClaim, func() (*RuntimeRunAssignedPayload, error) {
		return client.RuntimeClient.ClaimRuntimeRun(ctx, wait, request)
	})
}
func (client *chaosRuntimeClient) AckRuntimeAssignment(ctx context.Context, request RuntimeAssignmentAckPayload) (*RuntimeAssignmentConfirmedPayload, error) {
	return chaosCall(client, chaosAssignmentACK, func() (*RuntimeAssignmentConfirmedPayload, error) {
		return client.RuntimeClient.AckRuntimeAssignment(ctx, request)
	})
}
func (client *chaosRuntimeClient) RejectRuntimeAssignment(ctx context.Context, request RuntimeAssignmentRejectPayload) (*RuntimeAssignmentRejectedPayload, error) {
	return chaosCall(client, chaosReject, func() (*RuntimeAssignmentRejectedPayload, error) {
		return client.RuntimeClient.RejectRuntimeAssignment(ctx, request)
	})
}
func (client *chaosRuntimeClient) RenewRuntimeLease(ctx context.Context, request RuntimeLeaseRenewPayload) (*RuntimeLeaseRenewedPayload, error) {
	return chaosCall(client, chaosLease, func() (*RuntimeLeaseRenewedPayload, error) {
		return client.RuntimeClient.RenewRuntimeLease(ctx, request)
	})
}
func (client *chaosRuntimeClient) AppendRuntimeEvent(ctx context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
	return chaosCall(client, chaosEvent, func() (*RuntimeRunEventAckPayload, error) {
		return client.RuntimeClient.AppendRuntimeEvent(ctx, request)
	})
}
func (client *chaosRuntimeClient) FinalizeRuntimeResult(ctx context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
	return chaosCall(client, chaosResult, func() (*RuntimeRunResultAckPayload, error) {
		return client.RuntimeClient.FinalizeRuntimeResult(ctx, request)
	})
}
func (client *chaosRuntimeClient) ResumeRuntimeRuns(ctx context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
	return chaosCall(client, chaosResume, func() (*RuntimeResumeResponse, error) {
		return client.RuntimeClient.ResumeRuntimeRuns(ctx, request)
	})
}
func (client *chaosRuntimeClient) PollRuntimeCommands(ctx context.Context, sessionID string, wait int) (*RuntimeCommandsResponse, error) {
	return chaosCall(client, chaosCommands, func() (*RuntimeCommandsResponse, error) {
		return client.RuntimeClient.PollRuntimeCommands(ctx, sessionID, wait)
	})
}
func (client *chaosRuntimeClient) AckRuntimeCancel(ctx context.Context, request RuntimeRunCancelAckPayload) (*RuntimeRunCancellationState, error) {
	return chaosCall(client, chaosCancelACK, func() (*RuntimeRunCancellationState, error) {
		return client.RuntimeClient.AckRuntimeCancel(ctx, request)
	})
}
func (client *chaosRuntimeClient) CallRuntimeAgent(ctx context.Context, authorization RuntimeCallAgentAuthorization, request RuntimeCallAgentRequest) (*RuntimeRunSummary, error) {
	return chaosCall(client, chaosCallAgent, func() (*RuntimeRunSummary, error) {
		return client.RuntimeClient.CallRuntimeAgent(ctx, authorization, request)
	})
}

func TestChaosRuntimeClientSupportsEveryProtocolOperation(t *testing.T) {
	t.Parallel()
	base := newFakeRuntimeClient()
	base.commandsFn = func(context.Context, string, int) (*RuntimeCommandsResponse, error) {
		return &RuntimeCommandsResponse{}, nil
	}
	client := newChaosRuntimeClient(base)
	sentinel := errors.New("chaos before operation")
	tests := []struct {
		point runtimeChaosPoint
		call  func() error
	}{
		{chaosCreateSession, func() error {
			_, err := client.CreateRuntimeSession(context.Background(), RuntimeHelloPayload{})
			return err
		}},
		{chaosHeartbeat, func() error {
			_, err := client.HeartbeatRuntimeSession(context.Background(), RuntimeHelloPayload{})
			return err
		}},
		{chaosClose, func() error { return client.CloseRuntimeSession(context.Background(), RuntimeSessionCloseRequest{}) }},
		{chaosClaim, func() error {
			_, err := client.ClaimRuntimeRun(context.Background(), 0, RuntimeClaimRequest{})
			return err
		}},
		{chaosAssignmentACK, func() error {
			_, err := client.AckRuntimeAssignment(context.Background(), RuntimeAssignmentAckPayload{})
			return err
		}},
		{chaosReject, func() error {
			_, err := client.RejectRuntimeAssignment(context.Background(), RuntimeAssignmentRejectPayload{})
			return err
		}},
		{chaosLease, func() error {
			_, err := client.RenewRuntimeLease(context.Background(), RuntimeLeaseRenewPayload{})
			return err
		}},
		{chaosEvent, func() error {
			_, err := client.AppendRuntimeEvent(context.Background(), RuntimeRunEventPayload{})
			return err
		}},
		{chaosResult, func() error {
			_, err := client.FinalizeRuntimeResult(context.Background(), RuntimeRunResultPayload{})
			return err
		}},
		{chaosResume, func() error {
			_, err := client.ResumeRuntimeRuns(context.Background(), RuntimeResumePayload{})
			return err
		}},
		{chaosCommands, func() error { _, err := client.PollRuntimeCommands(context.Background(), "", 0); return err }},
		{chaosCancelACK, func() error {
			_, err := client.AckRuntimeCancel(context.Background(), RuntimeRunCancelAckPayload{})
			return err
		}},
		{chaosCallAgent, func() error {
			_, err := client.CallRuntimeAgent(context.Background(), RuntimeCallAgentAuthorization{}, RuntimeCallAgentRequest{})
			return err
		}},
	}
	for _, test := range tests {
		t.Run(string(test.point), func(t *testing.T) {
			client.add(test.point, chaosBefore, sentinel)
			if err := test.call(); !errors.Is(err, sentinel) {
				t.Fatalf("error = %v, want chaos sentinel", err)
			}
		})
	}
}

func TestRuntimeChaosACKLossPreservesStableIDsAndExecutesOnce(t *testing.T) {
	dataDir := t.TempDir()
	base := newFakeRuntimeClient()
	var createCalls atomic.Int32
	base.createFn = func(_ context.Context, _ RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		createCalls.Add(1)
		return base.readyPayload(), nil
	}
	var claimed atomic.Bool
	base.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		if claimed.CompareAndSwap(false, true) {
			return assignedRunForHello(base.helloSnapshot()), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	var ackMu sync.Mutex
	var assignmentACKs []RuntimeAssignmentAckPayload
	base.ackFn = func(_ context.Context, request RuntimeAssignmentAckPayload) (*RuntimeAssignmentConfirmedPayload, error) {
		ackMu.Lock()
		assignmentACKs = append(assignmentACKs, request)
		ackMu.Unlock()
		return &RuntimeAssignmentConfirmedPayload{
			AttemptIdentity: request.AttemptIdentity, AttemptNo: 1, LeaseExpiresAt: time.Now().Add(time.Minute).UTC(),
		}, nil
	}
	var eventMu sync.Mutex
	var events []RuntimeRunEventPayload
	base.eventFn = func(_ context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
		eventMu.Lock()
		events = append(events, request)
		count := len(events)
		eventMu.Unlock()
		return &RuntimeRunEventAckPayload{
			ClientEventID: request.ClientEventID, ClientEventSeq: request.ClientEventSeq,
			Sequence: request.ClientEventSeq, Replayed: count > 1,
		}, nil
	}
	resultDone := make(chan struct{})
	var resultOnce sync.Once
	var resultMu sync.Mutex
	var results []RuntimeRunResultPayload
	base.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		resultMu.Lock()
		results = append(results, request)
		count := len(results)
		resultMu.Unlock()
		if count > 1 {
			resultOnce.Do(func() { close(resultDone) })
		}
		ack := successfulResultACK(request.ResultID)
		ack.Replayed = count > 1
		return ack, nil
	}
	client := newChaosRuntimeClient(base,
		runtimeChaosFault{point: chaosCreateSession, stage: chaosAfter, err: errors.New("lost ready response")},
		runtimeChaosFault{point: chaosAssignmentACK, stage: chaosAfter, err: errors.New("lost assignment ACK response")},
		runtimeChaosFault{point: chaosEvent, stage: chaosAfter, err: errors.New("lost Event ACK response")},
		runtimeChaosFault{point: chaosResult, stage: chaosAfter, err: errors.New("lost Result ACK response")},
	)
	var handlerCalls atomic.Int32
	node := newRuntimeWorkerForTest(dataDir, client, testRuntimeHandlerFunc(func(_ context.Context, _ any, run RuntimeContext) (any, error) {
		handlerCalls.Add(1)
		if err := run.Emit("run.message.delta", RuntimeJSONMap{"text": "durable"}); err != nil {
			return nil, err
		}
		return RuntimeJSONMap{"ok": true}, nil
	}))
	errCh := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, resultDone, 7*time.Second, "Result replay after lost ACK")
	stopRuntimeWorkerForTest(t, node, errCh)

	if createCalls.Load() < 2 || handlerCalls.Load() != 1 {
		t.Fatalf("create calls=%d handler calls=%d", createCalls.Load(), handlerCalls.Load())
	}
	ackMu.Lock()
	if len(assignmentACKs) < 2 || assignmentACKs[0].AttemptIdentity != assignmentACKs[1].AttemptIdentity {
		t.Fatalf("assignment ACK replay = %#v", assignmentACKs)
	}
	ackMu.Unlock()
	eventMu.Lock()
	if len(events) < 2 || events[0].ClientEventID != events[1].ClientEventID || events[0].ClientEventSeq != events[1].ClientEventSeq {
		t.Fatalf("Event ACK-loss replay = %#v", events)
	}
	eventMu.Unlock()
	resultMu.Lock()
	if len(results) < 2 || results[0].ResultID != results[1].ResultID ||
		results[0].FinalClientEventSeq != 1 || results[1].FinalClientEventSeq != 1 {
		t.Fatalf("Result ACK-loss replay = %#v", results)
	}
	resultMu.Unlock()
}

func TestRuntimeAssignmentACKThenConfirmedSaveFailureResumesWithoutDuplicateExecution(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	injected := errors.New("simulated confirmed journal crash")
	var walSyncs atomic.Int32
	store.setDurableHookForTest(func(point, _ string) error {
		if point == durableAfterWALSync && walSyncs.Add(1) == 3 {
			return injected
		}
		return nil
	})
	firstClient := newFakeRuntimeClient()
	var claimed atomic.Bool
	firstClient.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		if claimed.CompareAndSwap(false, true) {
			return assignedRunForHello(firstClient.helloSnapshot()), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	var firstHandlerCalls atomic.Int32
	firstNode := newRuntimeWorkerForTest(dataDir, firstClient, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		firstHandlerCalls.Add(1)
		return RuntimeJSONMap{"unsafe": true}, nil
	}))
	firstNode.Store = store
	firstErrCh := startRuntimeWorkerForTest(firstNode)
	select {
	case err := <-firstErrCh:
		if err == nil || !strings.Contains(err.Error(), injected.Error()) {
			t.Fatalf("first worker error = %v", err)
		}
	case <-time.After(4 * time.Second):
		t.Fatal("timed out waiting for confirmed journal failure")
	}
	if firstHandlerCalls.Load() != 0 {
		t.Fatalf("handler ran across uncertain confirmation: %d", firstHandlerCalls.Load())
	}

	secondClient := newFakeRuntimeClient()
	secondClient.resumeFn = func(_ context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
		if len(request.Attempts) != 1 {
			return nil, fmt.Errorf("resume attempts = %d", len(request.Attempts))
		}
		expires := time.Now().Add(time.Minute).UTC()
		return &RuntimeResumeResponse{Decisions: []RuntimeResumeAcceptedPayload{{
			AttemptIdentity: request.Attempts[0].AttemptIdentity, Decision: RuntimeResumeContinue,
			LeaseExpiresAt: &expires,
			AllowedActions: []RuntimeResumeAction{RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult},
		}}}, nil
	}
	resultDone := make(chan struct{})
	var resultOnce sync.Once
	secondClient.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		resultOnce.Do(func() { close(resultDone) })
		return successfulResultACK(request.ResultID), nil
	}
	var secondHandlerCalls atomic.Int32
	secondNode := newRuntimeWorkerForTest(dataDir, secondClient, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		secondHandlerCalls.Add(1)
		return RuntimeJSONMap{"resumed": true}, nil
	}))
	secondErrCh := startRuntimeWorkerForTest(secondNode)
	waitForTestSignal(t, resultDone, 4*time.Second, "resumed Result after confirmed journal crash")
	stopRuntimeWorkerForTest(t, secondNode, secondErrCh)
	if secondHandlerCalls.Load() != 1 {
		t.Fatalf("resumed handler calls = %d", secondHandlerCalls.Load())
	}
}

func TestRuntimeCapacityFourResumesFourAttemptsConcurrently(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identities := make([]AttemptIdentity, 0, 4)
	for index := 0; index < 4; index++ {
		suffix := fmt.Sprintf("%d", index+1)
		identity := runtimeTestAttemptIdentity(store.Identity())
		identity.RunID = deterministicRuntimeUUID("chaos-run", suffix)
		identity.AttemptID = deterministicRuntimeUUID("chaos-attempt", suffix)
		identity.LeaseID = deterministicRuntimeUUID("chaos-lease", suffix)
		identity.OfferID = deterministicRuntimeUUID("chaos-offer", suffix)
		identity.AssignmentMessageID = deterministicRuntimeUUID("chaos-assignment", suffix)
		if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
			t.Fatal(err)
		}
		payload := runtimeTestAssignmentPayload(identity)
		payload.Input = json.RawMessage(fmt.Sprintf(`{"index":%d}`, index+1))
		if err := store.StoreAssignmentPayload(payload); err != nil {
			t.Fatal(err)
		}
		if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, AssignmentStateACKSent); err != nil {
			t.Fatal(err)
		}
		identities = append(identities, identity)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	client := newFakeRuntimeClient()
	client.resumeFn = func(_ context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
		if len(request.Attempts) != len(identities) {
			return nil, fmt.Errorf("resume attempts = %d", len(request.Attempts))
		}
		decisions := make([]RuntimeResumeAcceptedPayload, len(request.Attempts))
		for index, attempt := range request.Attempts {
			expires := time.Now().Add(time.Minute).UTC()
			decisions[index] = RuntimeResumeAcceptedPayload{
				AttemptIdentity: attempt.AttemptIdentity, Decision: RuntimeResumeContinue,
				LeaseExpiresAt: &expires,
				AllowedActions: []RuntimeResumeAction{RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult},
			}
		}
		return &RuntimeResumeResponse{Decisions: decisions}, nil
	}
	allResults := make(chan struct{})
	var resultCount atomic.Int32
	client.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		if resultCount.Add(1) == 4 {
			close(allResults)
		}
		return successfulResultACK(request.ResultID), nil
	}
	allStarted := make(chan struct{})
	release := make(chan struct{})
	var active atomic.Int32
	var maxActive atomic.Int32
	var started atomic.Int32
	handler := testRuntimeHandlerFunc(func(_ context.Context, _ any, _ RuntimeContext) (any, error) {
		current := active.Add(1)
		for {
			maximum := maxActive.Load()
			if current <= maximum || maxActive.CompareAndSwap(maximum, current) {
				break
			}
		}
		if started.Add(1) == 4 {
			close(allStarted)
		}
		<-release
		active.Add(-1)
		return RuntimeJSONMap{"ok": true}, nil
	})
	node := newRuntimeWorkerForTest(dataDir, client, handler)
	node.Capacity = 4
	errCh := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, allStarted, 4*time.Second, "four resumed handlers")
	close(release)
	waitForTestSignal(t, allResults, 4*time.Second, "four resumed Results")
	stopRuntimeWorkerForTest(t, node, errCh)
	if started.Load() != 4 || maxActive.Load() != 4 {
		t.Fatalf("started=%d max active=%d", started.Load(), maxActive.Load())
	}
}

func TestRuntimeDrainCommandAdvertisesZeroCapacity(t *testing.T) {
	node := &RuntimeWorker{Capacity: 4}
	node.handleDecodedCommand(RuntimeDecodedPendingCommand{Type: RuntimeDrain, Drain: &RuntimeDrainPayload{}})
	capacity, inflight := node.capacitySnapshot()
	if capacity != 0 || inflight != 0 || !node.isDraining() {
		t.Fatalf("capacity=%d inflight=%d draining=%v", capacity, inflight, node.isDraining())
	}
}

func TestRuntimeCancelDeadlineACKsFailedWithoutWaitingForever(t *testing.T) {
	client := newFakeRuntimeClient()
	var claimed atomic.Bool
	var assigned RuntimeRunAssignedPayload
	client.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		if claimed.CompareAndSwap(false, true) {
			assigned = *assignedRunForHello(client.helloSnapshot())
			return &assigned, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	var handlerOnce sync.Once
	var commandSent atomic.Bool
	client.commandsFn = func(ctx context.Context, _ string, _ int) (*RuntimeCommandsResponse, error) {
		select {
		case <-handlerStarted:
		case <-ctx.Done():
			return nil, ctx.Err()
		}
		if commandSent.CompareAndSwap(false, true) {
			payload, err := json.Marshal(RuntimeRunCancelPayload{
				CancellationID: testCancellationID, AttemptIdentity: assigned.AttemptIdentity,
				ReasonCode: "deadline_test", DeadlineAt: time.Now().Add(80 * time.Millisecond).UTC(),
			})
			if err != nil {
				return nil, err
			}
			return &RuntimeCommandsResponse{Commands: []RuntimePendingCommand{{Type: RuntimeRunCancel, Payload: payload}}}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	failedACK := make(chan struct{})
	var failedOnce sync.Once
	var ackMu sync.Mutex
	var acks []RuntimeRunCancelAckPayload
	client.cancelAckFn = func(_ context.Context, request RuntimeRunCancelAckPayload) (*RuntimeRunCancellationState, error) {
		ackMu.Lock()
		acks = append(acks, request)
		ackMu.Unlock()
		if request.CancelState == RuntimeCancelFailed && request.ErrorCode == "CANCEL_DEADLINE_EXCEEDED" {
			failedOnce.Do(func() { close(failedACK) })
		}
		return &RuntimeRunCancellationState{
			CancellationID: request.CancellationID, CancelState: request.CancelState,
			ErrorCode: request.ErrorCode, UpdatedAt: time.Now().UTC(),
		}, nil
	}
	node := newRuntimeWorkerForTest(t.TempDir(), client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		handlerOnce.Do(func() { close(handlerStarted) })
		<-releaseHandler
		return RuntimeJSONMap{"late": true}, nil
	}))
	errCh := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, failedACK, 3*time.Second, "cancel deadline failed ACK")
	close(releaseHandler)
	stopRuntimeWorkerForTest(t, node, errCh)
	ackMu.Lock()
	defer ackMu.Unlock()
	if len(acks) < 2 || acks[0].CancelState != RuntimeCancelStopping ||
		acks[len(acks)-1].CancelState != RuntimeCancelFailed || acks[len(acks)-1].ErrorCode != "CANCEL_DEADLINE_EXCEEDED" {
		t.Fatalf("cancel ACKs = %#v", acks)
	}
}
