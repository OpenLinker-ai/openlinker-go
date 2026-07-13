package openlinker

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeReliableFlowReplaysStableEventAndResult(t *testing.T) {
	dataDir := t.TempDir()
	client := newFakeRuntimeClient()
	var claimCalls atomic.Int32
	client.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		if claimCalls.Add(1) == 1 {
			return assignedRunForHello(client.helloSnapshot()), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	var eventMu sync.Mutex
	var eventRequests []RuntimeRunEventPayload
	client.eventFn = func(_ context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
		eventMu.Lock()
		eventRequests = append(eventRequests, request)
		attempt := len(eventRequests)
		eventMu.Unlock()
		if attempt == 1 {
			return nil, errors.New("simulated Event ACK loss")
		}
		return &RuntimeRunEventAckPayload{
			ClientEventID:  request.ClientEventID,
			ClientEventSeq: request.ClientEventSeq,
			Sequence:       71,
			Replayed:       true,
		}, nil
	}

	resultDone := make(chan struct{})
	var resultOnce sync.Once
	var resultMu sync.Mutex
	var resultRequests []RuntimeRunResultPayload
	client.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		resultMu.Lock()
		resultRequests = append(resultRequests, request)
		attempt := len(resultRequests)
		resultMu.Unlock()
		if attempt == 1 {
			return nil, errors.New("simulated Result ACK loss")
		}
		resultOnce.Do(func() { close(resultDone) })
		ack := successfulResultACK(request.ResultID)
		ack.Replayed = true
		return ack, nil
	}

	var delegatedMu sync.Mutex
	var delegatedAuth []RuntimeCallAgentAuthorization
	client.callAgentFn = func(_ context.Context, authorization RuntimeCallAgentAuthorization, request RuntimeCallAgentRequest) (*RuntimeRunSummary, error) {
		delegatedMu.Lock()
		delegatedAuth = append(delegatedAuth, authorization)
		delegatedMu.Unlock()
		if request.TargetAgentID != testTargetAgentID || request.Input["question"] != "status" {
			return nil, errors.New("delegated request mismatch")
		}
		return &RuntimeRunSummary{
			RunID:         "99999999-9999-4999-8999-999999999999",
			Status:        RuntimeRunRunning,
			DispatchState: RuntimeDispatchPending,
		}, nil
	}

	var adapterCalls atomic.Int32
	savedContext := make(chan RuntimeContext, 1)
	adapter := testRuntimeHandlerFunc(func(ctx context.Context, _ any, runCtx RuntimeContext) (any, error) {
		adapterCalls.Add(1)
		savedContext <- runCtx
		if runCtx.Metadata["source"] != "test" {
			return nil, fmt.Errorf("assignment metadata = %#v", runCtx.Metadata)
		}
		if _, err := runCtx.CallAgent(ctx, testTargetAgentID, RuntimeJSONMap{"question": "status"}, RuntimeCallOptions{
			IdempotencyKey: "child-intent-status-1",
			Reason:         "collect status",
		}); err != nil {
			return nil, err
		}
		runCtx.Emit("run.message.delta", RuntimeJSONMap{"text": "working"})
		return RuntimeJSONMap{"answer": 42}, nil
	})
	node := newRuntimeWorkerForTest(dataDir, client, adapter)
	errCh := startRuntimeWorkerForTest(node)

	waitForTestSignal(t, resultDone, 7*time.Second, "typed Result ACK")
	retainedContext := <-savedContext
	if _, err := retainedContext.CallAgent(context.Background(), testTargetAgentID, RuntimeJSONMap{"question": "too late"}, RuntimeCallOptions{
		IdempotencyKey: "after-handler-return",
	}); !errors.Is(err, context.Canceled) {
		t.Fatalf("retained RuntimeContext CallAgent error = %v, want context.Canceled", err)
	}
	stopRuntimeWorkerForTest(t, node, errCh)

	if adapterCalls.Load() != 1 {
		t.Fatalf("adapter calls = %d, want exactly 1", adapterCalls.Load())
	}
	eventMu.Lock()
	if len(eventRequests) != 2 || eventRequests[0].ClientEventID != eventRequests[1].ClientEventID || eventRequests[0].ClientEventSeq != eventRequests[1].ClientEventSeq {
		t.Fatalf("Event replay did not preserve identity: %#v", eventRequests)
	}
	stableEventID := eventRequests[0].ClientEventID
	eventMu.Unlock()
	if stableEventID == "" {
		t.Fatal("Event replay used an empty client_event_id")
	}
	resultMu.Lock()
	if len(resultRequests) != 2 || resultRequests[0].ResultID != resultRequests[1].ResultID || resultRequests[0].FinalClientEventSeq != 1 || resultRequests[1].FinalClientEventSeq != 1 {
		t.Fatalf("Result replay did not preserve identity/final sequence: %#v", resultRequests)
	}
	resultMu.Unlock()
	delegatedMu.Lock()
	if len(delegatedAuth) != 1 || delegatedAuth[0].NodeEnvelope != "ol_ctx_v2.header.payload.signature" ||
		delegatedAuth[0].AgentInvocationToken != "ol_inv_v2.header.payload.signature" || delegatedAuth[0].IdempotencyKey != "child-intent-status-1" {
		t.Fatalf("delegated authorization = %#v", delegatedAuth)
	}
	delegatedMu.Unlock()

	client.mu.Lock()
	if len(client.closes) != 1 || client.closes[0].RuntimeSessionID != client.hello.RuntimeSessionID {
		t.Fatalf("session closes = %#v hello=%#v", client.closes, client.hello)
	}
	foundDrainingHeartbeat := false
	for _, heartbeat := range client.heartbeats {
		foundDrainingHeartbeat = foundDrainingHeartbeat || heartbeat.Capacity == 0
	}
	client.mu.Unlock()
	if !foundDrainingHeartbeat {
		t.Fatal("graceful shutdown did not advertise capacity=0")
	}

	store := openRuntimeStoreForTest(t, dataDir)
	records, err := store.Assignments()
	if err != nil {
		t.Fatal(err)
	}
	if len(records) != 0 {
		t.Fatalf("fully acknowledged Attempt remained durable: %#v", records)
	}
}

func TestRuntimeFinishedAttemptRenewsLeaseUntilSpoolIsAcknowledged(t *testing.T) {
	dataDir := t.TempDir()
	client := newFakeRuntimeClient()
	client.createFn = func(_ context.Context, _ RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		ready := client.readyPayload()
		ready.LeaseTTLSeconds = 1
		return ready, nil
	}
	var claimed atomic.Bool
	client.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		if claimed.CompareAndSwap(false, true) {
			return assignedRunForHello(client.helloSnapshot()), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	const renewsBeforeRecovery = int32(3)
	var renewCalls atomic.Int32
	var invalidInflight atomic.Bool
	client.renewFn = func(_ context.Context, request RuntimeLeaseRenewPayload) (*RuntimeLeaseRenewedPayload, error) {
		if request.Inflight != 1 || request.Capacity != 1 {
			invalidInflight.Store(true)
		}
		renewCalls.Add(1)
		return &RuntimeLeaseRenewedPayload{
			AttemptIdentity: request.AttemptIdentity,
			LeaseExpiresAt:  time.Now().Add(2 * time.Second).UTC(),
		}, nil
	}

	firstUploadFailure := make(chan struct{})
	var firstUploadFailureOnce sync.Once
	client.eventFn = func(_ context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
		if renewCalls.Load() < renewsBeforeRecovery {
			firstUploadFailureOnce.Do(func() { close(firstUploadFailure) })
			return nil, errors.New("simulated spool upload outage")
		}
		return &RuntimeRunEventAckPayload{
			ClientEventID: request.ClientEventID, ClientEventSeq: request.ClientEventSeq,
			Sequence: request.ClientEventSeq,
		}, nil
	}
	resultACKed := make(chan struct{})
	var resultACKOnce sync.Once
	client.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		if renewCalls.Load() < renewsBeforeRecovery {
			return nil, errors.New("Result reached Core before the controlled outage recovered")
		}
		resultACKOnce.Do(func() { close(resultACKed) })
		return successfulResultACK(request.ResultID), nil
	}

	handlerReturned := make(chan struct{})
	var handlerCalls atomic.Int32
	node := newRuntimeWorkerForTest(dataDir, client, testRuntimeHandlerFunc(func(_ context.Context, _ any, runCtx RuntimeContext) (any, error) {
		handlerCalls.Add(1)
		if err := runCtx.Emit("run.progress", RuntimeJSONMap{"phase": "handler_finished"}); err != nil {
			return nil, err
		}
		close(handlerReturned)
		return RuntimeJSONMap{"ok": true}, nil
	}))
	node.Logger = log.New(io.Discard, "", 0)
	errCh := startRuntimeWorkerForTest(node)
	var stopOnce sync.Once
	stop := func() {
		stopOnce.Do(func() { stopRuntimeWorkerForTest(t, node, errCh) })
	}
	t.Cleanup(stop)

	waitForTestSignal(t, handlerReturned, 2*time.Second, "handler completion")
	waitForTestSignal(t, firstUploadFailure, 2*time.Second, "controlled spool upload failure")
	eventuallyForTest(t, 5*time.Second, func() bool {
		return renewCalls.Load() >= renewsBeforeRecovery
	}, "lease renewal across multiple spool retry intervals")
	waitForTestSignal(t, resultACKed, 2*time.Second, "Result ACK after spool recovery")
	if handlerCalls.Load() != 1 {
		t.Fatalf("handler calls = %d, want exactly 1", handlerCalls.Load())
	}
	if invalidInflight.Load() {
		t.Fatal("finished Attempt was removed from in-flight capacity before Result ACK")
	}
	eventuallyForTest(t, time.Second, func() bool {
		node.stateMu.RLock()
		defer node.stateMu.RUnlock()
		return len(node.active) == 0
	}, "Result ACK to retire the active Attempt")
	renewsAfterACK := renewCalls.Load()
	time.Sleep(450 * time.Millisecond)
	if got := renewCalls.Load(); got != renewsAfterACK {
		t.Fatalf("lease renewed after Result ACK: before=%d after=%d", renewsAfterACK, got)
	}
	stop()
}

func TestRuntimeResumeAfterAssignmentACKResponseLossExecutesOnce(t *testing.T) {
	dataDir := t.TempDir()
	firstClient := newFakeRuntimeClient()
	var firstClaim atomic.Bool
	firstClient.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		if firstClaim.CompareAndSwap(false, true) {
			return assignedRunForHello(firstClient.helloSnapshot()), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	ackEntered := make(chan struct{})
	var ackOnce sync.Once
	firstClient.ackFn = func(ctx context.Context, _ RuntimeAssignmentAckPayload) (*RuntimeAssignmentConfirmedPayload, error) {
		ackOnce.Do(func() { close(ackEntered) })
		<-ctx.Done()
		return nil, ctx.Err()
	}
	var firstAdapterCalls atomic.Int32
	firstNode := newRuntimeWorkerForTest(dataDir, firstClient, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		firstAdapterCalls.Add(1)
		return RuntimeJSONMap{"unsafe": true}, nil
	}))
	firstErrCh := startRuntimeWorkerForTest(firstNode)
	waitForTestSignal(t, ackEntered, 3*time.Second, "assignment ACK request")
	stopRuntimeWorkerForTest(t, firstNode, firstErrCh)
	if firstAdapterCalls.Load() != 0 {
		t.Fatalf("adapter ran before assignment confirmation: %d", firstAdapterCalls.Load())
	}
	firstHello := firstClient.helloSnapshot()

	secondClient := newFakeRuntimeClient()
	var resumeRequest RuntimeResumePayload
	secondClient.resumeFn = func(_ context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
		resumeRequest = request
		decisions := make([]RuntimeResumeAcceptedPayload, len(request.Attempts))
		for index, attempt := range request.Attempts {
			leaseExpiry := time.Now().Add(time.Minute).UTC()
			decisions[index] = RuntimeResumeAcceptedPayload{
				AttemptIdentity: attempt.AttemptIdentity,
				Decision:        RuntimeResumeContinue,
				LeaseExpiresAt:  &leaseExpiry,
				AllowedActions: []RuntimeResumeAction{
					RuntimeActionContinueExecution,
					RuntimeActionUploadEvents,
					RuntimeActionUploadResult,
				},
			}
		}
		return &RuntimeResumeResponse{Decisions: decisions}, nil
	}
	resultDone := make(chan struct{})
	var resultOnce sync.Once
	secondClient.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		resultOnce.Do(func() { close(resultDone) })
		return successfulResultACK(request.ResultID), nil
	}
	var secondAdapterCalls atomic.Int32
	secondNode := newRuntimeWorkerForTest(dataDir, secondClient, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		secondAdapterCalls.Add(1)
		return RuntimeJSONMap{"resumed": true}, nil
	}))
	secondErrCh := startRuntimeWorkerForTest(secondNode)
	waitForTestSignal(t, resultDone, 4*time.Second, "resumed Result")
	stopRuntimeWorkerForTest(t, secondNode, secondErrCh)

	if secondAdapterCalls.Load() != 1 {
		t.Fatalf("resumed adapter calls = %d, want 1", secondAdapterCalls.Load())
	}
	secondHello := secondClient.helloSnapshot()
	if len(resumeRequest.Attempts) != 1 || resumeRequest.Attempts[0].AttemptIdentity.RuntimeSessionID != firstHello.RuntimeSessionID {
		t.Fatalf("resume did not carry the durable Attempt identity: %#v", resumeRequest)
	}
	if resumeRequest.RuntimeSessionID != secondHello.RuntimeSessionID || resumeRequest.RuntimeSessionID == firstHello.RuntimeSessionID {
		t.Fatalf("resume session rotation mismatch: first=%s second=%s request=%s", firstHello.RuntimeSessionID, secondHello.RuntimeSessionID, resumeRequest.RuntimeSessionID)
	}
}

func TestRuntimeReplacementSessionUploadsDurableSpoolWithoutRerun(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := runtimeTestAttemptIdentity(store.Identity())
	if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAssignmentPayload(runtimeTestAssignmentPayload(identity)); err != nil {
		t.Fatal(err)
	}
	for _, state := range []AssignmentState{AssignmentStateACKSent, AssignmentStateConfirmed, AssignmentStateStarted} {
		if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, state); err != nil {
			t.Fatal(err)
		}
	}
	event, err := store.AppendEvent(identity, "run.message.delta", json.RawMessage(`{"text":"durable"}`))
	if err != nil {
		t.Fatal(err)
	}
	resultID, err := newRuntimeUUID()
	if err != nil {
		t.Fatal(err)
	}
	resultPayload := RuntimeRunResultPayload{
		AttemptIdentity:     sdkAttemptIdentity(identity),
		ResultID:            resultID,
		Status:              "success",
		Output:              map[string]any{"answer": 42},
		DurationMS:          10,
		FinalClientEventSeq: event.ClientEventSeq,
	}
	resultRaw, err := json.Marshal(resultPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreResult(ResultSpoolRecord{
		Identity:            identity,
		ResultID:            resultID,
		FinalClientEventSeq: event.ClientEventSeq,
		Status:              "success",
		Payload:             resultRaw,
	}); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	client := newFakeRuntimeClient()
	client.resumeFn = func(_ context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
		if len(request.Attempts) != 1 || request.Attempts[0].PendingResultID != resultID || len(request.Attempts[0].PendingClientEventRanges) != 1 {
			return nil, errors.New("resume inventory mismatch")
		}
		return &RuntimeResumeResponse{Decisions: []RuntimeResumeAcceptedPayload{{
			AttemptIdentity: request.Attempts[0].AttemptIdentity,
			Decision:        RuntimeResumeUploadSpool,
			AllowedActions: []RuntimeResumeAction{
				RuntimeActionUploadEvents,
				RuntimeActionUploadResult,
			},
		}}}, nil
	}
	client.eventFn = func(_ context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
		if request.ClientEventID != event.ClientEventID || request.ClientEventSeq != event.ClientEventSeq {
			return nil, errors.New("durable Event identity changed")
		}
		return &RuntimeRunEventAckPayload{
			ClientEventID: request.ClientEventID, ClientEventSeq: request.ClientEventSeq, Sequence: 1,
		}, nil
	}
	resultDone := make(chan struct{})
	var resultOnce sync.Once
	client.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		if request.ResultID != resultID || request.FinalClientEventSeq != event.ClientEventSeq {
			return nil, errors.New("durable Result identity changed")
		}
		resultOnce.Do(func() { close(resultDone) })
		return successfulResultACK(request.ResultID), nil
	}
	var adapterCalls atomic.Int32
	node := newRuntimeWorkerForTest(dataDir, client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		adapterCalls.Add(1)
		return nil, errors.New("durable spool must not rerun adapter")
	}))
	errCh := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, resultDone, 3*time.Second, "replacement-session durable Result")
	stopRuntimeWorkerForTest(t, node, errCh)
	if adapterCalls.Load() != 0 {
		t.Fatalf("replacement session reran adapter %d time(s)", adapterCalls.Load())
	}
}

func TestRuntimeRefusesToRerunStartedAttemptAfterProcessRestart(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := runtimeTestAttemptIdentity(store.Identity())
	if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAssignmentPayload(runtimeTestAssignmentPayload(identity)); err != nil {
		t.Fatal(err)
	}
	for _, state := range []AssignmentState{AssignmentStateACKSent, AssignmentStateConfirmed, AssignmentStateStarted} {
		if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, state); err != nil {
			t.Fatal(err)
		}
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	client := newFakeRuntimeClient()
	client.resumeFn = func(_ context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
		leaseExpiry := time.Now().Add(time.Minute).UTC()
		return &RuntimeResumeResponse{Decisions: []RuntimeResumeAcceptedPayload{{
			AttemptIdentity: request.Attempts[0].AttemptIdentity,
			Decision:        RuntimeResumeContinue,
			LeaseExpiresAt:  &leaseExpiry,
			AllowedActions: []RuntimeResumeAction{
				RuntimeActionContinueExecution,
				RuntimeActionUploadEvents,
				RuntimeActionUploadResult,
			},
		}}}, nil
	}
	var adapterCalls atomic.Int32
	node := newRuntimeWorkerForTest(dataDir, client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		adapterCalls.Add(1)
		return nil, nil
	}))
	err := node.Start(context.Background())
	if err == nil || !strings.Contains(err.Error(), "unsafe resume refused") {
		t.Fatalf("Start error = %v, want unsafe resume refusal", err)
	}
	if adapterCalls.Load() != 0 {
		t.Fatalf("started Attempt was rerun %d time(s)", adapterCalls.Load())
	}
}

func TestRuntimeCancelACKsStoppedOnlyAfterAdapterExited(t *testing.T) {
	dataDir := t.TempDir()
	client := newFakeRuntimeClient()
	var claimed atomic.Bool
	client.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		if claimed.CompareAndSwap(false, true) {
			return assignedRunForHello(client.helloSnapshot()), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	adapterStarted := make(chan struct{})
	adapterExited := make(chan struct{})
	adapter := testRuntimeHandlerFunc(func(ctx context.Context, _ any, _ RuntimeContext) (any, error) {
		close(adapterStarted)
		<-ctx.Done()
		time.Sleep(40 * time.Millisecond)
		close(adapterExited)
		return nil, ctx.Err()
	})
	var commandDelivered atomic.Bool
	var polledSession string
	client.commandsFn = func(ctx context.Context, runtimeSessionID string, _ int) (*RuntimeCommandsResponse, error) {
		polledSession = runtimeSessionID
		if commandDelivered.CompareAndSwap(false, true) {
			select {
			case <-adapterStarted:
			case <-ctx.Done():
				return nil, ctx.Err()
			}
			assigned := assignedRunForHello(client.helloSnapshot())
			payload, err := json.Marshal(RuntimeRunCancelPayload{
				CancellationID:  testCancellationID,
				AttemptIdentity: assigned.AttemptIdentity,
				ReasonCode:      "USER_REQUESTED",
				DeadlineAt:      time.Now().Add(3 * time.Second).UTC(),
			})
			if err != nil {
				return nil, err
			}
			return &RuntimeCommandsResponse{
				Commands:     []RuntimePendingCommand{{Type: RuntimeRunCancel, Payload: payload}},
				DatabaseTime: time.Now().UTC(),
			}, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	stoppedACK := make(chan struct{})
	var stoppedOnce sync.Once
	var cancelMu sync.Mutex
	var cancelStates []RuntimeCancelState
	var stoppedBeforeExit bool
	client.cancelAckFn = func(_ context.Context, request RuntimeRunCancelAckPayload) (*RuntimeRunCancellationState, error) {
		cancelMu.Lock()
		cancelStates = append(cancelStates, request.CancelState)
		if request.CancelState == RuntimeCancelStopped {
			select {
			case <-adapterExited:
			default:
				stoppedBeforeExit = true
			}
			stoppedOnce.Do(func() { close(stoppedACK) })
		}
		cancelMu.Unlock()
		return &RuntimeRunCancellationState{
			CancellationID: request.CancellationID,
			CancelState:    request.CancelState,
			UpdatedAt:      time.Now().UTC(),
		}, nil
	}

	node := newRuntimeWorkerForTest(dataDir, client, adapter)
	errCh := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, stoppedACK, 4*time.Second, "cancel stopped ACK")
	eventuallyForTest(t, time.Second, func() bool {
		node.stateMu.RLock()
		defer node.stateMu.RUnlock()
		return len(node.cancellations) == 0
	}, "completed cancellation to leave the in-flight dedupe set")
	eventuallyForTest(t, 2*time.Second, func() bool {
		record, err := node.store.Assignment(deterministicRuntimeUUID("assignment", testAttemptID, testLeaseID))
		return err == nil && record.State == AssignmentStateRevoked
	}, "canceled Attempt journal to become revoked")
	stopRuntimeWorkerForTest(t, node, errCh)

	cancelMu.Lock()
	if stoppedBeforeExit || len(cancelStates) < 2 || cancelStates[0] != RuntimeCancelStopping || cancelStates[len(cancelStates)-1] != RuntimeCancelStopped {
		t.Fatalf("cancel ACK order = %#v stoppedBeforeExit=%v", cancelStates, stoppedBeforeExit)
	}
	cancelMu.Unlock()
	if polledSession != client.helloSnapshot().RuntimeSessionID {
		t.Fatalf("command poll session = %q, want %q", polledSession, client.helloSnapshot().RuntimeSessionID)
	}
}

func TestRuntimeStaleLeaseCancelsExactAttempt(t *testing.T) {
	dataDir := t.TempDir()
	client := newFakeRuntimeClient()
	client.createFn = func(_ context.Context, _ RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		ready := client.readyPayload()
		ready.LeaseTTLSeconds = 1
		return ready, nil
	}
	var claimed atomic.Bool
	client.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		if claimed.CompareAndSwap(false, true) {
			return assignedRunForHello(client.helloSnapshot()), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	renewed := make(chan struct{})
	var renewOnce sync.Once
	client.renewFn = func(_ context.Context, request RuntimeLeaseRenewPayload) (*RuntimeLeaseRenewedPayload, error) {
		renewOnce.Do(func() { close(renewed) })
		if request.AttemptIdentity.AttemptID != testAttemptID {
			return nil, errors.New("wrong Attempt renewed")
		}
		return nil, &Error{StatusCode: 409, Code: "STALE_LEASE", Message: "stale lease"}
	}
	adapterExited := make(chan struct{})
	adapter := testRuntimeHandlerFunc(func(ctx context.Context, _ any, _ RuntimeContext) (any, error) {
		<-ctx.Done()
		close(adapterExited)
		return nil, ctx.Err()
	})
	node := newRuntimeWorkerForTest(dataDir, client, adapter)
	errCh := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, renewed, 2*time.Second, "lease renewal")
	waitForTestSignal(t, adapterExited, 2*time.Second, "stale-lease cancellation")
	eventuallyForTest(t, 2*time.Second, func() bool {
		record, err := node.store.Assignment(deterministicRuntimeUUID("assignment", testAttemptID, testLeaseID))
		return err == nil && record.State == AssignmentStateRevoked
	}, "stale-lease Attempt journal to become revoked")
	stopRuntimeWorkerForTest(t, node, errCh)
}

func TestRuntimeLeaseRevokeCancelsOnlyTargetAttempt(t *testing.T) {
	store := openRuntimeStoreForTest(t, t.TempDir())
	defer store.Close()
	first := persistStartedAssignmentForTest(t, store, "cancel-a")
	second := persistStartedAssignmentForTest(t, store, "cancel-b")
	firstCtx, cancelFirst := context.WithCancel(context.Background())
	secondCtx, cancelSecond := context.WithCancel(context.Background())
	defer cancelSecond()
	node := &RuntimeWorker{
		store: store,
		active: map[string]*activeRuntimeAttempt{
			first.AttemptID:  {identity: first, ctx: firstCtx, cancel: cancelFirst},
			second.AttemptID: {identity: second, ctx: secondCtx, cancel: cancelSecond},
		},
		spoolAllowed: make(map[string]spoolPermission),
		fatal:        make(chan error, 1),
		runtimeCtx:   context.Background(),
	}
	node.handleLeaseRevoke(RuntimeRunLeaseRevokedPayload{
		AttemptIdentity: sdkAttemptIdentity(first), ReasonCode: "LEASE_REVOKED",
		DispatchState: RuntimeDispatchPending, RunStatus: RuntimeRunRunning,
	})
	select {
	case <-firstCtx.Done():
	case <-time.After(time.Second):
		t.Fatal("target Attempt was not canceled")
	}
	select {
	case <-secondCtx.Done():
		t.Fatal("canceling Attempt A canceled Attempt B")
	default:
	}
	record, err := store.Assignment(first.AssignmentMessageID)
	if err != nil || record.State != AssignmentStateRevoked {
		t.Fatalf("target journal = %#v, %v", record, err)
	}
	record, err = store.Assignment(second.AssignmentMessageID)
	if err != nil || record.State != AssignmentStateStarted {
		t.Fatalf("unrelated journal = %#v, %v", record, err)
	}
}

func TestDelegatedAgentCallRequiresExplicitIntentKey(t *testing.T) {
	client := newFakeRuntimeClient()
	var mu sync.Mutex
	var keys []string
	client.callAgentFn = func(_ context.Context, authorization RuntimeCallAgentAuthorization, _ RuntimeCallAgentRequest) (*RuntimeRunSummary, error) {
		mu.Lock()
		keys = append(keys, authorization.IdempotencyKey)
		mu.Unlock()
		return &RuntimeRunSummary{
			RunID:         "99999999-9999-4999-8999-999999999999",
			Status:        RuntimeRunRunning,
			DispatchState: RuntimeDispatchPending,
		}, nil
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	attempt := &activeRuntimeAttempt{
		identity: runtimeTestAttemptIdentity(RuntimeIdentity{
			WorkerID:         "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa",
			RuntimeSessionID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
			SessionEpoch:     1,
		}),
		payload: DurableAssignmentPayload{
			NodeEnvelope:         "ol_ctx_v2.header.payload.signature",
			AgentInvocationToken: "ol_inv_v2.header.payload.signature",
		},
		ctx: ctx,
	}
	node := &RuntimeWorker{runtimeClient: client}
	body := RuntimeJSONMap{"same": "body"}
	if _, err := node.callAgentForAttempt(ctx, attempt, testTargetAgentID, body, RuntimeCallOptions{}); err == nil || !strings.Contains(err.Error(), "idempotency key is required") {
		t.Fatalf("missing key error = %v", err)
	}
	if _, err := node.callAgentForAttempt(ctx, attempt, testTargetAgentID, body, RuntimeCallOptions{IdempotencyKey: " normalized "}); err == nil || !strings.Contains(err.Error(), "without surrounding spaces") {
		t.Fatalf("normalized key error = %v", err)
	}
	for _, key := range []string{"intent-a", "intent-b", "intent-a"} {
		if _, err := node.callAgentForAttempt(ctx, attempt, testTargetAgentID, body, RuntimeCallOptions{IdempotencyKey: key}); err != nil {
			t.Fatalf("delegated call with key %q: %v", key, err)
		}
	}
	attempt.finished.Store(true)
	if _, err := node.callAgentForAttempt(context.Background(), attempt, testTargetAgentID, body, RuntimeCallOptions{IdempotencyKey: "after-handler"}); !errors.Is(err, context.Canceled) {
		t.Fatalf("post-handler delegated call error = %v, want context.Canceled", err)
	}
	mu.Lock()
	defer mu.Unlock()
	if strings.Join(keys, ",") != "intent-a,intent-b,intent-a" {
		t.Fatalf("forwarded intent keys = %#v", keys)
	}
}

func TestRuntimeTypedACKMismatchNeverClearsSpool(t *testing.T) {
	store := openRuntimeStoreForTest(t, t.TempDir())
	identity := persistStartedAssignmentForTest(t, store, "typed-ack")
	event, err := store.AppendEvent(identity, "run.message.delta", json.RawMessage(`{"text":"ack me"}`))
	if err != nil {
		t.Fatal(err)
	}
	resultID, err := newRuntimeUUID()
	if err != nil {
		t.Fatal(err)
	}
	resultPayload := RuntimeRunResultPayload{
		AttemptIdentity: sdkAttemptIdentity(identity), ResultID: resultID, Status: "success",
		Output: map[string]any{"ok": true}, DurationMS: 1, FinalClientEventSeq: 1,
	}
	resultRaw, err := json.Marshal(resultPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.StoreResult(ResultSpoolRecord{
		Identity: identity, ResultID: resultID, FinalClientEventSeq: 1, Status: "success", Payload: resultRaw,
	}); err != nil {
		t.Fatal(err)
	}
	client := newFakeRuntimeClient()
	client.eventFn = func(_ context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
		return &RuntimeRunEventAckPayload{
			ClientEventID: "dddddddd-dddd-4ddd-8ddd-dddddddddddd", ClientEventSeq: request.ClientEventSeq, Sequence: 1,
		}, nil
	}
	node := &RuntimeWorker{store: store, runtimeClient: client, runtimeCtx: context.Background()}
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if err := node.flushAttemptSpool(record, spoolPermission{events: true, result: true}); !errors.Is(err, ErrRuntimeProtocolMismatch) {
		t.Fatalf("Event ACK mismatch error = %v", err)
	}
	pending, err := store.PendingEvents(identity.AttemptID)
	if err != nil || len(pending) != 1 || pending[0].ClientEventID != event.ClientEventID {
		t.Fatalf("Event cleared by mismatched ACK: %#v err=%v", pending, err)
	}

	client.eventFn = nil
	client.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		return successfulResultACK("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"), nil
	}
	if err := node.flushAttemptSpool(record, spoolPermission{events: true, result: true}); !errors.Is(err, ErrRuntimeProtocolMismatch) {
		t.Fatalf("Result ACK mismatch error = %v", err)
	}
	if _, err := store.PendingResult(identity.AttemptID); err != nil {
		t.Fatalf("Result cleared by mismatched ACK: %v", err)
	}
}

func TestRuntimeResultEventsMissingReplaysRetainedRangeBeforeRetry(t *testing.T) {
	store := openRuntimeStoreForTest(t, t.TempDir())
	defer store.Close()
	identity := persistStartedAssignmentForTest(t, store, "missing-events")
	event, err := store.AppendEvent(identity, "run.message.delta", json.RawMessage(`{"text":"retained"}`))
	if err != nil {
		t.Fatal(err)
	}
	if err = store.AckEvent(identity.AttemptID, event.ClientEventID, event.ClientEventSeq); err != nil {
		t.Fatal(err)
	}
	resultID, err := newRuntimeUUID()
	if err != nil {
		t.Fatal(err)
	}
	resultPayload := RuntimeRunResultPayload{
		AttemptIdentity: sdkAttemptIdentity(identity), ResultID: resultID, Status: "success",
		Output: map[string]any{"ok": true}, DurationMS: 1, FinalClientEventSeq: 1,
	}
	resultRaw, err := json.Marshal(resultPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err = store.StoreResult(ResultSpoolRecord{
		Identity: identity, ResultID: resultID, FinalClientEventSeq: 1,
		Status: "success", Payload: resultRaw,
	}); err != nil {
		t.Fatal(err)
	}

	client := newFakeRuntimeClient()
	var mu sync.Mutex
	steps := make([]string, 0, 3)
	client.eventFn = func(_ context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
		mu.Lock()
		steps = append(steps, "event")
		mu.Unlock()
		if request.ClientEventID != event.ClientEventID || request.ClientEventSeq != 1 {
			return nil, errors.New("missing Event replay identity changed")
		}
		return &RuntimeRunEventAckPayload{
			ClientEventID: request.ClientEventID, ClientEventSeq: request.ClientEventSeq,
			Sequence: 91, Replayed: true,
		}, nil
	}
	var resultCalls int
	client.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		mu.Lock()
		steps = append(steps, "result")
		resultCalls++
		call := resultCalls
		mu.Unlock()
		if call == 1 {
			return nil, &Error{
				Code: "EVENTS_MISSING", Message: "Event range is missing",
				Details: RuntimeErrorBody{
					Code: "EVENTS_MISSING", Message: "Event range is missing",
					MissingEventRanges: []RuntimeEventRange{{Start: 1, End: 1}},
				},
			}
		}
		return successfulResultACK(request.ResultID), nil
	}
	node := &RuntimeWorker{store: store, runtimeClient: client, runtimeCtx: context.Background()}
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	if err = node.flushAttemptSpool(record, spoolPermission{events: true, result: true}); err != nil {
		t.Fatal(err)
	}
	mu.Lock()
	got := strings.Join(steps, ",")
	mu.Unlock()
	if got != "result,event,result" {
		t.Fatalf("missing Event repair order = %q", got)
	}
	usage, err := store.SpoolUsage()
	if err != nil {
		t.Fatal(err)
	}
	if usage.Records != 0 {
		t.Fatalf("ACKed Event/Result remained after Result ACK: %#v", usage)
	}
	if records, recordsErr := store.Assignments(); recordsErr != nil || len(records) != 0 {
		t.Fatalf("completed assignment remained: %#v, %v", records, recordsErr)
	}
}

func TestRuntimeStopCancelsBlockedSessionStartup(t *testing.T) {
	client := newFakeRuntimeClient()
	client.createFn = func(ctx context.Context, _ RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		<-ctx.Done()
		return nil, ctx.Err()
	}
	node := newRuntimeWorkerForTest(t.TempDir(), client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{}, nil
	}))
	errCh := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, client.ready, 2*time.Second, "blocked session startup")
	stopCtx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	if err := node.Stop(stopCtx); err != nil {
		t.Fatalf("Stop during startup: %v", err)
	}
	select {
	case err := <-errCh:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("Start error after startup cancellation = %v", err)
		}
	case <-stopCtx.Done():
		t.Fatalf("Start remained blocked after Stop: %v", stopCtx.Err())
	}
}

func TestRuntimeMessageBoundaryAndOversizedResultFallback(t *testing.T) {
	request := RuntimeRunEventPayload{
		AttemptIdentity: RuntimeAttemptIdentity{
			RunID: testRunID, AttemptID: testAttemptID, LeaseID: testLeaseID, FencingToken: 1,
			NodeID: testNodeID, AgentID: testAgentID,
			WorkerID: "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", RuntimeSessionID: "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb",
		},
		ClientEventID:  "cccccccc-cccc-4ccc-8ccc-cccccccccccc",
		ClientEventSeq: 1,
		EventType:      "run.message.delta",
		Payload:        map[string]any{"blob": ""},
	}
	base, err := json.Marshal(request)
	if err != nil {
		t.Fatal(err)
	}
	filler := int(RuntimeMaxMessageBytes) - len(base)
	request.Payload["blob"] = strings.Repeat("a", filler)
	if encoded, _ := json.Marshal(request); int64(len(encoded)) != RuntimeMaxMessageBytes {
		t.Fatalf("boundary fixture size = %d", len(encoded))
	}
	if err := enforceRuntimeMessageLimit(request); err != nil {
		t.Fatalf("exact 4 MiB message rejected: %v", err)
	}
	request.Payload["blob"] = strings.Repeat("a", filler+1)
	if err := enforceRuntimeMessageLimit(request); !errors.Is(err, ErrRuntimeMessageTooLarge) {
		t.Fatalf("oversized message error = %v", err)
	}

	store := openRuntimeStoreForTest(t, t.TempDir())
	identity := persistStartedAssignmentForTest(t, store, "oversized-result")
	node := &RuntimeWorker{store: store}
	attempt := &activeRuntimeAttempt{identity: identity}
	if err := node.persistRunResult(attempt, RuntimeResult{
		Status: "success",
		Output: RuntimeJSONMap{"blob": strings.Repeat("z", int(RuntimeMaxMessageBytes))},
	}); err != nil {
		t.Fatal(err)
	}
	spooled, err := store.PendingResult(identity.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	var fallback RuntimeRunResultPayload
	if err := decodeStrictJSON(spooled.Payload, &fallback); err != nil {
		t.Fatal(err)
	}
	if fallback.Status != "failed" || fallback.Error == nil || fallback.Error.ErrorCode != "RESULT_TOO_LARGE" {
		t.Fatalf("oversized result fallback = %#v", fallback)
	}
}

func TestRuntimeExpiredAttemptDeadlineDoesNotInvokeAdapter(t *testing.T) {
	store := openRuntimeStoreForTest(t, t.TempDir())
	identity := runtimeTestAttemptIdentity(store.Identity())
	if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
		t.Fatal(err)
	}
	payload := runtimeTestAssignmentPayload(identity)
	payload.AttemptDeadlineAt = time.Now().Add(-time.Second).UTC()
	payload.RunDeadlineAt = time.Now().Add(time.Minute).UTC()
	if err := store.StoreAssignmentPayload(payload); err != nil {
		t.Fatal(err)
	}
	for _, state := range []AssignmentState{AssignmentStateACKSent, AssignmentStateConfirmed} {
		if _, err := store.AdvanceAssignment(identity.AssignmentMessageID, state); err != nil {
			t.Fatal(err)
		}
	}
	record, err := store.Assignment(identity.AssignmentMessageID)
	if err != nil {
		t.Fatal(err)
	}
	var adapterCalls atomic.Int32
	node := &RuntimeWorker{
		Handler: testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
			adapterCalls.Add(1)
			return RuntimeJSONMap{}, nil
		}),
		runtimeClient: newFakeRuntimeClient(),
		store:         store,
		runtimeCtx:    context.Background(),
		active:        make(map[string]*activeRuntimeAttempt),
		wakeSpool:     make(chan struct{}, 1),
	}
	if err := node.startConfirmedAttempt(record, payload, time.Now().Add(time.Minute)); err != nil {
		t.Fatal(err)
	}
	done := make(chan struct{})
	go func() {
		node.executions.Wait()
		close(done)
	}()
	waitForTestSignal(t, done, 2*time.Second, "expired Attempt failure Result")
	if adapterCalls.Load() != 0 {
		t.Fatalf("expired Attempt invoked adapter %d time(s)", adapterCalls.Load())
	}
	result, err := store.PendingResult(identity.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	var payloadResult RuntimeRunResultPayload
	if err := decodeStrictJSON(result.Payload, &payloadResult); err != nil {
		t.Fatal(err)
	}
	if payloadResult.Error == nil || payloadResult.Error.ErrorCode != "ATTEMPT_DEADLINE_EXCEEDED" {
		t.Fatalf("expired Attempt Result = %#v", payloadResult)
	}
}

func TestAssignmentPayloadIsEncryptedDurableAndFailsClosedOnCorruption(t *testing.T) {
	dataDir := t.TempDir()
	store := openRuntimeStoreForTest(t, dataDir)
	identity := testAttemptIdentity(store.Identity(), "assignment-payload")
	if err := store.CreateAssignment(testAssignmentRecord(identity)); err != nil {
		t.Fatal(err)
	}
	payload := runtimeTestAssignmentPayload(identity)
	payload.Input = json.RawMessage(`{"private_marker":"never-plaintext"}`)
	payload.AgentInvocationToken = "ol_inv_v2.private.token.signature"
	if err := store.StoreAssignmentPayload(payload); err != nil {
		t.Fatal(err)
	}
	if err := store.StoreAssignmentPayload(payload); err != nil {
		t.Fatalf("idempotent payload replay: %v", err)
	}
	conflict := payload
	conflict.AgentInvocationToken = "ol_inv_v2.different.token.signature"
	if err := store.StoreAssignmentPayload(conflict); !errors.Is(err, ErrSpoolRecordConflict) {
		t.Fatalf("conflicting payload error = %v", err)
	}
	paths, err := filepath.Glob(filepath.Join(dataDir, assignmentSpoolDirectory, "*"+spoolRecordExtension))
	if err != nil || len(paths) != 1 {
		t.Fatalf("assignment spool paths = %#v err=%v", paths, err)
	}
	raw, err := os.ReadFile(paths[0])
	if err != nil {
		t.Fatal(err)
	}
	if bytes.Contains(raw, []byte("never-plaintext")) || bytes.Contains(raw, []byte("ol_inv_v2.private.token.signature")) {
		t.Fatal("encrypted assignment spool exposed input or invocation token")
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}

	store = openRuntimeStoreForTest(t, dataDir)
	replayed, err := store.AssignmentPayload(identity.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(replayed.Input, payload.Input) || replayed.AgentInvocationToken != payload.AgentInvocationToken || replayed.CreatedAt.IsZero() {
		t.Fatalf("replayed assignment payload = %#v", replayed)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	raw[len(raw)/2] ^= 0xff
	if err := os.WriteFile(paths[0], raw, 0o600); err != nil {
		t.Fatal(err)
	}
	corruptStore, err := OpenFileRuntimeStore(dataDir)
	if corruptStore != nil {
		_ = corruptStore.Close()
	}
	if !errors.Is(err, ErrRuntimeRecordCorrupt) {
		t.Fatalf("corrupt assignment payload open error = %v", err)
	}
}

func runtimeTestAttemptIdentity(identity RuntimeIdentity) AttemptIdentity {
	return AttemptIdentity{
		NodeID:              testNodeID,
		AgentID:             testAgentID,
		WorkerID:            identity.WorkerID,
		RuntimeSessionID:    identity.RuntimeSessionID,
		SessionEpoch:        identity.SessionEpoch,
		AssignmentMessageID: deterministicRuntimeUUID("assignment", testAttemptID, testLeaseID),
		RunID:               testRunID,
		AttemptID:           testAttemptID,
		OfferID:             deterministicRuntimeUUID("offer", testAttemptID, testLeaseID),
		LeaseID:             testLeaseID,
		FencingToken:        1,
	}
}

func runtimeTestAssignmentPayload(identity AttemptIdentity) DurableAssignmentPayload {
	return DurableAssignmentPayload{
		Identity:             identity,
		Input:                json.RawMessage(`{"task":"resume"}`),
		Metadata:             json.RawMessage(`{"source":"test"}`),
		NodeEnvelope:         "ol_ctx_v2.header.payload.signature",
		AgentInvocationToken: "ol_inv_v2.header.payload.signature",
		OfferExpiresAt:       time.Now().Add(time.Minute).UTC(),
		AttemptDeadlineAt:    time.Now().Add(2 * time.Minute).UTC(),
		RunDeadlineAt:        time.Now().Add(3 * time.Minute).UTC(),
	}
}

func startRuntimeWorkerForTest(node *RuntimeWorker) <-chan error {
	errCh := make(chan error, 1)
	go func() { errCh <- node.Start(context.Background()) }()
	return errCh
}

func stopRuntimeWorkerForTest(t *testing.T, node *RuntimeWorker, errCh <-chan error) {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 4*time.Second)
	defer cancel()
	if err := node.Stop(ctx); err != nil {
		t.Fatalf("stop runtime node: %v", err)
	}
	select {
	case err := <-errCh:
		if err != nil {
			t.Fatalf("runtime node returned: %v", err)
		}
	case <-ctx.Done():
		t.Fatalf("runtime node did not return: %v", ctx.Err())
	}
}

func TestRuntimeWorkerIsSingleUse(t *testing.T) {
	client := newFakeRuntimeClient()
	worker := newRuntimeWorkerForTest(t.TempDir(), client, RuntimeHandlerFunc(func(
		context.Context,
		RuntimeContext,
	) (RuntimeResult, error) {
		return RuntimeResult{Status: "succeeded"}, nil
	}))

	runDone := startRuntimeWorkerForTest(worker)
	waitForTestSignal(t, client.ready, time.Second, "Runtime session creation")
	stopRuntimeWorkerForTest(t, worker, runDone)

	if err := worker.Start(context.Background()); err == nil || !strings.Contains(err.Error(), "cannot be restarted") {
		t.Fatalf("second Start error = %v, want single-use rejection", err)
	}
}

func TestRuntimeDeterministicIdentityDomainIsSDKOwned(t *testing.T) {
	joined := joinRuntimeIdentityParts([]string{"assignment", testAttemptID, testLeaseID})
	if !strings.HasPrefix(joined, "openlinker/runtime/deterministic-id\x00") {
		t.Fatalf("deterministic identity domain = %q", joined)
	}
	if strings.Contains(joined, "agent-node") {
		t.Fatalf("temporary Adapter leaked into SDK identity domain: %q", joined)
	}
}

func waitForTestSignal(t *testing.T, signal <-chan struct{}, timeout time.Duration, label string) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(timeout):
		t.Fatalf("timed out waiting for %s", label)
	}
}

func eventuallyForTest(t *testing.T, timeout time.Duration, condition func() bool, label string) {
	t.Helper()
	deadline := time.Now().Add(timeout)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", label)
}
