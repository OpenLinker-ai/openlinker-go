package openlinker

import (
	"context"
	"errors"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeWorkerDrainWaitsForCoreFenceHandlerAndDurableSpool(t *testing.T) {
	client := newFakeRuntimeClient()
	var claimed atomic.Bool
	client.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		if claimed.CompareAndSwap(false, true) {
			return assignedRunForHello(client.helloSnapshot()), nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}

	serverDrainEntered := make(chan struct{})
	allowServerDrain := make(chan struct{})
	var serverDrainOnce sync.Once
	var serverDrainCalls atomic.Int32
	client.drainFn = func(ctx context.Context, runtimeSessionID string, request RuntimeDrainPayload) (*RuntimeDrainPayload, error) {
		if runtimeSessionID != client.helloSnapshot().RuntimeSessionID {
			return nil, errors.New("drain used another Runtime Session")
		}
		call := serverDrainCalls.Add(1)
		if call == 1 {
			serverDrainOnce.Do(func() { close(serverDrainEntered) })
			select {
			case <-ctx.Done():
				return nil, ctx.Err()
			case <-allowServerDrain:
			}
		}
		response := request
		response.ReasonCode = "FIRST_WRITER_REASON"
		response.DeadlineAt = request.DeadlineAt.Add(-time.Second)
		response.Capacity = 0
		if call == 1 {
			response.Inflight = 1
		} else {
			response.Inflight = 0
		}
		return &response, nil
	}

	handlerStarted := make(chan struct{})
	releaseHandler := make(chan struct{})
	resultEntered := make(chan struct{})
	releaseResult := make(chan struct{})
	client.resultFn = func(ctx context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		select {
		case <-resultEntered:
		default:
			close(resultEntered)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-releaseResult:
		}
		return successfulResultACK(request.ResultID), nil
	}

	node := newRuntimeWorkerForTest(t.TempDir(), client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		close(handlerStarted)
		<-releaseHandler
		return RuntimeJSONMap{"drained": true}, nil
	}))
	runDone := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, handlerStarted, 2*time.Second, "handler start")

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- node.Drain(context.Background(), RuntimeWorkerDrainOptions{
			Timeout:    2 * time.Second,
			ReasonCode: "DEPLOYMENT",
		})
	}()
	waitForTestSignal(t, serverDrainEntered, time.Second, "server drain request")
	assertNoDrainResult(t, drainDone, "before Core committed the drain fence")
	capacity, inflight := node.capacitySnapshot()
	if capacity != 0 || inflight != 1 || !node.isDraining() {
		t.Fatalf("local drain fence = capacity %d inflight %d draining %v", capacity, inflight, node.isDraining())
	}
	close(allowServerDrain)
	assertNoDrainResult(t, drainDone, "before the handler completed")
	close(releaseHandler)
	waitForTestSignal(t, resultEntered, time.Second, "durable Result upload")
	assertNoDrainResult(t, drainDone, "before Core acknowledged the durable Result")
	close(releaseResult)

	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("drain: %v", err)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("drain did not finish")
	}
	if err := <-runDone; err != nil {
		t.Fatalf("worker returned: %v", err)
	}
	if serverDrainCalls.Load() < 2 {
		t.Fatalf("Core drain checks = %d, want at least 2", serverDrainCalls.Load())
	}
	client.mu.Lock()
	defer client.mu.Unlock()
	if len(client.drains) < 2 || client.drains[0].Capacity != 0 || client.drains[0].Inflight != 1 || client.drains[0].ReasonCode != "DEPLOYMENT" {
		t.Fatalf("drain requests = %#v", client.drains)
	}
	if len(client.closes) != 1 {
		t.Fatalf("session closes = %#v", client.closes)
	}
}

func TestRuntimeWorkerDrainTimeoutFailsClosedAndPreservesSpool(t *testing.T) {
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
	client.drainFn = func(_ context.Context, _ string, request RuntimeDrainPayload) (*RuntimeDrainPayload, error) {
		response := request
		response.Capacity = 0
		response.Inflight = 1
		return &response, nil
	}
	resultEntered := make(chan struct{})
	client.resultFn = func(ctx context.Context, _ RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		select {
		case <-resultEntered:
		default:
			close(resultEntered)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	node := newRuntimeWorkerForTest(dataDir, client, testRuntimeHandlerFunc(func(_ context.Context, _ any, runCtx RuntimeContext) (any, error) {
		if err := runCtx.Emit("run.progress", RuntimeJSONMap{"phase": "durable"}); err != nil {
			return nil, err
		}
		return RuntimeJSONMap{"pending": true}, nil
	}))
	runDone := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, resultEntered, 2*time.Second, "blocked Result upload")

	err := node.Drain(context.Background(), RuntimeWorkerDrainOptions{Timeout: 40 * time.Millisecond})
	var timeoutErr *RuntimeDrainTimeoutError
	if !errors.As(err, &timeoutErr) {
		t.Fatalf("drain error = %T %v, want RuntimeDrainTimeoutError", err, err)
	}
	if timeoutErr.Spool.Empty || timeoutErr.Spool.Assignments != 1 || timeoutErr.Spool.Events != 1 || timeoutErr.Spool.Results != 1 {
		t.Fatalf("timeout spool = %#v", timeoutErr.Spool)
	}
	select {
	case runErr := <-runDone:
		if runErr != nil {
			t.Fatalf("worker returned: %v", runErr)
		}
	case <-time.After(3 * time.Second):
		t.Fatal("timed-out drain did not stop the worker")
	}

	store := openRuntimeStoreForTest(t, dataDir)
	status, statusErr := store.SpoolStatus()
	if statusErr != nil {
		t.Fatal(statusErr)
	}
	if status.Empty || status.Assignments != 1 || status.Events != 1 || status.Results != 1 {
		t.Fatalf("reopened spool = %#v", status)
	}
}

func TestRuntimeWorkerDrainAndStopRaceCannotReportSuccess(t *testing.T) {
	client := newFakeRuntimeClient()
	drainEntered := make(chan struct{})
	client.drainFn = func(ctx context.Context, _ string, _ RuntimeDrainPayload) (*RuntimeDrainPayload, error) {
		select {
		case <-drainEntered:
		default:
			close(drainEntered)
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	node := newRuntimeWorkerForTest(t.TempDir(), client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	runDone := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, client.ready, time.Second, "worker ready")
	drainDone := make(chan error, 1)
	go func() {
		drainDone <- node.Drain(context.Background(), RuntimeWorkerDrainOptions{Timeout: time.Second})
	}()
	waitForTestSignal(t, drainEntered, time.Second, "drain request")
	stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	if err := node.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	select {
	case err := <-drainDone:
		if err == nil {
			t.Fatal("drain reported success after Stop won the race")
		}
	case <-time.After(time.Second):
		t.Fatal("drain did not observe concurrent Stop")
	}
}

func TestRuntimeWorkerDrainWaitsForAttachAndRetriesTransportSwitch(t *testing.T) {
	client := newFakeRuntimeClient()
	attachEntered := make(chan struct{})
	releaseAttach := make(chan struct{})
	client.createFn = func(ctx context.Context, _ RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		close(attachEntered)
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-releaseAttach:
			return client.readyPayload(), nil
		}
	}
	var drainCalls atomic.Int32
	drainReached := make(chan struct{})
	client.drainFn = func(_ context.Context, _ string, request RuntimeDrainPayload) (*RuntimeDrainPayload, error) {
		if drainCalls.Add(1) == 1 {
			return nil, ErrRuntimeTransportSwitching
		}
		select {
		case <-drainReached:
		default:
			close(drainReached)
		}
		response := request
		response.Capacity = 0
		response.Inflight = 0
		return &response, nil
	}
	node := newRuntimeWorkerForTest(t.TempDir(), client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	runDone := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, attachEntered, time.Second, "Session attach")
	drainDone := make(chan error, 1)
	go func() {
		drainDone <- node.Drain(context.Background(), RuntimeWorkerDrainOptions{Timeout: 2 * time.Second})
	}()
	assertNoDrainResult(t, drainDone, "before attachment")
	close(releaseAttach)
	waitForTestSignal(t, drainReached, time.Second, "post-switch drain retry")
	if err := <-drainDone; err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if drainCalls.Load() != 2 {
		t.Fatalf("drain calls = %d, want exactly 2", drainCalls.Load())
	}
}

func TestRuntimeWorkerDrainCannotSucceedWhenStopWinsAfterFinalEvidence(t *testing.T) {
	client := newFakeRuntimeClient()
	client.drainFn = func(_ context.Context, _ string, request RuntimeDrainPayload) (*RuntimeDrainPayload, error) {
		response := request
		response.Capacity = 0
		response.Inflight = 0
		return &response, nil
	}
	node := newRuntimeWorkerForTest(t.TempDir(), client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	proofReached := make(chan struct{})
	releaseProof := make(chan struct{})
	node.drainBeforeStop = func() {
		close(proofReached)
		<-releaseProof
	}
	runDone := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, client.ready, time.Second, "worker ready")
	drainDone := make(chan error, 1)
	go func() {
		drainDone <- node.Drain(context.Background(), RuntimeWorkerDrainOptions{Timeout: time.Second})
	}()
	waitForTestSignal(t, proofReached, time.Second, "final drain evidence")
	stopDone := make(chan error, 1)
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), time.Second)
		defer cancel()
		stopDone <- node.Stop(stopCtx)
	}()
	eventuallyForTest(t, time.Second, func() bool {
		node.lifecycleMu.Lock()
		defer node.lifecycleMu.Unlock()
		return node.stopRequested && !node.drainOwnsStop
	}, "external Stop ownership")
	close(releaseProof)
	if err := <-drainDone; !errors.Is(err, errRuntimeWorkerStoppedBeforeDrain) {
		t.Fatalf("drain error = %v, want stopped-before-drain", err)
	}
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

type legacyRuntimeStore struct{ RuntimeStore }

var _ RuntimeStore = (*legacyRuntimeStore)(nil)

func TestRuntimeWorkerDrainSupportsLegacyCustomStoreWithoutSpoolStatus(t *testing.T) {
	inner := openRuntimeStoreForTest(t, t.TempDir())
	store := &legacyRuntimeStore{RuntimeStore: inner}
	client := newFakeRuntimeClient()
	node := newRuntimeWorkerForTest("", client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	node.Store = store
	runDone := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, client.ready, time.Second, "legacy-store worker ready")
	if err := node.Drain(context.Background(), RuntimeWorkerDrainOptions{Timeout: time.Second}); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

type legacyRuntimeClient struct{ RuntimeClient }

var _ RuntimeClient = (*legacyRuntimeClient)(nil)

func TestRuntimeWorkerStartSupportsLegacyCustomClientWithoutDrain(t *testing.T) {
	inner := newFakeRuntimeClient()
	client := &legacyRuntimeClient{RuntimeClient: inner}
	if _, implementsDrain := any(client).(runtimeDrainClient); implementsDrain {
		t.Fatal("legacy Runtime Client unexpectedly implements optional drain")
	}
	node := newRuntimeWorkerForTest(t.TempDir(), client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	runDone := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, inner.ready, time.Second, "legacy-client worker ready")
	stopRuntimeWorkerForTest(t, node, runDone)
}

func TestRuntimeWorkerDrainFailsClosedForLegacyCustomClientWithoutDrain(t *testing.T) {
	inner := newFakeRuntimeClient()
	client := &legacyRuntimeClient{RuntimeClient: inner}
	node := newRuntimeWorkerForTest(t.TempDir(), client, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	runDone := startRuntimeWorkerForTest(node)
	waitForTestSignal(t, inner.ready, time.Second, "legacy-client worker ready")
	err := node.Drain(context.Background(), RuntimeWorkerDrainOptions{Timeout: time.Second})
	if !errors.Is(err, ErrRuntimeProtocolMismatch) || !strings.Contains(err.Error(), "does not implement session drain") {
		t.Fatalf("legacy-client drain error = %v", err)
	}
	if runErr := <-runDone; runErr != nil {
		t.Fatal(runErr)
	}
}

func assertNoDrainResult(t *testing.T, result <-chan error, stage string) {
	t.Helper()
	select {
	case err := <-result:
		t.Fatalf("drain returned %v %s", err, stage)
	case <-time.After(30 * time.Millisecond):
	}
}
