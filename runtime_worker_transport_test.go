package openlinker

import (
	"context"
	"errors"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

func TestRuntimeTransportAutoConfirmsBeforeExecuteAndSwitchesWSPullWS(t *testing.T) {
	tracker := &runtimeClaimTracker{}
	ackEntered := make(chan struct{})
	allowConfirmation := make(chan struct{})
	pullResumed := make(chan struct{}, 1)
	secondWSResumed := make(chan struct{}, 1)

	firstWSClient := newFakeRuntimeClient()
	configureSwitchClient(firstWSClient, testCoreInstanceID, tracker, nil)
	firstWSClient.ackFn = func(ctx context.Context, request RuntimeAssignmentAckPayload) (*RuntimeAssignmentConfirmedPayload, error) {
		select {
		case <-ackEntered:
		default:
			close(ackEntered)
		}
		select {
		case <-ctx.Done():
			return nil, ctx.Err()
		case <-allowConfirmation:
		}
		return confirmedAssignment(request.AttemptIdentity), nil
	}
	firstWS := newFakeRuntimeDuplex(firstWSClient)

	pull := newFakeRuntimeClient()
	configureSwitchClient(pull, "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa", tracker, pullResumed)
	secondWSClient := newFakeRuntimeClient()
	configureSwitchClient(secondWSClient, "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb", tracker, secondWSResumed)
	secondWS := newFakeRuntimeDuplex(secondWSClient)
	dialer := &fakeRuntimeTransportDialer{connections: []RuntimeDuplexClient{firstWS, secondWS}}

	adapter := newBlockingSwitchAdapter()
	node := newRuntimeWorkerForTest(t.TempDir(), pull, adapter)
	node.Transport = RuntimeTransportAuto
	node.runtimeDialer = dialer
	node.HeartbeatInterval = time.Hour
	node.RetryMinimum = 5 * time.Millisecond
	node.RetryMaximum = 20 * time.Millisecond

	runDone := make(chan error, 1)
	go func() { runDone <- node.Start(context.Background()) }()

	select {
	case <-ackEntered:
	case <-time.After(3 * time.Second):
		t.Fatal("WebSocket assignment was not ACKed")
	}
	select {
	case <-adapter.started:
		t.Fatal("adapter started before run.assignment.confirmed")
	case <-time.After(75 * time.Millisecond):
	}
	close(allowConfirmation)
	select {
	case <-adapter.started:
	case <-time.After(3 * time.Second):
		t.Fatal("adapter did not start after assignment confirmation")
	}

	firstWS.disconnect(errors.New("core A disconnected"))
	select {
	case <-pullResumed:
	case <-time.After(3 * time.Second):
		t.Fatal("durable state was not resumed on HTTP pull")
	}
	waitForRuntimeTransport(t, node, RuntimeTransportPullActive)
	if adapter.count.Load() != 1 {
		t.Fatalf("adapter executions after WS to pull = %d", adapter.count.Load())
	}

	dialer.allowProbe.Store(true)
	select {
	case <-secondWSResumed:
	case <-time.After(3 * time.Second):
		t.Fatal("durable state was not resumed on replacement WebSocket")
	}
	waitForRuntimeTransport(t, node, RuntimeTransportWebSocketActive)
	if adapter.count.Load() != 1 {
		t.Fatalf("adapter executions after WS to pull to WS = %d", adapter.count.Load())
	}
	node.stateMu.RLock()
	ready := node.ready
	node.stateMu.RUnlock()
	if ready == nil || ready.CoreInstanceID != "bbbbbbbb-bbbb-4bbb-8bbb-bbbbbbbbbbbb" {
		t.Fatalf("replacement Core ready = %#v", ready)
	}
	if tracker.maximum.Load() > 1 {
		t.Fatalf("concurrent claims across transports = %d", tracker.maximum.Load())
	}

	close(adapter.release)
	stopCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := node.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	if adapter.count.Load() != 1 {
		t.Fatalf("final adapter executions = %d", adapter.count.Load())
	}
}

func TestRuntimeTransportAutoFallsBackWhenInitialWebSocketIsUnavailable(t *testing.T) {
	pull := newFakeRuntimeClient()
	dialer := &fakeRuntimeTransportDialer{}
	node := newRuntimeWorkerForTest(t.TempDir(), pull, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	node.Transport = RuntimeTransportAuto
	node.runtimeDialer = dialer
	node.RetryMinimum = 5 * time.Millisecond
	node.RetryMaximum = 20 * time.Millisecond
	runDone := make(chan error, 1)
	go func() { runDone <- node.Start(context.Background()) }()
	select {
	case <-pull.ready:
	case <-time.After(3 * time.Second):
		t.Fatal("Pull session was not attached after initial WS failure")
	}
	waitForRuntimeTransport(t, node, RuntimeTransportPullActive)
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := node.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}

func TestRuntimeTransportPullRetriesSessionConflict(t *testing.T) {
	pull := newFakeRuntimeClient()
	var creates atomic.Int32
	attached := make(chan struct{})
	pull.createFn = func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		if creates.Add(1) == 1 {
			return nil, runtimeSessionConflictTestError()
		}
		select {
		case <-attached:
		default:
			close(attached)
		}
		return pull.readyPayload(), nil
	}
	node := newRuntimeWorkerForTest(t.TempDir(), pull, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	node.Transport = RuntimeTransportPull
	node.runtimeDialer = &fakeRuntimeTransportDialer{}
	node.RetryMinimum = time.Millisecond
	node.RetryMaximum = 5 * time.Millisecond
	runDone := make(chan error, 1)
	go func() { runDone <- node.Start(context.Background()) }()
	select {
	case <-attached:
	case <-time.After(3 * time.Second):
		t.Fatal("Pull did not retry the transient attachment conflict")
	}
	waitForRuntimeTransport(t, node, RuntimeTransportPullActive)
	stopRuntimeWorkerTest(t, node, runDone)
	if creates.Load() < 2 {
		t.Fatalf("Pull create calls = %d", creates.Load())
	}
}

func TestRuntimeTransportWebSocketRetriesSessionConflict(t *testing.T) {
	conflictedClient := newFakeRuntimeClient()
	conflictedClient.createFn = func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		return nil, runtimeSessionConflictTestError()
	}
	conflicted := newFakeRuntimeDuplex(conflictedClient)
	connected := newFakeRuntimeDuplex(newFakeRuntimeClient())
	dialer := &fakeRuntimeTransportDialer{connections: []RuntimeDuplexClient{conflicted, connected}}
	pull := newFakeRuntimeClient()
	node := newRuntimeWorkerForTest(t.TempDir(), pull, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	node.Transport = RuntimeTransportWebSocket
	node.runtimeDialer = dialer
	node.RetryMinimum = time.Millisecond
	node.RetryMaximum = 5 * time.Millisecond
	runDone := make(chan error, 1)
	go func() { runDone <- node.Start(context.Background()) }()
	select {
	case <-connected.ready:
	case <-time.After(3 * time.Second):
		t.Fatal("WebSocket did not retry the transient attachment conflict")
	}
	waitForRuntimeTransport(t, node, RuntimeTransportWebSocketActive)
	stopRuntimeWorkerTest(t, node, runDone)
}

func TestRuntimeTransportAutoRestoresPullAfterWebSocketSessionConflict(t *testing.T) {
	pull := newFakeRuntimeClient()
	var pullCreates atomic.Int32
	pull.createFn = func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		pullCreates.Add(1)
		return pull.readyPayload(), nil
	}
	dialer := &fakeRuntimeTransportDialer{}
	node := newRuntimeWorkerForTest(t.TempDir(), pull, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	node.Transport = RuntimeTransportAuto
	node.runtimeDialer = dialer
	node.RetryMinimum = time.Millisecond
	node.RetryMaximum = 5 * time.Millisecond
	runDone := make(chan error, 1)
	go func() { runDone <- node.Start(context.Background()) }()
	waitForRuntimeTransport(t, node, RuntimeTransportPullActive)

	conflictedClient := newFakeRuntimeClient()
	conflictedClient.createFn = func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		return nil, runtimeSessionConflictTestError()
	}
	dialer.mu.Lock()
	dialer.connections = append(dialer.connections,
		newFakeRuntimeDuplex(conflictedClient),
		newFakeRuntimeDuplex(newFakeRuntimeClient()),
	)
	dialer.mu.Unlock()
	dialer.allowProbe.Store(true)
	waitForRuntimeTransport(t, node, RuntimeTransportWebSocketActive)
	stopRuntimeWorkerTest(t, node, runDone)
	if pullCreates.Load() < 2 {
		t.Fatalf("Pull was not restored after the failed WebSocket attach; create calls = %d", pullCreates.Load())
	}
}

func TestRuntimeWorkerStopFencesLatePullAttachment(t *testing.T) {
	pull := newFakeRuntimeClient()
	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	pull.createFn = func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		close(createStarted)
		<-releaseCreate // Simulate a transport that returns success after cancellation.
		return pull.readyPayload(), nil
	}
	firstWS := newFakeRuntimeDuplex(newFakeRuntimeClient())
	node := newRuntimeWorkerForTest(t.TempDir(), pull, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	node.Transport = RuntimeTransportAuto
	node.runtimeDialer = &fakeRuntimeTransportDialer{connections: []RuntimeDuplexClient{firstWS}}
	node.RetryMinimum = time.Millisecond
	node.RetryMaximum = 5 * time.Millisecond
	runDone := make(chan error, 1)
	go func() { runDone <- node.Start(context.Background()) }()
	waitForRuntimeTransport(t, node, RuntimeTransportWebSocketActive)
	firstWS.disconnect(errors.New("force Pull fallback"))
	select {
	case <-createStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("Pull attachment did not start")
	}
	stopDone := make(chan error, 1)
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		stopDone <- node.Stop(stopCtx)
	}()
	waitForRuntimeTransport(t, node, RuntimeTransportStopped)
	close(releaseCreate)
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	pull.mu.Lock()
	closes := append([]RuntimeSessionCloseRequest(nil), pull.closes...)
	pull.mu.Unlock()
	if len(closes) != 1 || closes[0].Reason != "transport_transition_superseded" {
		t.Fatalf("late Pull attachment closes = %#v", closes)
	}
}

func TestRuntimeWorkerStopFencesLateWebSocketAttachment(t *testing.T) {
	pull := newFakeRuntimeClient()
	wsClient := newFakeRuntimeClient()
	createStarted := make(chan struct{})
	releaseCreate := make(chan struct{})
	wsClient.createFn = func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		close(createStarted)
		<-releaseCreate // Simulate a WebSocket ready reply after cancellation.
		return wsClient.readyPayload(), nil
	}
	lateWS := newFakeRuntimeDuplex(wsClient)
	dialer := &fakeRuntimeTransportDialer{}
	node := newRuntimeWorkerForTest(t.TempDir(), pull, testRuntimeHandlerFunc(func(context.Context, any, RuntimeContext) (any, error) {
		return RuntimeJSONMap{"unused": true}, nil
	}))
	node.Transport = RuntimeTransportAuto
	node.runtimeDialer = dialer
	node.RetryMinimum = time.Millisecond
	node.RetryMaximum = 5 * time.Millisecond
	runDone := make(chan error, 1)
	go func() { runDone <- node.Start(context.Background()) }()
	waitForRuntimeTransport(t, node, RuntimeTransportPullActive)
	dialer.mu.Lock()
	dialer.connections = append(dialer.connections, lateWS)
	dialer.mu.Unlock()
	dialer.allowProbe.Store(true)
	select {
	case <-createStarted:
	case <-time.After(3 * time.Second):
		t.Fatal("WebSocket attachment did not start")
	}
	stopDone := make(chan error, 1)
	go func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
		defer cancel()
		stopDone <- node.Stop(stopCtx)
	}()
	waitForRuntimeTransport(t, node, RuntimeTransportStopped)
	close(releaseCreate)
	if err := <-stopDone; err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
	wsClient.mu.Lock()
	closes := append([]RuntimeSessionCloseRequest(nil), wsClient.closes...)
	wsClient.mu.Unlock()
	if len(closes) != 1 || closes[0].Reason != "transport_transition_superseded" {
		t.Fatalf("late WebSocket attachment closes = %#v", closes)
	}
}

func TestSwitchingRuntimeClientCancelsOldGenerationBeforePublishingNew(t *testing.T) {
	oldClient := newFakeRuntimeClient()
	entered := make(chan struct{})
	exited := make(chan struct{})
	oldClient.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		close(entered)
		<-ctx.Done()
		close(exited)
		return nil, ctx.Err()
	}
	gate := newSwitchingRuntimeClient(oldClient)
	epoch, _, _ := gate.beginTransition(RuntimeTransportSwitchingPull)
	if !gate.activateIfCurrent(epoch, RuntimeTransportPull, oldClient) {
		t.Fatal("initial Pull activation was rejected")
	}
	callDone := make(chan error, 1)
	go func() {
		_, err := gate.ClaimRuntimeRun(context.Background(), 25, RuntimeClaimRequest{
			RuntimeSessionID: testRunID, Capacity: 1,
		})
		callDone <- err
	}()
	<-entered
	transitionDone := make(chan struct{})
	go func() {
		gate.beginTransition(RuntimeTransportSwitchingWS)
		close(transitionDone)
	}()
	select {
	case <-exited:
	case <-time.After(time.Second):
		t.Fatal("old generation was not canceled")
	}
	select {
	case <-transitionDone:
	case <-time.After(time.Second):
		t.Fatal("transition did not wait for the old call")
	}
	if !errors.Is(<-callDone, context.Canceled) {
		t.Fatal("old call did not return context cancellation")
	}
	if _, err := gate.ClaimRuntimeRun(context.Background(), 0, RuntimeClaimRequest{}); !errors.Is(err, ErrRuntimeTransportSwitching) {
		t.Fatalf("claim during transition = %v", err)
	}
}

func TestSwitchingRuntimeClientStopFencesLateActivation(t *testing.T) {
	gate := newSwitchingRuntimeClient(newFakeRuntimeClient())
	epoch, _, _ := gate.beginTransition(RuntimeTransportSwitchingPull)
	gate.stop()
	if gate.activateIfCurrent(epoch, RuntimeTransportPull, newFakeRuntimeClient()) {
		t.Fatal("a stopped transport accepted a late attachment")
	}
	_, state, active := gate.snapshot()
	if state != RuntimeTransportStopped || active != nil {
		t.Fatalf("stopped transport snapshot = state %s, active %#v", state, active)
	}
	if next, _, _ := gate.beginTransition(RuntimeTransportConnectingWS); next != 0 {
		t.Fatalf("stopped transport reserved transition epoch %d", next)
	}
}

type fakeRuntimeDuplex struct {
	*fakeRuntimeClient
	done chan struct{}
	once sync.Once
	err  atomic.Value
}

func newFakeRuntimeDuplex(client *fakeRuntimeClient) *fakeRuntimeDuplex {
	return &fakeRuntimeDuplex{fakeRuntimeClient: client, done: make(chan struct{})}
}

func (client *fakeRuntimeDuplex) Done() <-chan struct{} { return client.done }

func (client *fakeRuntimeDuplex) Err() error {
	value := client.err.Load()
	if value == nil {
		return nil
	}
	return value.(error)
}

func (client *fakeRuntimeDuplex) disconnect(err error) {
	if err != nil {
		client.err.Store(err)
	}
	client.once.Do(func() { close(client.done) })
}

type fakeRuntimeTransportDialer struct {
	mu          sync.Mutex
	connections []RuntimeDuplexClient
	allowProbe  atomic.Bool
}

func (dialer *fakeRuntimeTransportDialer) DialRuntimeWebSocket(
	_ context.Context,
	_ RuntimeHelloPayload,
) (RuntimeDuplexClient, error) {
	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	if len(dialer.connections) == 0 {
		return nil, errors.New("no WebSocket connection is available")
	}
	connection := dialer.connections[0]
	dialer.connections = dialer.connections[1:]
	return connection, nil
}

func (dialer *fakeRuntimeTransportDialer) ProbeRuntimeWebSocket(context.Context) error {
	if !dialer.allowProbe.Load() {
		return errors.New("WebSocket is still unavailable")
	}
	return nil
}

type runtimeClaimTracker struct {
	current atomic.Int64
	maximum atomic.Int64
}

func (tracker *runtimeClaimTracker) enter() func() {
	current := tracker.current.Add(1)
	for {
		maximum := tracker.maximum.Load()
		if current <= maximum || tracker.maximum.CompareAndSwap(maximum, current) {
			break
		}
	}
	return func() { tracker.current.Add(-1) }
}

func configureSwitchClient(
	client *fakeRuntimeClient,
	coreID string,
	tracker *runtimeClaimTracker,
	resumed chan<- struct{},
) {
	client.createFn = func(_ context.Context, request RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		ready := client.readyPayload()
		ready.CoreInstanceID = coreID
		return ready, nil
	}
	var claimOnce sync.Once
	client.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
		leave := tracker.enter()
		defer leave()
		var assignment *RuntimeRunAssignedPayload
		claimOnce.Do(func() {
			hello := client.helloSnapshot()
			assignment = assignedRunForHello(hello)
		})
		if assignment != nil {
			return assignment, nil
		}
		<-ctx.Done()
		return nil, ctx.Err()
	}
	client.ackFn = func(_ context.Context, request RuntimeAssignmentAckPayload) (*RuntimeAssignmentConfirmedPayload, error) {
		return confirmedAssignment(request.AttemptIdentity), nil
	}
	client.resumeFn = func(_ context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
		if resumed != nil {
			select {
			case resumed <- struct{}{}:
			default:
			}
		}
		decisions := make([]RuntimeResumeAcceptedPayload, len(request.Attempts))
		for index, attempt := range request.Attempts {
			expires := time.Now().Add(time.Minute).UTC()
			decisions[index] = RuntimeResumeAcceptedPayload{
				AttemptIdentity: attempt.AttemptIdentity,
				Decision:        RuntimeResumeContinue,
				LeaseExpiresAt:  &expires,
				AllowedActions: []RuntimeResumeAction{
					RuntimeActionContinueExecution,
					RuntimeActionUploadEvents,
					RuntimeActionUploadResult,
				},
			}
		}
		return &RuntimeResumeResponse{Decisions: decisions}, nil
	}
}

func confirmedAssignment(identity RuntimeAttemptIdentity) *RuntimeAssignmentConfirmedPayload {
	return &RuntimeAssignmentConfirmedPayload{
		AttemptIdentity: identity,
		AttemptNo:       1,
		LeaseExpiresAt:  time.Now().Add(time.Minute).UTC(),
	}
}

type blockingSwitchAdapter struct {
	count   atomic.Int64
	started chan struct{}
	release chan struct{}
	once    sync.Once
}

func newBlockingSwitchAdapter() *blockingSwitchAdapter {
	return &blockingSwitchAdapter{started: make(chan struct{}), release: make(chan struct{})}
}

func (adapter *blockingSwitchAdapter) Handle(ctx context.Context, _ RuntimeContext) (RuntimeResult, error) {
	adapter.count.Add(1)
	adapter.once.Do(func() { close(adapter.started) })
	select {
	case <-ctx.Done():
		return RuntimeResult{}, ctx.Err()
	case <-adapter.release:
		return RuntimeResult{Status: "success", Output: RuntimeJSONMap{"ok": true}}, nil
	}
}

func waitForRuntimeTransport(t *testing.T, node *RuntimeWorker, expected RuntimeTransportState) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		node.lifecycleMu.Lock()
		transport := node.transport
		node.lifecycleMu.Unlock()
		if transport == nil {
			time.Sleep(5 * time.Millisecond)
			continue
		}
		_, state, _ := transport.snapshot()
		if state == expected {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	node.lifecycleMu.Lock()
	transport := node.transport
	node.lifecycleMu.Unlock()
	if transport == nil {
		t.Fatalf("runtime transport was not initialized; want %s", expected)
	}
	_, state, _ := transport.snapshot()
	t.Fatalf("runtime transport state = %s, want %s", state, expected)
}

func runtimeSessionConflictTestError() error {
	return &Error{StatusCode: 409, Code: "RUNTIME_SESSION_CONFLICT", Message: "attachment is owned elsewhere"}
}

func stopRuntimeWorkerTest(t *testing.T, node *RuntimeWorker, runDone <-chan error) {
	t.Helper()
	stopCtx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	if err := node.Stop(stopCtx); err != nil {
		t.Fatal(err)
	}
	if err := <-runDone; err != nil {
		t.Fatal(err)
	}
}
