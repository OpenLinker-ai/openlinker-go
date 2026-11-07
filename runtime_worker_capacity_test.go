package openlinker

import (
	"context"
	"errors"
	"fmt"
	"runtime/debug"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// admissionToggleStore simulates spool backpressure: AcceptsNewRuns is the
// only behaviour that changes; every durable operation is the real store.
type admissionToggleStore struct {
	RuntimeStore
	accepting atomic.Bool
	// stall, when set, blocks AcceptsNewRuns for the capacity reattach path,
	// like a store mutex held across a slow fsync or a stalled Statfs.
	stall atomic.Pointer[storeStall]
}

type storeStall struct {
	entered chan struct{}
	release chan struct{}
	once    sync.Once
}

func (store *admissionToggleStore) AcceptsNewRuns() bool {
	if stall := store.stall.Load(); stall != nil && strings.Contains(string(debug.Stack()), "retireWebSocketForCapacity") {
		stall.once.Do(func() { close(stall.entered) })
		<-stall.release
	}
	return store.accepting.Load() && store.RuntimeStore.AcceptsNewRuns()
}

func (store *admissionToggleStore) SpoolStatus() (RuntimeSpoolStatus, error) {
	return runtimeStoreSpoolStatus(store.RuntimeStore)
}

type helloRecordingDialer struct {
	*fakeRuntimeTransportDialer
	mu     sync.Mutex
	hellos []RuntimeHelloPayload
}

func (dialer *helloRecordingDialer) DialRuntimeWebSocket(ctx context.Context, hello RuntimeHelloPayload) (RuntimeDuplexClient, error) {
	dialer.mu.Lock()
	dialer.hellos = append(dialer.hellos, hello)
	dialer.mu.Unlock()
	return dialer.fakeRuntimeTransportDialer.DialRuntimeWebSocket(ctx, hello)
}

func (dialer *helloRecordingDialer) dialedCapacities() []int64 {
	dialer.mu.Lock()
	defer dialer.mu.Unlock()
	capacities := make([]int64, len(dialer.hellos))
	for index, hello := range dialer.hellos {
		capacities[index] = hello.Capacity
	}
	return capacities
}

type capacityCloseLog struct {
	mu      sync.Mutex
	reasons []string
}

func (log *capacityCloseLog) add(reason string) {
	log.mu.Lock()
	log.reasons = append(log.reasons, reason)
	log.mu.Unlock()
}

func (log *capacityCloseLog) count(reason string) int {
	log.mu.Lock()
	defer log.mu.Unlock()
	count := 0
	for _, value := range log.reasons {
		if value == reason {
			count++
		}
	}
	return count
}

// newCapacityDuplex closes its Done channel when the Worker closes it, like
// RuntimeWebSocket.CloseRuntimeSession, and records every close reason.
func newCapacityDuplex(client *fakeRuntimeClient, closes *capacityCloseLog) *fakeRuntimeDuplex {
	duplex := newFakeRuntimeDuplex(client)
	client.closeFn = func(_ context.Context, request RuntimeSessionCloseRequest) error {
		closes.add(request.Reason)
		duplex.disconnect(errors.New("openlinker: runtime WebSocket closed"))
		return nil
	}
	return duplex
}

func continueResume(resumed chan<- struct{}) func(context.Context, RuntimeResumePayload) (*RuntimeResumeResponse, error) {
	return func(_ context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
		decisions := make([]RuntimeResumeAcceptedPayload, len(request.Attempts))
		for index, attempt := range request.Attempts {
			expires := time.Now().Add(time.Minute).UTC()
			decisions[index] = RuntimeResumeAcceptedPayload{
				AttemptIdentity: attempt.AttemptIdentity,
				Decision:        RuntimeResumeContinue,
				LeaseExpiresAt:  &expires,
				AllowedActions: []RuntimeResumeAction{
					RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult,
				},
			}
		}
		if resumed != nil {
			select {
			case resumed <- struct{}{}:
			default:
			}
		}
		return &RuntimeResumeResponse{Decisions: decisions}, nil
	}
}

const capacityPullClient = -1

// capacityCore models Core's idempotent Event/Result acceptance across every
// transport: an effect is applied once per stable ClientEventID / ResultID and
// a re-send with the same ID is acknowledged without a second effect. It can
// lose the ACK after applying the effect on one transport.
type capacityCore struct {
	mu sync.Mutex
	// eventIDs maps each client sequence to the stable ID first seen for it.
	eventIDs     map[int64]string
	eventEffects map[string]int
	eventSends   map[string][]int
	resultIDs    []string
	resultSends  map[string][]int
	conflicts    []string
	// loseEventACK / loseResultACK name the client index whose ACKs are lost
	// after the effect is applied (simulating a reply lost on the wire).
	loseEventACK  map[int]int64
	loseResultACK map[int]bool
}

func newCapacityCore() *capacityCore {
	return &capacityCore{
		eventIDs: map[int64]string{}, eventEffects: map[string]int{}, eventSends: map[string][]int{},
		resultSends: map[string][]int{}, loseEventACK: map[int]int64{}, loseResultACK: map[int]bool{},
	}
}

func (core *capacityCore) attach(client *fakeRuntimeClient, index int) {
	client.eventFn = func(_ context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
		core.mu.Lock()
		defer core.mu.Unlock()
		if known, ok := core.eventIDs[request.ClientEventSeq]; ok && known != request.ClientEventID {
			core.conflicts = append(core.conflicts, fmt.Sprintf("event seq %d re-sent as %s, first %s", request.ClientEventSeq, request.ClientEventID, known))
		} else if !ok {
			core.eventIDs[request.ClientEventSeq] = request.ClientEventID
			core.eventEffects[request.ClientEventID]++
		}
		core.eventSends[request.ClientEventID] = append(core.eventSends[request.ClientEventID], index)
		if seq, lose := core.loseEventACK[index]; lose && seq == request.ClientEventSeq {
			return nil, errors.New("event accepted but its ACK was lost")
		}
		return &RuntimeRunEventAckPayload{ClientEventID: request.ClientEventID, ClientEventSeq: request.ClientEventSeq, Sequence: request.ClientEventSeq}, nil
	}
	client.resultFn = func(_ context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
		core.mu.Lock()
		defer core.mu.Unlock()
		if _, seen := core.resultSends[request.ResultID]; !seen {
			core.resultIDs = append(core.resultIDs, request.ResultID)
		}
		core.resultSends[request.ResultID] = append(core.resultSends[request.ResultID], index)
		if core.loseResultACK[index] {
			return nil, errors.New("result accepted but its ACK was lost")
		}
		return successfulResultACK(request.ResultID), nil
	}
}

func (core *capacityCore) eventSendsFor(seq int64) []int {
	core.mu.Lock()
	defer core.mu.Unlock()
	return append([]int(nil), core.eventSends[core.eventIDs[seq]]...)
}

func (core *capacityCore) resultSendsFor() []int {
	core.mu.Lock()
	defer core.mu.Unlock()
	if len(core.resultIDs) == 0 {
		return nil
	}
	return append([]int(nil), core.resultSends[core.resultIDs[0]]...)
}

// assertExactlyOnce rejects duplicate effects: exactly the expected Event
// sequences each with one stable ID and one applied effect, and exactly one
// Result ID. Transport-level re-sends are allowed only with the same ID.
func (core *capacityCore) assertExactlyOnce(t *testing.T, sequences ...int64) {
	t.Helper()
	core.mu.Lock()
	defer core.mu.Unlock()
	if len(core.conflicts) != 0 {
		t.Fatalf("stable IDs changed on re-send: %v", core.conflicts)
	}
	if len(core.eventIDs) != len(sequences) || len(core.eventEffects) != len(sequences) {
		t.Fatalf("event sequences = %v effects = %v, want exactly %v", core.eventIDs, core.eventEffects, sequences)
	}
	for _, seq := range sequences {
		id, ok := core.eventIDs[seq]
		if !ok || core.eventEffects[id] != 1 {
			t.Fatalf("event seq %d effects = %d (id %q)", seq, core.eventEffects[id], id)
		}
	}
	if len(core.resultIDs) != 1 {
		t.Fatalf("result IDs = %v, want exactly one", core.resultIDs)
	}
}

type capacityHandler struct {
	count    atomic.Int32
	canceled atomic.Bool
	started  chan struct{}
	release  chan struct{}
	once     sync.Once
}

func (handler *capacityHandler) Handle(ctx context.Context, assignment RuntimeContext) (RuntimeResult, error) {
	handler.count.Add(1)
	if err := assignment.Emit("run.progress", RuntimeJSONMap{"step": "before"}); err != nil {
		return RuntimeResult{}, err
	}
	handler.once.Do(func() { close(handler.started) })
	select {
	case <-ctx.Done():
		handler.canceled.Store(true)
		return RuntimeResult{}, ctx.Err()
	case <-handler.release:
	}
	if err := assignment.Emit("run.progress", RuntimeJSONMap{"step": "after"}); err != nil {
		return RuntimeResult{}, err
	}
	return RuntimeResult{Status: "success", Output: RuntimeJSONMap{"ok": true}}, nil
}

type capacityHarness struct {
	node     *RuntimeWorker
	store    *admissionToggleStore
	dialer   *helloRecordingDialer
	handler  *capacityHandler
	closes   *capacityCloseLog
	core     *capacityCore
	clients  []*fakeRuntimeClient
	duplexes []*fakeRuntimeDuplex
	resumed  []chan struct{}
	runDone  chan error

	pull           *fakeRuntimeClient
	pullMu         sync.Mutex
	pullCapacities []int64
}

func (harness *capacityHarness) pullSessionCapacities() []int64 {
	harness.pullMu.Lock()
	defer harness.pullMu.Unlock()
	return append([]int64(nil), harness.pullCapacities...)
}

// newCapacityHarness builds a WebSocket-capable Worker whose first socket
// offers one Attempt. Later sockets and HTTP pull offer nothing and resume it.
// configure runs before Start so fake behaviour is fixed without data races.
func newCapacityHarness(t *testing.T, sockets int, transport RuntimeTransportMode, configure func(*capacityHarness)) *capacityHarness {
	t.Helper()
	harness := &capacityHarness{
		handler: &capacityHandler{started: make(chan struct{}), release: make(chan struct{})},
		closes:  &capacityCloseLog{},
		core:    newCapacityCore(),
		runDone: make(chan error, 1),
	}
	connections := make([]RuntimeDuplexClient, 0, sockets)
	for index := 0; index < sockets; index++ {
		client := newFakeRuntimeClient()
		resumed := make(chan struct{}, 1)
		client.resumeFn = continueResume(resumed)
		harness.core.attach(client, index)
		if index == 0 {
			var offered atomic.Bool
			client.claimFn = func(ctx context.Context, _ int, _ RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
				if offered.CompareAndSwap(false, true) {
					return assignedRunForHello(client.helloSnapshot()), nil
				}
				<-ctx.Done()
				return nil, ctx.Err()
			}
		}
		harness.clients = append(harness.clients, client)
		harness.resumed = append(harness.resumed, resumed)
		duplex := newCapacityDuplex(client, harness.closes)
		harness.duplexes = append(harness.duplexes, duplex)
		connections = append(connections, duplex)
	}
	harness.dialer = &helloRecordingDialer{fakeRuntimeTransportDialer: &fakeRuntimeTransportDialer{connections: connections}}

	dataDir := t.TempDir()
	fileStore, err := OpenFileRuntimeStore(dataDir)
	if err != nil {
		t.Fatal(err)
	}
	harness.store = &admissionToggleStore{RuntimeStore: fileStore}
	harness.store.accepting.Store(true)

	pull := newFakeRuntimeClient()
	pull.resumeFn = continueResume(nil)
	harness.core.attach(pull, capacityPullClient)
	pull.createFn = func(_ context.Context, hello RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		harness.pullMu.Lock()
		harness.pullCapacities = append(harness.pullCapacities, hello.Capacity)
		harness.pullMu.Unlock()
		return pull.readyPayload(), nil
	}
	harness.pull = pull

	node := newRuntimeWorkerForTest(dataDir, pull, harness.handler)
	node.Store = harness.store
	node.Capacity = 2
	node.Transport = transport
	node.runtimeDialer = harness.dialer
	node.HeartbeatInterval = 10 * time.Millisecond
	harness.node = node
	if configure != nil {
		configure(harness)
	}
	go func() { harness.runDone <- node.Start(context.Background()) }()
	return harness
}

func (harness *capacityHarness) dialCount() int {
	return len(harness.dialer.dialedCapacities())
}

// attachAtZeroCapacity reproduces the stuck state in explicit WebSocket mode:
// an Attempt is running, the spool crosses its admission threshold, and an
// ordinary disconnect makes the replacement WebSocket attach with capacity 0.
func (harness *capacityHarness) attachAtZeroCapacity(t *testing.T) {
	t.Helper()
	waitForSignal(t, "handler start", harness.handler.started)
	harness.store.accepting.Store(false)
	harness.duplexes[0].disconnect(errors.New("network drop"))
	waitForSignal(t, "durable resume on the zero-capacity WebSocket", harness.resumed[1])
	waitForRuntimeTransport(t, harness.node, RuntimeTransportWebSocketActive)
	if capacities := harness.dialer.dialedCapacities(); len(capacities) != 2 || capacities[0] != 2 || capacities[1] != 0 {
		t.Fatalf("dialed capacities = %v, want [2 0]", capacities)
	}
}

func (harness *capacityHarness) waitForEmptySpool(t *testing.T) {
	t.Helper()
	waitForCapacityCondition(t, "empty durable spool", func() bool {
		status, err := runtimeStoreSpoolStatus(harness.store)
		return err == nil && status.Empty
	})
}

func waitForCapacityCondition(t *testing.T, description string, condition func() bool) {
	t.Helper()
	deadline := time.Now().Add(5 * time.Second)
	for time.Now().Before(deadline) {
		if condition() {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("timed out waiting for %s", description)
}

func waitForSignal(t *testing.T, description string, signal <-chan struct{}) {
	t.Helper()
	select {
	case <-signal:
	case <-time.After(5 * time.Second):
		t.Fatalf("timed out waiting for %s", description)
	}
}

func containsClient(clients []int, index int) bool {
	for _, value := range clients {
		if value == index {
			return true
		}
	}
	return false
}

func TestRuntimeWebSocketCapacityReattachKeepsInFlightAttemptAndDelivery(t *testing.T) {
	harness := newCapacityHarness(t, 3, RuntimeTransportWebSocket, nil)
	harness.attachAtZeroCapacity(t)

	// Backpressure persists: the zero-capacity attach is accurate, no reattach.
	time.Sleep(20 * harness.node.HeartbeatInterval)
	if harness.dialCount() != 2 || harness.closes.count("capacity_restored") != 0 {
		t.Fatalf("reattached while capacity was still 0: dials=%d", harness.dialCount())
	}

	// The spool drains. The existing reconnect path must carry capacity 2.
	harness.store.accepting.Store(true)
	waitForSignal(t, "durable resume on the reattached WebSocket", harness.resumed[2])
	waitForRuntimeTransport(t, harness.node, RuntimeTransportWebSocketActive)
	if capacities := harness.dialer.dialedCapacities(); len(capacities) != 3 || capacities[2] != 2 {
		t.Fatalf("dialed capacities = %v, want [2 0 2]", capacities)
	}
	if harness.closes.count("capacity_restored") != 1 {
		t.Fatalf("capacity reattach closes = %d", harness.closes.count("capacity_restored"))
	}

	// The confirmed Attempt survived both transport changes: one execution, no
	// cancellation, and its Events/Result are applied exactly once.
	if harness.handler.count.Load() != 1 || harness.handler.canceled.Load() {
		t.Fatalf("executions=%d canceled=%v", harness.handler.count.Load(), harness.handler.canceled.Load())
	}
	close(harness.handler.release)
	harness.waitForEmptySpool(t)
	harness.core.assertExactlyOnce(t, 1, 2)
	if sends := harness.core.resultSendsFor(); len(sends) != 1 || sends[0] != 2 {
		t.Fatalf("result sends by transport = %v, want one on the reattached socket", sends)
	}
	if harness.dialCount() != 3 {
		t.Fatalf("unexpected extra reattach: dials=%d", harness.dialCount())
	}
	stopRuntimeWorkerTest(t, harness.node, harness.runDone)
	if harness.handler.count.Load() != 1 {
		t.Fatalf("final executions = %d", harness.handler.count.Load())
	}
}

// Core applies an Event or Result on the zero-capacity socket but the ACK is
// lost. After the capacity reattach the same stable ID is re-sent on the new
// socket, Core keeps exactly one effect, and the spool ends empty.
func TestRuntimeWebSocketCapacityReattachResendsStableIDAfterLostACK(t *testing.T) {
	for _, testCase := range []struct {
		name      string
		configure func(*capacityHarness)
		applied   func(*capacityHarness) []int
	}{
		{
			name:      "event",
			configure: func(harness *capacityHarness) { harness.core.loseEventACK[1] = 2 },
			applied:   func(harness *capacityHarness) []int { return harness.core.eventSendsFor(2) },
		},
		{
			name:      "result",
			configure: func(harness *capacityHarness) { harness.core.loseResultACK[1] = true },
			applied:   func(harness *capacityHarness) []int { return harness.core.resultSendsFor() },
		},
	} {
		t.Run(testCase.name, func(t *testing.T) {
			harness := newCapacityHarness(t, 3, RuntimeTransportWebSocket, testCase.configure)
			harness.attachAtZeroCapacity(t)
			// Finish the handler while the zero-capacity socket is attached: Core
			// accepts the record there, but the reply never arrives.
			close(harness.handler.release)
			waitForCapacityCondition(t, "record applied on the zero-capacity socket", func() bool {
				return containsClient(testCase.applied(harness), 1)
			})
			if status, err := runtimeStoreSpoolStatus(harness.store); err != nil || status.Empty {
				t.Fatalf("spool must still hold the unacknowledged record: %+v %v", status, err)
			}

			harness.store.accepting.Store(true)
			waitForSignal(t, "durable resume on the reattached WebSocket", harness.resumed[2])
			harness.waitForEmptySpool(t)
			if sends := testCase.applied(harness); !containsClient(sends, 2) {
				t.Fatalf("record was not re-sent on the reattached socket: sends by transport %v", sends)
			}
			harness.core.assertExactlyOnce(t, 1, 2)
			if harness.closes.count("capacity_restored") != 1 || harness.handler.count.Load() != 1 || harness.handler.canceled.Load() {
				t.Fatalf("reattaches=%d executions=%d canceled=%v",
					harness.closes.count("capacity_restored"), harness.handler.count.Load(), harness.handler.canceled.Load())
			}
			stopRuntimeWorkerTest(t, harness.node, harness.runDone)
		})
	}
}

func TestRuntimeWebSocketCapacityReattachUsesAutoPullFallback(t *testing.T) {
	harness := newCapacityHarness(t, 2, RuntimeTransportAuto, nil)
	waitForSignal(t, "handler start", harness.handler.started)
	harness.store.accepting.Store(false)
	harness.dialer.allowProbe.Store(true)
	harness.duplexes[0].disconnect(errors.New("network drop"))
	// auto: WS drop -> HTTP pull -> WebSocket recovery probe attaches at 0.
	waitForSignal(t, "durable resume on the zero-capacity WebSocket", harness.resumed[1])
	waitForRuntimeTransport(t, harness.node, RuntimeTransportWebSocketActive)
	if capacities := harness.dialer.dialedCapacities(); len(capacities) != 2 || capacities[1] != 0 {
		t.Fatalf("dialed capacities = %v, want [2 0]", capacities)
	}
	harness.dialer.allowProbe.Store(false)
	pullAttaches := len(harness.pullSessionCapacities())

	// The capacity reattach takes the existing auto fallback: HTTP pull attaches
	// with the restored capacity and keeps the running Attempt.
	harness.store.accepting.Store(true)
	waitForCapacityCondition(t, "pull attach with restored capacity", func() bool {
		capacities := harness.pullSessionCapacities()
		return len(capacities) > pullAttaches && capacities[len(capacities)-1] == 2
	})
	waitForRuntimeTransport(t, harness.node, RuntimeTransportPullActive)
	if harness.closes.count("capacity_restored") != 1 {
		t.Fatalf("capacity reattach closes = %d", harness.closes.count("capacity_restored"))
	}
	if harness.handler.count.Load() != 1 || harness.handler.canceled.Load() {
		t.Fatalf("executions=%d canceled=%v", harness.handler.count.Load(), harness.handler.canceled.Load())
	}
	close(harness.handler.release)
	harness.waitForEmptySpool(t)
	harness.core.assertExactlyOnce(t, 1, 2)
	stopRuntimeWorkerTest(t, harness.node, harness.runDone)
	if harness.handler.count.Load() != 1 {
		t.Fatalf("final executions = %d", harness.handler.count.Load())
	}
}

func TestRuntimeWebSocketCapacityReattachNeverInterruptsDrain(t *testing.T) {
	harness := newCapacityHarness(t, 3, RuntimeTransportWebSocket, func(harness *capacityHarness) {
		// Like Core, report the Attempt as inflight until its Result is applied.
		harness.clients[1].drainFn = func(_ context.Context, _ string, request RuntimeDrainPayload) (*RuntimeDrainPayload, error) {
			response := request
			response.Capacity, response.Inflight = 0, 1
			if len(harness.core.resultSendsFor()) > 0 {
				response.Inflight = 0
			}
			return &response, nil
		}
	})
	harness.attachAtZeroCapacity(t)

	drainDone := make(chan error, 1)
	go func() {
		drainDone <- harness.node.Drain(context.Background(), RuntimeWorkerDrainOptions{Timeout: 10 * time.Second})
	}()
	waitForCapacityCondition(t, "drain request on the attached WebSocket", func() bool {
		harness.clients[1].mu.Lock()
		defer harness.clients[1].mu.Unlock()
		return len(harness.clients[1].drains) > 0
	})
	// Admission reopens locally, but draining keeps capacity at 0, so the
	// attached socket is still accurate and must not be replaced.
	harness.store.accepting.Store(true)
	time.Sleep(20 * harness.node.HeartbeatInterval)
	if harness.dialCount() != 2 || harness.closes.count("capacity_restored") != 0 {
		t.Fatalf("drain was interrupted by a capacity reattach: dials=%d", harness.dialCount())
	}
	close(harness.handler.release)
	select {
	case err := <-drainDone:
		if err != nil {
			t.Fatalf("drain = %v", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("drain did not finish")
	}
	if err := <-harness.runDone; err != nil {
		t.Fatal(err)
	}
	if harness.dialCount() != 2 || harness.handler.count.Load() != 1 || harness.handler.canceled.Load() {
		t.Fatalf("dials=%d executions=%d canceled=%v", harness.dialCount(), harness.handler.count.Load(), harness.handler.canceled.Load())
	}
	harness.core.assertExactlyOnce(t, 1, 2)
}

// capacityDecisionWorker is a minimal Worker with an attached fake WebSocket
// for exercising the reattach decision without running loops.
func capacityDecisionWorker(t *testing.T) (*RuntimeWorker, *admissionToggleStore, *fakeRuntimeDuplex) {
	t.Helper()
	store := &admissionToggleStore{RuntimeStore: openRuntimeStoreForTest(t, t.TempDir())}
	node := &RuntimeWorker{NodeID: testNodeID, AgentID: testAgentID, NodeVersion: "test", Capacity: 2}
	node.store = store
	duplex := newFakeRuntimeDuplex(newFakeRuntimeClient())
	node.transport = newSwitchingRuntimeClient(nil)
	epoch, _, _ := node.transport.beginTransition(RuntimeTransportConnectingWS)
	if !node.transport.activateIfCurrent(epoch, RuntimeTransportWebSocket, duplex) {
		t.Fatal("activate fake WebSocket")
	}
	return node, store, duplex
}

func capacityTarget(node *RuntimeWorker, now time.Time) RuntimeDuplexClient {
	return node.webSocketCapacityReattachTarget(now, node.runtimeHello())
}

func TestRuntimeWebSocketCapacityReattachDecisionHasHysteresisAndRateLimit(t *testing.T) {
	node, store, duplex := capacityDecisionWorker(t)
	start := time.Date(2026, 11, 6, 0, 0, 0, 0, time.UTC)

	// Attached while backpressured.
	store.accepting.Store(false)
	node.recordWebSocketAttachment(duplex, node.runtimeHello())
	if capacityTarget(node, start) != nil {
		t.Fatal("reattached while capacity is still 0")
	}

	store.accepting.Store(true)
	if capacityTarget(node, start) != nil {
		t.Fatal("reattached on the first restored observation")
	}
	store.accepting.Store(false)
	if capacityTarget(node, start.Add(time.Second)) != nil {
		t.Fatal("reattached while capacity flapped back to 0")
	}
	store.accepting.Store(true)
	if capacityTarget(node, start.Add(2*time.Second)) != nil {
		t.Fatal("a flap did not reset the consecutive-observation requirement")
	}
	if capacityTarget(node, start.Add(3*time.Second)) != duplex {
		t.Fatal("restored capacity did not select the attached socket")
	}

	// The same stale attach is not reattached again inside the minimum interval.
	for offset := 4 * time.Second; offset < runtimeCapacityReattachMinInterval; offset += time.Second {
		if capacityTarget(node, start.Add(offset)) != nil {
			t.Fatalf("second reattach after %s", offset)
		}
	}
	if capacityTarget(node, start.Add(runtimeCapacityReattachMinInterval+3*time.Second)) != duplex {
		t.Fatal("rate limit never expired")
	}

	// A socket attached with real capacity is never replaced for backpressure.
	node.recordWebSocketAttachment(duplex, node.runtimeHello())
	store.accepting.Store(false)
	for offset := time.Duration(0); offset < 3*runtimeCapacityReattachMinInterval; offset += time.Second {
		if capacityTarget(node, start.Add(time.Hour+offset)) != nil {
			t.Fatal("reattached when capacity dropped to 0")
		}
	}

	// Draining or stopping keeps capacity at 0, so it never reattaches.
	store.accepting.Store(false)
	node.recordWebSocketAttachment(duplex, node.runtimeHello())
	store.accepting.Store(true)
	node.setDraining(true)
	for offset := time.Duration(0); offset < 3*runtimeCapacityReattachMinInterval; offset += time.Second {
		if capacityTarget(node, start.Add(2*time.Hour+offset)) != nil {
			t.Fatal("reattached while draining")
		}
		if node.reattachWebSocketIfCapacityRestored(start.Add(3*time.Hour + offset)) {
			t.Fatal("reattached while draining")
		}
	}
}

func TestRuntimeWebSocketCapacityReattachIgnoresOtherIdentityAndConnections(t *testing.T) {
	node, store, active := capacityDecisionWorker(t)
	other := newFakeRuntimeDuplex(newFakeRuntimeClient())
	start := time.Now()
	check := func(description string) {
		t.Helper()
		for index := 0; index < 4; index++ {
			if capacityTarget(node, start.Add(time.Duration(index)*time.Second)) != nil {
				t.Fatalf("reattached for %s", description)
			}
		}
	}

	store.accepting.Store(false)
	node.recordWebSocketAttachment(other, node.runtimeHello())
	store.accepting.Store(true)
	check("a connection that is not active")

	store.accepting.Store(false)
	hello := node.runtimeHello()
	hello.SessionEpoch++
	node.recordWebSocketAttachment(active, hello)
	store.accepting.Store(true)
	check("a different Session identity")

	store.accepting.Store(false)
	node.recordWebSocketAttachment(active, node.runtimeHello())
	store.accepting.Store(true)
	node.transport.setState(RuntimeTransportSwitchingPull)
	check("a transport that is switching")
}

// The decision and the logical close (retiring the socket) are atomic with a
// Drain/Stop admission fence, while the network close runs unlocked: the order
// is always decided -> fenced -> network close, the fence never waits for the
// network close, and every operation after the fence sees the socket closed.
func TestRuntimeWebSocketCapacityReattachIsAtomicWithDrainAndStop(t *testing.T) {
	for _, transition := range []struct {
		name  string
		fence func(*RuntimeWorker)
	}{
		{name: "drain", fence: func(node *RuntimeWorker) { node.setDraining(true) }},
		{name: "core drain command", fence: func(node *RuntimeWorker) { node.markDraining() }},
		{name: "stop", fence: func(node *RuntimeWorker) { node.requestStop() }},
	} {
		t.Run(transition.name, func(t *testing.T) {
			node, store, duplex := capacityDecisionWorker(t)
			var orderMu sync.Mutex
			var order []string
			record := func(event string) {
				orderMu.Lock()
				order = append(order, event)
				orderMu.Unlock()
			}
			fenced := make(chan struct{})
			var hello RuntimeHelloPayload // computed before the reattach; no lock in the close
			node.capacityReattachRetired = func() {
				record("decided")
				// stateMu is held shared: no fence can run between the decision
				// and the retirement of the socket.
				if node.stateMu.TryLock() {
					node.stateMu.Unlock()
					t.Error("an admission fence could start between decision and retirement")
				}
				go func() {
					transition.fence(node)
					record("fenced")
					close(fenced)
				}()
			}
			duplex.closeFn = func(_ context.Context, _ RuntimeSessionCloseRequest) error {
				// Like a stalled writer, the network close is still pending: the
				// fence must complete without waiting for it.
				select {
				case <-fenced:
				case <-time.After(5 * time.Second):
					t.Error("the fence waited for the network close")
				}
				// After the fence, the retired socket is refused like a closed one,
				// so a Drain request can never go out on it.
				if _, err := node.transport.HeartbeatRuntimeSession(context.Background(), hello); !errors.Is(err, ErrRuntimeTransportSwitching) {
					t.Errorf("operation after the fence used the retired socket: %v", err)
				}
				record("network closed")
				duplex.disconnect(nil)
				return nil
			}
			store.accepting.Store(false)
			node.recordWebSocketAttachment(duplex, node.runtimeHello())
			store.accepting.Store(true)
			hello = node.runtimeHello()
			start := time.Now()
			if node.reattachWebSocketIfCapacityRestored(start) {
				t.Fatal("reattached on the first observation")
			}
			if !node.reattachWebSocketIfCapacityRestored(start.Add(time.Second)) {
				t.Fatal("restored capacity did not reattach")
			}
			waitForSignal(t, "admission fence", fenced)
			orderMu.Lock()
			got := strings.Join(order, ",")
			orderMu.Unlock()
			if got != "decided,fenced,network closed" {
				t.Fatalf("transition order = %s, want decided,fenced,network closed", got)
			}
			if capacity, _ := node.capacitySnapshot(); capacity != 0 {
				t.Fatalf("capacity after fence = %d", capacity)
			}
			// Once fenced, a later check never closes another socket.
			if node.reattachWebSocketIfCapacityRestored(start.Add(time.Hour)) {
				t.Fatal("reattached after the fence")
			}
		})
	}
}

// A close that does not disconnect the client must not leave the transport
// refusing every operation.
func TestRuntimeWebSocketCapacityReattachRestoresSocketWhenCloseDoesNotDisconnect(t *testing.T) {
	node, store, duplex := capacityDecisionWorker(t)
	duplex.closeFn = func(context.Context, RuntimeSessionCloseRequest) error { return errors.New("close identity mismatch") }
	store.accepting.Store(false)
	node.recordWebSocketAttachment(duplex, node.runtimeHello())
	store.accepting.Store(true)
	start := time.Now()
	node.reattachWebSocketIfCapacityRestored(start)
	if !node.reattachWebSocketIfCapacityRestored(start.Add(time.Second)) {
		t.Fatal("restored capacity did not attempt a reattach")
	}
	if _, err := node.transport.HeartbeatRuntimeSession(context.Background(), node.runtimeHello()); err != nil {
		t.Fatalf("socket stayed retired after a close that did not disconnect it: %v", err)
	}
}

// A capacity close stuck on a stalled writer must not delay Drain or Stop:
// both return within their own deadlines.
func TestRuntimeWebSocketCapacityReattachBlockedCloseDoesNotDelayStopOrDrain(t *testing.T) {
	for _, operation := range []string{"drain", "stop"} {
		t.Run(operation, func(t *testing.T) {
			closeEntered := make(chan struct{})
			releaseClose := make(chan struct{})
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(releaseClose) }) }
			t.Cleanup(release)
			harness := newCapacityHarness(t, 3, RuntimeTransportWebSocket, func(harness *capacityHarness) {
				duplex := harness.duplexes[1]
				harness.clients[1].closeFn = func(_ context.Context, request RuntimeSessionCloseRequest) error {
					harness.closes.add(request.Reason)
					if request.Reason == "capacity_restored" {
						close(closeEntered)
						<-releaseClose // stalled writer
					}
					duplex.disconnect(errors.New("openlinker: runtime WebSocket closed"))
					return nil
				}
			})
			t.Cleanup(func() {
				release()
				select {
				case <-harness.handler.release:
				default:
					close(harness.handler.release)
				}
			})
			harness.attachAtZeroCapacity(t)
			harness.store.accepting.Store(true)
			waitForSignal(t, "capacity close blocked on a stalled writer", closeEntered)
			// An implementation that holds a Worker lock across the close would
			// hang here; the watchdog turns that into a measured overrun.
			watchdog := time.AfterFunc(3*time.Second, release)
			defer watchdog.Stop()

			deadline := 300 * time.Millisecond
			started := time.Now()
			if operation == "drain" {
				err := harness.node.Drain(context.Background(), RuntimeWorkerDrainOptions{Timeout: deadline})
				var timeoutErr *RuntimeDrainTimeoutError
				if !errors.As(err, &timeoutErr) {
					t.Fatalf("drain with a stalled capacity close = %v, want a drain timeout", err)
				}
				harness.clients[1].mu.Lock()
				drains := len(harness.clients[1].drains)
				harness.clients[1].mu.Unlock()
				if drains != 0 {
					t.Fatal("drain request used the socket that was being closed")
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), deadline)
				defer cancel()
				if err := harness.node.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("stop with a stalled capacity close = %v, want its deadline", err)
				}
			}
			if elapsed := time.Since(started); elapsed > deadline+time.Second {
				t.Fatalf("%s took %s with deadline %s: the stalled close delayed it", operation, elapsed, deadline)
			}

			release()
			close(harness.handler.release)
			select {
			case <-harness.runDone:
			case <-time.After(15 * time.Second):
				t.Fatal("Worker did not finish after the stalled close was released")
			}
		})
	}
}

// Store I/O on the capacity reattach path (admission snapshot under the store
// mutex, Statfs) must not hold the admission-fence lock: with that I/O stalled,
// Stop and Drain still return within their deadlines, and the reattach then
// observes the fence and retires nothing.
func TestRuntimeWebSocketCapacityReattachStalledStoreDoesNotDelayStopOrDrain(t *testing.T) {
	for _, operation := range []string{"drain", "stop"} {
		t.Run(operation, func(t *testing.T) {
			harness := newCapacityHarness(t, 3, RuntimeTransportWebSocket, func(harness *capacityHarness) {
				// Only this test drives the reattach decision.
				harness.node.HeartbeatInterval = time.Hour
			})
			stall := &storeStall{entered: make(chan struct{}), release: make(chan struct{})}
			var releaseOnce sync.Once
			release := func() { releaseOnce.Do(func() { close(stall.release) }) }
			t.Cleanup(func() {
				release()
				select {
				case <-harness.handler.release:
				default:
					close(harness.handler.release)
				}
			})
			harness.attachAtZeroCapacity(t)
			harness.store.accepting.Store(true)
			start := time.Now()
			if harness.node.reattachWebSocketIfCapacityRestored(start) {
				t.Fatal("reattached on the first observation")
			}
			harness.store.stall.Store(stall)
			reattached := make(chan bool, 1)
			go func() { reattached <- harness.node.reattachWebSocketIfCapacityRestored(start.Add(time.Second)) }()
			waitForSignal(t, "reattach blocked in store I/O", stall.entered)
			// An implementation that holds stateMu across store I/O would hang
			// the fence; the watchdog turns that into a measured overrun.
			watchdog := time.AfterFunc(3*time.Second, release)
			defer watchdog.Stop()

			deadline := 300 * time.Millisecond
			started := time.Now()
			if operation == "drain" {
				err := harness.node.Drain(context.Background(), RuntimeWorkerDrainOptions{Timeout: deadline})
				var timeoutErr *RuntimeDrainTimeoutError
				if !errors.As(err, &timeoutErr) {
					t.Fatalf("drain with stalled store I/O = %v, want a drain timeout", err)
				}
			} else {
				ctx, cancel := context.WithTimeout(context.Background(), deadline)
				defer cancel()
				if err := harness.node.Stop(ctx); !errors.Is(err, context.DeadlineExceeded) {
					t.Fatalf("stop with stalled store I/O = %v, want its deadline", err)
				}
			}
			if elapsed := time.Since(started); elapsed > deadline+time.Second {
				t.Fatalf("%s took %s with deadline %s: stalled store I/O delayed the fence", operation, elapsed, deadline)
			}

			release()
			select {
			case retired := <-reattached:
				if retired {
					t.Fatal("reattach retired a socket after the fence")
				}
			case <-time.After(5 * time.Second):
				t.Fatal("reattach did not finish after the store recovered")
			}
			if harness.closes.count("capacity_restored") != 0 {
				t.Fatal("a fenced Worker closed its socket for capacity")
			}
			close(harness.handler.release)
			select {
			case <-harness.runDone:
			case <-time.After(15 * time.Second):
				t.Fatal("Worker did not finish")
			}
		})
	}
}

func TestRuntimeWebSocketCapacityMismatchHasAccurateError(t *testing.T) {
	attached := runtimeTestHello()
	attached.Capacity = 0
	socket := &RuntimeWebSocket{hello: attached, done: make(chan struct{})}
	current := runtimeTestHello()
	current.Capacity = 3
	_, err := socket.HeartbeatRuntimeSession(context.Background(), current)
	var capacityErr *runtimeWebSocketCapacityChangedError
	if !errors.As(err, &capacityErr) || capacityErr.Attached != 0 || capacityErr.Current != 3 {
		t.Fatalf("capacity-only mismatch = %v", err)
	}
	if strings.Contains(err.Error(), "another session") || !strings.Contains(err.Error(), "capacity 0") {
		t.Fatalf("misleading capacity error: %v", err)
	}
	if runtimeErrorIsPermanent(err) || runtimePolicyRecoverySignal(err) {
		t.Fatal("capacity mismatch changed retry classification")
	}
	other := runtimeTestHello()
	other.SessionEpoch = 2
	if _, err = socket.CreateRuntimeSession(context.Background(), other); err == nil ||
		err.Error() != "openlinker: runtime WebSocket is attached to another session" || errors.As(err, &capacityErr) {
		t.Fatalf("identity mismatch = %v", err)
	}
	if _, err = socket.CreateRuntimeSession(context.Background(), attached); err != nil {
		t.Fatalf("matching hello = %v", err)
	}
}
