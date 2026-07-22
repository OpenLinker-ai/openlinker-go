package openlinker

import (
	"context"
	"encoding/json"
	"errors"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/gorilla/websocket"
)

func TestRuntimePolicyRecoveryConcurrentFailureIsSingleFlightAndTerminal(t *testing.T) {
	oldClient := newFakeRuntimeClient()
	var initialCalls atomic.Int32
	releaseInitial := make(chan struct{})
	oldClient.heartbeatFn = func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		if initialCalls.Add(1) == 2 {
			close(releaseInitial)
		}
		<-releaseInitial
		return nil, runtimePolicyChangedTestError()
	}
	gate := activeRuntimePolicyTestGate(t, oldClient)
	var discoveryCalls atomic.Int32
	node := &RuntimeWorker{
		PlatformURL: "https://platform.example.test",
		transport:   gate,
		runtimeDiscovery: func(context.Context, string) (runtimeConnectionInformation, error) {
			discoveryCalls.Add(1)
			return runtimeConnectionInformation{}, errors.New("manifest unavailable")
		},
	}
	client := &policyRecoveringRuntimeClient{node: node, transport: gate}
	errorsByCaller := make([]error, 2)
	var callers sync.WaitGroup
	for index := range errorsByCaller {
		callers.Add(1)
		go func(index int) {
			defer callers.Done()
			_, errorsByCaller[index] = client.HeartbeatRuntimeSession(context.Background(), RuntimeHelloPayload{})
		}(index)
	}
	callers.Wait()
	if discoveryCalls.Load() != 1 || initialCalls.Load() != 2 {
		t.Fatalf("calls = discovery %d operation %d, want 1/2", discoveryCalls.Load(), initialCalls.Load())
	}
	if errorsByCaller[0] == nil || errorsByCaller[0] != errorsByCaller[1] {
		t.Fatalf("concurrent recovery failures were not shared: %#v", errorsByCaller)
	}
	var recoveryErr *runtimePolicyRecoveryError
	if !errors.As(errorsByCaller[0], &recoveryErr) {
		t.Fatalf("failure = %T %v", errorsByCaller[0], errorsByCaller[0])
	}
	_, thirdErr := client.HeartbeatRuntimeSession(context.Background(), RuntimeHelloPayload{})
	if thirdErr != errorsByCaller[0] || discoveryCalls.Load() != 1 || initialCalls.Load() != 2 {
		t.Fatalf("terminal failure was retried: err=%v discovery=%d operation=%d", thirdErr, discoveryCalls.Load(), initialCalls.Load())
	}
}

func TestRuntimePolicyRecoveryConcurrentSuccessPreservesDurableState(t *testing.T) {
	harness := newRuntimePolicyRecoveryHarness(t, false, true)
	oldIdentity := harness.store.Identity()
	oldAssignments, err := harness.store.Assignments()
	if err != nil {
		t.Fatal(err)
	}
	oldEvents, err := harness.store.PendingEvents(harness.attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	oldResult, err := harness.store.PendingResult(harness.attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}

	results := make([]*RuntimeReadyPayload, 2)
	errorsByCaller := make([]error, 2)
	var callers sync.WaitGroup
	for index := range results {
		callers.Add(1)
		go func(index int) {
			defer callers.Done()
			results[index], errorsByCaller[index] = harness.client.HeartbeatRuntimeSession(context.Background(), harness.hello)
		}(index)
	}
	callers.Wait()
	for index, callErr := range errorsByCaller {
		if callErr != nil || results[index] == nil {
			t.Fatalf("caller %d result = %#v, %v", index, results[index], callErr)
		}
	}
	if harness.discoveryCalls.Load() != 1 || harness.oldHeartbeatCalls.Load() != 2 ||
		harness.createCalls.Load() != 1 || harness.resumeCalls.Load() != 1 || harness.newHeartbeatCalls.Load() != 2 {
		t.Fatalf("calls = discovery %d old-heartbeat %d create %d resume %d new-heartbeat %d",
			harness.discoveryCalls.Load(), harness.oldHeartbeatCalls.Load(), harness.createCalls.Load(),
			harness.resumeCalls.Load(), harness.newHeartbeatCalls.Load())
	}
	if harness.createReason.Load() != string(runtimeFallbackPolicyForced) {
		t.Fatalf("policy rediscovery attach reason = %q", harness.createReason.Load())
	}
	if harness.store.Identity() != oldIdentity {
		t.Fatalf("durable identity changed: before=%#v after=%#v", oldIdentity, harness.store.Identity())
	}
	assignments, err := harness.store.Assignments()
	if err != nil {
		t.Fatal(err)
	}
	events, err := harness.store.PendingEvents(harness.attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := harness.store.PendingResult(harness.attempt.AttemptID)
	if err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(assignments, oldAssignments) || !reflect.DeepEqual(events, oldEvents) || !reflect.DeepEqual(result, oldResult) {
		t.Fatalf("durable journal/spool changed across recovery:\nassignments=%#v\nevents=%#v\nresult=%#v", assignments, events, result)
	}
}

func TestRuntimePolicyRecoverySecondSignalDoesNotLoop(t *testing.T) {
	harness := newRuntimePolicyRecoveryHarness(t, true, false)
	_, firstErr := harness.client.HeartbeatRuntimeSession(context.Background(), harness.hello)
	var recoveryErr *runtimePolicyRecoveryError
	if !errors.As(firstErr, &recoveryErr) {
		t.Fatalf("second signal error = %T %v", firstErr, firstErr)
	}
	if firstErr.Error() != runtimePolicyRecoveryExhausted {
		t.Fatalf("second signal message = %q", firstErr.Error())
	}
	_, secondErr := harness.client.HeartbeatRuntimeSession(context.Background(), harness.hello)
	if secondErr != firstErr {
		t.Fatalf("terminal second-signal failure was not shared: first=%v second=%v", firstErr, secondErr)
	}
	if harness.discoveryCalls.Load() != 1 || harness.oldHeartbeatCalls.Load() != 1 ||
		harness.createCalls.Load() != 1 || harness.newHeartbeatCalls.Load() != 1 {
		t.Fatalf("second signal looped: discovery=%d old=%d create=%d new=%d",
			harness.discoveryCalls.Load(), harness.oldHeartbeatCalls.Load(), harness.createCalls.Load(), harness.newHeartbeatCalls.Load())
	}
}

func TestRuntimePolicyRecoveryFromEstablishedWebSocketClose(t *testing.T) {
	harness := newRuntimePolicyRecoveryHarness(t, false, false)
	oldWebSocket := newFakeRuntimeDuplex(harness.oldClient)
	epoch, _, previous := harness.node.transport.beginTransition(RuntimeTransportConnectingWS)
	if previous != harness.oldClient || !harness.node.transport.activateIfCurrent(epoch, RuntimeTransportWebSocket, oldWebSocket) {
		t.Fatal("could not establish policy recovery WebSocket")
	}

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		harness.node.transportSupervisorLoop(ctx)
	}()
	oldWebSocket.disconnect(&websocket.CloseError{Code: websocket.ClosePolicyViolation, Text: runtimePolicyChangedMessage})

	deadline := time.Now().Add(3 * time.Second)
	for {
		kind, state, _ := harness.node.transport.snapshot()
		if harness.discoveryCalls.Load() == 1 && kind == RuntimeTransportPull && state == RuntimeTransportPullActive {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("WebSocket policy recovery did not install pull: discovery=%d kind=%q state=%q",
				harness.discoveryCalls.Load(), kind, state)
		}
		time.Sleep(time.Millisecond)
	}
	time.Sleep(10 * time.Millisecond)
	if harness.discoveryCalls.Load() != 1 || harness.createCalls.Load() != 1 {
		t.Fatalf("WebSocket close caused repeated recovery: discovery=%d create=%d",
			harness.discoveryCalls.Load(), harness.createCalls.Load())
	}
	cancel()
	select {
	case <-done:
	case <-time.After(time.Second):
		t.Fatal("transport supervisor did not stop")
	}
}

func TestRuntimePolicyRecoveryFromInitialPullAttachSignal(t *testing.T) {
	harness := newRuntimePolicyRecoveryHarness(t, false, false)
	harness.oldClient.createFn = func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		return nil, runtimePolicyChangedTestError()
	}
	ready, err := harness.node.startInitialRuntimeTransport(context.Background())
	if !runtimePolicyRecoverySignal(err) {
		t.Fatalf("initial attach signal was flattened: ready=%#v err=%T %v", ready, err, err)
	}
	ready, err = harness.node.recoverRuntimePolicy(context.Background(), harness.node.currentPolicyRevision())
	if err != nil || ready == nil {
		t.Fatalf("initial attach recovery = ready %#v, %v", ready, err)
	}
	if harness.discoveryCalls.Load() != 1 || harness.createCalls.Load() != 1 {
		t.Fatalf("initial attach recovery calls = discovery %d create %d",
			harness.discoveryCalls.Load(), harness.createCalls.Load())
	}
}

func TestRuntimePolicyRecoveryFailsClosedWithoutCanonicalPlatform(t *testing.T) {
	node := &RuntimeWorker{}
	_, err := node.recoverRuntimePolicy(context.Background(), 0)
	var recoveryErr *runtimePolicyRecoveryError
	if !errors.As(err, &recoveryErr) || !strings.Contains(err.Error(), "requires PlatformURL") {
		t.Fatalf("recovery error = %T %v", err, err)
	}
}

func TestRuntimePolicyRecoveryRejectsMTLSRequirementChange(t *testing.T) {
	node := &RuntimeWorker{
		PlatformURL:         "https://platform.example.test",
		Transport:           RuntimeTransportAuto,
		runtimeMTLSRequired: false,
		runtimeDiscovery: func(context.Context, string) (runtimeConnectionInformation, error) {
			return runtimeConnectionInformation{
				RuntimeURL:   "https://runtime.example.test",
				MTLSRequired: true,
				Policy:       legacyRuntimeTransportPolicy(),
			}, nil
		},
	}

	_, err := node.recoverRuntimePolicy(context.Background(), 0)
	var recoveryErr *runtimePolicyRecoveryError
	if !errors.As(err, &recoveryErr) || !strings.Contains(err.Error(), "mTLS requirement changed") {
		t.Fatalf("recovery error = %T %v", err, err)
	}
}

func TestRuntimePolicyRecoveryTokenOnlyModeReturnsTypedSecurityError(t *testing.T) {
	node := &RuntimeWorker{
		PlatformURL:         "https://platform.example.test",
		Transport:           RuntimeTransportAuto,
		RequireTokenOnly:    true,
		runtimeMTLSRequired: false,
		runtimeDiscovery: func(context.Context, string) (runtimeConnectionInformation, error) {
			return runtimeConnectionInformation{
				RuntimeURL: "https://runtime.example.test", MTLSRequired: true,
				Policy: legacyRuntimeTransportPolicy(),
			}, nil
		},
	}

	_, err := node.recoverRuntimePolicy(context.Background(), 0)
	if !errors.Is(err, ErrRuntimeSecurityPolicyUnsupported) {
		t.Fatalf("recovery error = %T %v", err, err)
	}
}

func TestRuntimePolicyRecoveryRejectsExplicitTransportOutsideNewAllowlist(t *testing.T) {
	var discoveryCalls atomic.Int32
	node := &RuntimeWorker{
		PlatformURL: "https://platform.example.test",
		Transport:   RuntimeTransportWebSocket,
		runtimeDiscovery: func(context.Context, string) (runtimeConnectionInformation, error) {
			discoveryCalls.Add(1)
			policy := legacyRuntimeTransportPolicy()
			policy.Allowed = []RuntimeTransportMode{RuntimeTransportPull}
			policy.Default = RuntimeTransportPull
			return runtimeConnectionInformation{RuntimeURL: "https://runtime.example.test", Policy: policy}, nil
		},
	}
	_, err := node.recoverRuntimePolicy(context.Background(), 0)
	var recoveryErr *runtimePolicyRecoveryError
	if discoveryCalls.Load() != 1 || !errors.As(err, &recoveryErr) || !strings.Contains(err.Error(), "not allowed") {
		t.Fatalf("explicit transport recovery = calls %d, %T %v", discoveryCalls.Load(), err, err)
	}
}

type runtimePolicyRecoveryHarness struct {
	node              *RuntimeWorker
	oldClient         *fakeRuntimeClient
	client            *policyRecoveringRuntimeClient
	store             *FileRuntimeStore
	attempt           AttemptIdentity
	hello             RuntimeHelloPayload
	discoveryCalls    atomic.Int32
	oldHeartbeatCalls atomic.Int32
	createCalls       atomic.Int32
	resumeCalls       atomic.Int32
	newHeartbeatCalls atomic.Int32
	createReason      atomic.Value
}

func newRuntimePolicyRecoveryHarness(t *testing.T, retrySignals bool, durable bool) *runtimePolicyRecoveryHarness {
	t.Helper()
	harness := &runtimePolicyRecoveryHarness{}
	harness.createReason.Store("")
	now := time.Now().UTC().Truncate(time.Millisecond)
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		w.Header().Set("Content-Type", "application/json")
		switch {
		case request.URL.Path == "/api/v1/agent-runtime/sessions":
			harness.createCalls.Add(1)
			harness.createReason.Store(request.Header.Get(RuntimeFallbackReasonHeader))
			writeRuntimeTestJSON(t, w, RuntimeReadyPayload{
				CoreInstanceID: runtimeTestCoreID, AttachmentID: runtimeTestAttachmentID,
				Features: RuntimeRequiredFeatures(), OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
			})
		case strings.HasPrefix(request.URL.Path, "/api/v1/agent-runtime/sessions/") && strings.HasSuffix(request.URL.Path, "/heartbeat"):
			harness.newHeartbeatCalls.Add(1)
			if retrySignals {
				w.WriteHeader(http.StatusForbidden)
				_, _ = w.Write([]byte(`{"error":{"code":"FORBIDDEN","message":"RUNTIME_POLICY_CHANGED"}}`))
				return
			}
			writeRuntimeTestJSON(t, w, RuntimeReadyPayload{
				CoreInstanceID: runtimeTestCoreID, AttachmentID: runtimeTestAttachmentID,
				Features: RuntimeRequiredFeatures(), OfferTTLSeconds: 30, LeaseTTLSeconds: 60, DatabaseTime: now,
			})
		case request.URL.Path == "/api/v1/agent-runtime/runs/resume":
			harness.resumeCalls.Add(1)
			var resume RuntimeResumePayload
			if err := json.NewDecoder(request.Body).Decode(&resume); err != nil {
				http.Error(w, "invalid resume", http.StatusBadRequest)
				return
			}
			decisions := make([]RuntimeResumeAcceptedPayload, len(resume.Attempts))
			for index, attempt := range resume.Attempts {
				expires := now.Add(time.Minute)
				decisions[index] = RuntimeResumeAcceptedPayload{
					AttemptIdentity: attempt.AttemptIdentity,
					Decision:        RuntimeResumeContinue,
					LeaseExpiresAt:  &expires,
					AllowedActions: []RuntimeResumeAction{
						RuntimeActionContinueExecution, RuntimeActionUploadEvents, RuntimeActionUploadResult,
					},
				}
			}
			writeRuntimeTestJSON(t, w, RuntimeResumeResponse{Decisions: decisions})
		default:
			http.NotFound(w, request)
		}
	}))
	t.Cleanup(server.Close)

	store := openRuntimeStoreForTest(t, t.TempDir())
	harness.store = store
	if durable {
		harness.attempt = AttemptIdentity{
			NodeID: testNodeID, AgentID: testAgentID, WorkerID: store.Identity().WorkerID,
			RuntimeSessionID: store.Identity().RuntimeSessionID, SessionEpoch: store.Identity().SessionEpoch,
			AssignmentMessageID: runtimeTestEventID, RunID: testRunID, AttemptID: testAttemptID,
			OfferID: runtimeTestResultID, LeaseID: testLeaseID, FencingToken: 1,
		}
		if err := store.CreateAssignment(testAssignmentRecord(harness.attempt)); err != nil {
			t.Fatal(err)
		}
		for _, state := range []AssignmentState{AssignmentStateACKSent, AssignmentStateConfirmed, AssignmentStateStarted} {
			if _, err := store.AdvanceAssignment(harness.attempt.AssignmentMessageID, state); err != nil {
				t.Fatal(err)
			}
		}
		if _, err := store.AppendEvent(harness.attempt, "run.progress", json.RawMessage(`{"step":1}`)); err != nil {
			t.Fatal(err)
		}
		if _, err := store.AppendResult(harness.attempt, "success", json.RawMessage(`{"answer":42}`)); err != nil {
			t.Fatal(err)
		}
	}

	oldClient := newFakeRuntimeClient()
	harness.oldClient = oldClient
	releaseInitial := make(chan struct{})
	oldClient.heartbeatFn = func(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
		calls := harness.oldHeartbeatCalls.Add(1)
		if !durable || calls == 2 {
			select {
			case <-releaseInitial:
			default:
				close(releaseInitial)
			}
		}
		<-releaseInitial
		return nil, runtimePolicyChangedTestError()
	}
	gate := activeRuntimePolicyTestGate(t, oldClient)
	policy := legacyRuntimeTransportPolicy()
	policy.Allowed = []RuntimeTransportMode{RuntimeTransportPull}
	policy.Default = RuntimeTransportPull
	node := &RuntimeWorker{
		PlatformURL: "https://platform.example.test",
		RuntimeURL:  "https://old-runtime.example.test",
		Transport:   RuntimeTransportAuto,
		NodeID:      testNodeID,
		NodeVersion: runtimeWorkerSDKAgent,
		AgentID:     testAgentID,
		AgentToken:  "ol_agent_runtime",
		Capacity:    1,

		HeartbeatInterval: time.Hour,
		RetryMinimum:      time.Millisecond,
		RetryMaximum:      5 * time.Millisecond,

		store:                 store,
		runtimeCtx:            context.Background(),
		httpClient:            server.Client(),
		transport:             gate,
		runtimeDialer:         &sdkRuntimeTransportDialer{},
		initialResumeComplete: durable,
		active:                make(map[string]*activeRuntimeAttempt),
		spoolAllowed:          make(map[string]spoolPermission),
		wakeSpool:             make(chan struct{}, 1),
		jitter:                func(value time.Duration) time.Duration { return value },
		runtimeDiscovery: func(context.Context, string) (runtimeConnectionInformation, error) {
			harness.discoveryCalls.Add(1)
			return runtimeConnectionInformation{RuntimeURL: server.URL, Policy: policy}, nil
		},
	}
	if err := node.applyRuntimeTransportPolicy(policy); err != nil {
		t.Fatal(err)
	}
	harness.node = node
	harness.hello = node.runtimeHello()
	harness.client = &policyRecoveringRuntimeClient{node: node, transport: gate}
	return harness
}

func activeRuntimePolicyTestGate(t *testing.T, client RuntimeClient) *switchingRuntimeClient {
	t.Helper()
	gate := newSwitchingRuntimeClient(client)
	epoch, _, _ := gate.beginTransition(RuntimeTransportSwitchingPull)
	if !gate.activateIfCurrent(epoch, RuntimeTransportPull, client) {
		t.Fatal("could not activate policy test transport")
	}
	return gate
}

func runtimePolicyChangedTestError() error {
	return &Error{StatusCode: http.StatusForbidden, Code: "FORBIDDEN", Message: runtimePolicyChangedMessage}
}
