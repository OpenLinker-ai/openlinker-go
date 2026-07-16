package openlinker

import (
	"context"
	"errors"
	"fmt"
	"sync"
)

type RuntimeTransportMode string

const (
	RuntimeTransportAuto      RuntimeTransportMode = "auto"
	RuntimeTransportWebSocket RuntimeTransportMode = "ws"
	RuntimeTransportPull      RuntimeTransportMode = "pull"
)

type RuntimeTransportState string

const (
	RuntimeTransportDisconnected    RuntimeTransportState = "disconnected"
	RuntimeTransportConnectingWS    RuntimeTransportState = "connecting_ws"
	RuntimeTransportWebSocketActive RuntimeTransportState = "ws_active"
	RuntimeTransportSwitchingPull   RuntimeTransportState = "switching_to_pull"
	RuntimeTransportPullActive      RuntimeTransportState = "pull_active"
	RuntimeTransportProbingWS       RuntimeTransportState = "probing_ws"
	RuntimeTransportSwitchingWS     RuntimeTransportState = "switching_to_ws"
	RuntimeTransportStopped         RuntimeTransportState = "stopped"
)

var ErrRuntimeTransportSwitching = errors.New("runtime transport is switching")

var errRuntimeTransportTransitionSuperseded = errors.New("runtime transport transition was superseded")

type RuntimeDuplexClient interface {
	RuntimeClient
	Done() <-chan struct{}
	Err() error
}

type RuntimeTransportDialer interface {
	DialRuntimeWebSocket(context.Context, RuntimeHelloPayload) (RuntimeDuplexClient, error)
	ProbeRuntimeWebSocket(context.Context) error
}

type sdkRuntimeTransportDialer struct {
	mu      sync.RWMutex
	runtime *Runtime
}

func (dialer *sdkRuntimeTransportDialer) setRuntime(runtime *Runtime) {
	dialer.mu.Lock()
	dialer.runtime = runtime
	dialer.mu.Unlock()
}

func (dialer *sdkRuntimeTransportDialer) current() *Runtime {
	dialer.mu.RLock()
	defer dialer.mu.RUnlock()
	return dialer.runtime
}

func (dialer *sdkRuntimeTransportDialer) DialRuntimeWebSocket(
	ctx context.Context,
	hello RuntimeHelloPayload,
) (RuntimeDuplexClient, error) {
	runtime := dialer.current()
	if runtime == nil {
		return nil, errors.New("runtime WebSocket dialer is unavailable")
	}
	return runtime.DialRuntimeWebSocket(ctx, hello)
}

func (dialer *sdkRuntimeTransportDialer) ProbeRuntimeWebSocket(ctx context.Context) error {
	runtime := dialer.current()
	if runtime == nil {
		return errors.New("runtime WebSocket dialer is unavailable")
	}
	return runtime.ProbeRuntimeWebSocket(ctx)
}

// switchingRuntimeClient is the transport gate shared by every runtime
// loop. A transition first removes the active client and cancels its generation
// context, then waits for every in-flight operation to exit. A new client is
// published only after attach and durable resume have succeeded.
type switchingRuntimeClient struct {
	mu         sync.Mutex
	cond       *sync.Cond
	active     RuntimeClient
	kind       RuntimeTransportMode
	state      RuntimeTransportState
	generation context.Context
	cancel     context.CancelFunc
	operations int
	callClient RuntimeClient
	// transitionEpoch fences attach work that completes after a newer transport
	// transition (especially shutdown) has already taken ownership of the gate.
	transitionEpoch uint64
}

func newSwitchingRuntimeClient(callClient RuntimeClient) *switchingRuntimeClient {
	client := &switchingRuntimeClient{
		state:      RuntimeTransportDisconnected,
		callClient: callClient,
	}
	client.cond = sync.NewCond(&client.mu)
	return client
}

func (client *switchingRuntimeClient) setState(state RuntimeTransportState) {
	client.mu.Lock()
	if client.state != RuntimeTransportStopped {
		client.state = state
	}
	client.mu.Unlock()
}

func (client *switchingRuntimeClient) snapshot() (RuntimeTransportMode, RuntimeTransportState, RuntimeClient) {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.kind, client.state, client.active
}

func (client *switchingRuntimeClient) callRuntimeClient() RuntimeClient {
	client.mu.Lock()
	defer client.mu.Unlock()
	return client.callClient
}

func (client *switchingRuntimeClient) replaceCallClient(callClient RuntimeClient) {
	client.mu.Lock()
	client.callClient = callClient
	client.mu.Unlock()
}

func (client *switchingRuntimeClient) activateIfCurrent(epoch uint64, kind RuntimeTransportMode, active RuntimeClient) bool {
	client.mu.Lock()
	defer client.mu.Unlock()
	if epoch == 0 || epoch != client.transitionEpoch || client.state == RuntimeTransportStopped {
		return false
	}
	if client.cancel != nil {
		client.cancel()
	}
	client.generation, client.cancel = context.WithCancel(context.Background())
	client.active = active
	client.kind = kind
	if kind == RuntimeTransportWebSocket {
		client.state = RuntimeTransportWebSocketActive
	} else {
		client.state = RuntimeTransportPullActive
	}
	return true
}

func (client *switchingRuntimeClient) beginTransition(state RuntimeTransportState) (uint64, RuntimeTransportMode, RuntimeClient) {
	client.mu.Lock()
	if client.state == RuntimeTransportStopped {
		client.mu.Unlock()
		return 0, "", nil
	}
	client.transitionEpoch++
	epoch := client.transitionEpoch
	client.state = state
	kind, active := client.kind, client.active
	client.active = nil
	client.kind = ""
	if client.cancel != nil {
		client.cancel()
	}
	for client.operations > 0 {
		client.cond.Wait()
	}
	client.mu.Unlock()
	return epoch, kind, active
}

func (client *switchingRuntimeClient) stop() (RuntimeTransportMode, RuntimeClient) {
	_, kind, active := client.beginTransition(RuntimeTransportStopped)
	return kind, active
}

func (client *switchingRuntimeClient) begin(
	parent context.Context,
) (RuntimeClient, context.Context, func(), error) {
	if parent == nil {
		parent = context.Background()
	}
	client.mu.Lock()
	active := client.active
	generation := client.generation
	if active == nil || generation == nil {
		client.mu.Unlock()
		return nil, nil, nil, ErrRuntimeTransportSwitching
	}
	client.operations++
	client.mu.Unlock()

	ctx, cancel := context.WithCancel(parent)
	stopGeneration := context.AfterFunc(generation, cancel)
	done := func() {
		stopGeneration()
		cancel()
		client.mu.Lock()
		client.operations--
		if client.operations == 0 {
			client.cond.Broadcast()
		}
		client.mu.Unlock()
	}
	return active, ctx, done, nil
}

func (client *switchingRuntimeClient) CreateRuntimeSession(ctx context.Context, request RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.CreateRuntimeSession(callCtx, request)
}

func (client *switchingRuntimeClient) HeartbeatRuntimeSession(ctx context.Context, request RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.HeartbeatRuntimeSession(callCtx, request)
}

func (client *switchingRuntimeClient) DrainRuntimeSession(
	ctx context.Context,
	runtimeSessionID string,
	request RuntimeDrainPayload,
) (*RuntimeDrainPayload, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	drainer, ok := active.(runtimeDrainClient)
	if !ok {
		return nil, fmt.Errorf("%w: Runtime Client does not implement session drain", ErrRuntimeProtocolMismatch)
	}
	return drainer.DrainRuntimeSession(callCtx, runtimeSessionID, request)
}

func (client *switchingRuntimeClient) CloseRuntimeSession(ctx context.Context, request RuntimeSessionCloseRequest) error {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return err
	}
	defer done()
	return active.CloseRuntimeSession(callCtx, request)
}

func (client *switchingRuntimeClient) ClaimRuntimeRun(ctx context.Context, wait int, request RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.ClaimRuntimeRun(callCtx, wait, request)
}

func (client *switchingRuntimeClient) AckRuntimeAssignment(ctx context.Context, request RuntimeAssignmentAckPayload) (*RuntimeAssignmentConfirmedPayload, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.AckRuntimeAssignment(callCtx, request)
}

func (client *switchingRuntimeClient) RejectRuntimeAssignment(ctx context.Context, request RuntimeAssignmentRejectPayload) (*RuntimeAssignmentRejectedPayload, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.RejectRuntimeAssignment(callCtx, request)
}

func (client *switchingRuntimeClient) RenewRuntimeLease(ctx context.Context, request RuntimeLeaseRenewPayload) (*RuntimeLeaseRenewedPayload, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.RenewRuntimeLease(callCtx, request)
}

func (client *switchingRuntimeClient) AppendRuntimeEvent(ctx context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.AppendRuntimeEvent(callCtx, request)
}

func (client *switchingRuntimeClient) FinalizeRuntimeResult(ctx context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.FinalizeRuntimeResult(callCtx, request)
}

func (client *switchingRuntimeClient) ResumeRuntimeRuns(ctx context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.ResumeRuntimeRuns(callCtx, request)
}

func (client *switchingRuntimeClient) PollRuntimeCommands(ctx context.Context, sessionID string, wait int) (*RuntimeCommandsResponse, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.PollRuntimeCommands(callCtx, sessionID, wait)
}

func (client *switchingRuntimeClient) AckRuntimeCancel(ctx context.Context, request RuntimeRunCancelAckPayload) (*RuntimeRunCancellationState, error) {
	active, callCtx, done, err := client.begin(ctx)
	if err != nil {
		return nil, err
	}
	defer done()
	return active.AckRuntimeCancel(callCtx, request)
}

func (client *switchingRuntimeClient) CallRuntimeAgent(ctx context.Context, authorization RuntimeCallAgentAuthorization, request RuntimeCallAgentRequest) (*RuntimeRunSummary, error) {
	callClient := client.callRuntimeClient()
	if callClient == nil {
		return nil, ErrRuntimeTransportSwitching
	}
	return callClient.CallRuntimeAgent(ctx, authorization, request)
}

var _ RuntimeClient = (*switchingRuntimeClient)(nil)
