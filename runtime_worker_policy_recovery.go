package openlinker

import (
	"context"
	"errors"
	"fmt"
	"net/http"

	"github.com/gorilla/websocket"
)

const (
	runtimeTransportForbiddenMessage = "RUNTIME_TRANSPORT_FORBIDDEN"
	runtimePolicyChangedMessage      = "RUNTIME_POLICY_CHANGED"
	runtimePolicyRecoveryExhausted   = "OpenLinker Runtime policy recovery failed: policy signal persisted after one canonical rediscovery"
)

type runtimePolicyRecoveryError struct {
	cause   error
	message string
}

func (err *runtimePolicyRecoveryError) Error() string {
	if err.message != "" {
		return err.message
	}
	return "OpenLinker Runtime policy recovery failed: " + err.cause.Error()
}

func (err *runtimePolicyRecoveryError) Unwrap() error { return err.cause }

func runtimePolicyRecoveryHTTPError(err error) bool {
	var runtimeErr *Error
	if !errors.As(err, &runtimeErr) || runtimeErr.StatusCode != http.StatusForbidden || runtimeErr.Code != "FORBIDDEN" {
		return false
	}
	return runtimeErr.Message == runtimeTransportForbiddenMessage ||
		runtimeErr.Message == runtimePolicyChangedMessage
}

func runtimePolicyRecoveryWSClose(err error) bool {
	var closeErr *websocket.CloseError
	return errors.As(err, &closeErr) && closeErr.Code == websocket.ClosePolicyViolation &&
		closeErr.Text == runtimePolicyChangedMessage
}

func runtimePolicyRecoverySignal(err error) bool {
	var recoveryErr *runtimePolicyRecoveryError
	if errors.As(err, &recoveryErr) {
		return false
	}
	return err != nil && (runtimePolicyRecoveryHTTPError(err) || runtimePolicyRecoveryWSClose(err))
}

func runtimePolicyRecoverOnce[T any](
	operation func() (T, error),
	recoverPolicy func(error) error,
) (T, error) {
	value, err := operation()
	if !runtimePolicyRecoverySignal(err) {
		return value, err
	}
	if recoveryErr := recoverPolicy(err); recoveryErr != nil {
		var zero T
		return zero, recoveryErr
	}
	// Deliberately invoke the operation only once more. A second policy signal
	// is terminally wrapped and can never start a rediscovery loop.
	value, err = operation()
	if runtimePolicyRecoverySignal(err) {
		var zero T
		return zero, newRuntimePolicyRecoveryExhaustedError(err)
	}
	return value, err
}

type policyRecoveringRuntimeClient struct {
	node      *RuntimeWorker
	transport *switchingRuntimeClient
}

func (client *policyRecoveringRuntimeClient) call(
	ctx context.Context,
	operation func() (any, error),
) (any, error) {
	revision, terminalErr := client.node.policyRecoverySnapshot()
	if terminalErr != nil {
		return nil, terminalErr
	}
	value, err := operation()
	if !runtimePolicyRecoverySignal(err) {
		return value, err
	}
	if _, recoveryErr := client.node.recoverRuntimePolicy(ctx, revision); recoveryErr != nil {
		return nil, recoveryErr
	}
	value, err = operation()
	if runtimePolicyRecoverySignal(err) {
		return nil, client.node.failRuntimePolicyRecovery(err)
	}
	return value, err
}

func (client *policyRecoveringRuntimeClient) CreateRuntimeSession(ctx context.Context, request RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.CreateRuntimeSession(ctx, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeReadyPayload), err
}

func (client *policyRecoveringRuntimeClient) HeartbeatRuntimeSession(ctx context.Context, request RuntimeHelloPayload) (*RuntimeReadyPayload, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.HeartbeatRuntimeSession(ctx, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeReadyPayload), err
}

func (client *policyRecoveringRuntimeClient) DrainRuntimeSession(
	ctx context.Context,
	runtimeSessionID string,
	request RuntimeDrainPayload,
) (*RuntimeDrainPayload, error) {
	value, err := client.call(ctx, func() (any, error) {
		return client.transport.DrainRuntimeSession(ctx, runtimeSessionID, request)
	})
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeDrainPayload), err
}

func (client *policyRecoveringRuntimeClient) CloseRuntimeSession(ctx context.Context, request RuntimeSessionCloseRequest) error {
	_, err := client.call(ctx, func() (any, error) { return nil, client.transport.CloseRuntimeSession(ctx, request) })
	return err
}

func (client *policyRecoveringRuntimeClient) ClaimRuntimeRun(ctx context.Context, wait int, request RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.ClaimRuntimeRun(ctx, wait, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeRunAssignedPayload), err
}

func (client *policyRecoveringRuntimeClient) AckRuntimeAssignment(ctx context.Context, request RuntimeAssignmentAckPayload) (*RuntimeAssignmentConfirmedPayload, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.AckRuntimeAssignment(ctx, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeAssignmentConfirmedPayload), err
}

func (client *policyRecoveringRuntimeClient) RejectRuntimeAssignment(ctx context.Context, request RuntimeAssignmentRejectPayload) (*RuntimeAssignmentRejectedPayload, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.RejectRuntimeAssignment(ctx, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeAssignmentRejectedPayload), err
}

func (client *policyRecoveringRuntimeClient) RenewRuntimeLease(ctx context.Context, request RuntimeLeaseRenewPayload) (*RuntimeLeaseRenewedPayload, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.RenewRuntimeLease(ctx, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeLeaseRenewedPayload), err
}

func (client *policyRecoveringRuntimeClient) AppendRuntimeEvent(ctx context.Context, request RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.AppendRuntimeEvent(ctx, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeRunEventAckPayload), err
}

func (client *policyRecoveringRuntimeClient) FinalizeRuntimeResult(ctx context.Context, request RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.FinalizeRuntimeResult(ctx, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeRunResultAckPayload), err
}

func (client *policyRecoveringRuntimeClient) ResumeRuntimeRuns(ctx context.Context, request RuntimeResumePayload) (*RuntimeResumeResponse, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.ResumeRuntimeRuns(ctx, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeResumeResponse), err
}

func (client *policyRecoveringRuntimeClient) PollRuntimeCommands(ctx context.Context, sessionID string, wait int) (*RuntimeCommandsResponse, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.PollRuntimeCommands(ctx, sessionID, wait) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeCommandsResponse), err
}

func (client *policyRecoveringRuntimeClient) AckRuntimeCancel(ctx context.Context, request RuntimeRunCancelAckPayload) (*RuntimeRunCancellationState, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.AckRuntimeCancel(ctx, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeRunCancellationState), err
}

func (client *policyRecoveringRuntimeClient) CallRuntimeAgent(ctx context.Context, authorization RuntimeCallAgentAuthorization, request RuntimeCallAgentRequest) (*RuntimeRunSummary, error) {
	value, err := client.call(ctx, func() (any, error) { return client.transport.CallRuntimeAgent(ctx, authorization, request) })
	if value == nil {
		return nil, err
	}
	return value.(*RuntimeRunSummary), err
}

func (node *RuntimeWorker) currentPolicyRevision() uint64 {
	revision, _ := node.policyRecoverySnapshot()
	return revision
}

func (node *RuntimeWorker) policyRecoverySnapshot() (uint64, error) {
	node.policyRecoveryMu.Lock()
	defer node.policyRecoveryMu.Unlock()
	return node.policyRevision, node.policyTerminalError
}

func (node *RuntimeWorker) recoverRuntimePolicy(
	ctx context.Context,
	observedRevision uint64,
) (*RuntimeReadyPayload, error) {
	node.policyRecoveryMu.Lock()
	defer node.policyRecoveryMu.Unlock()
	if node.policyTerminalError != nil {
		return node.policyLastReady, node.policyTerminalError
	}
	if node.policyRevision != observedRevision {
		if node.policyLastObserved == observedRevision {
			return node.policyLastReady, node.policyLastError
		}
		node.stateMu.RLock()
		ready := node.ready
		node.stateMu.RUnlock()
		return ready, nil
	}
	// Consume this incident before touching discovery. Success and failure are
	// both memoized so concurrent callers that observed the same generation can
	// never serialize into multiple rediscovery attempts.
	node.policyRevision++
	node.policyLastObserved = observedRevision
	ready, err := node.recoverRuntimePolicyLocked(ctx)
	node.policyLastReady = ready
	node.policyLastError = err
	if err != nil {
		node.policyTerminalError = err
	}
	return ready, err
}

func (node *RuntimeWorker) failRuntimePolicyRecovery(cause error) error {
	node.policyRecoveryMu.Lock()
	defer node.policyRecoveryMu.Unlock()
	if node.policyTerminalError != nil {
		return node.policyTerminalError
	}
	err := newRuntimePolicyRecoveryExhaustedError(cause)
	node.policyLastObserved = node.policyRevision
	node.policyLastReady = nil
	node.policyLastError = err
	node.policyTerminalError = err
	return err
}

func newRuntimePolicyRecoveryExhaustedError(cause error) *runtimePolicyRecoveryError {
	return &runtimePolicyRecoveryError{cause: cause, message: runtimePolicyRecoveryExhausted}
}

func (node *RuntimeWorker) recoverRuntimePolicyLocked(ctx context.Context) (*RuntimeReadyPayload, error) {
	if node.PlatformURL == "" {
		return nil, &runtimePolicyRecoveryError{cause: errors.New(
			"canonical rediscovery requires PlatformURL; an explicit RuntimeURL alone fails closed",
		)}
	}
	discover := node.runtimeDiscovery
	if discover == nil {
		discover = func(parent context.Context, platformURL string) (runtimeConnectionInformation, error) {
			return resolveRuntimeConnection(parent, platformURL, "")
		}
	}
	connection, err := discover(ctx, node.PlatformURL)
	if err != nil {
		return nil, &runtimePolicyRecoveryError{cause: fmt.Errorf("rediscover canonical manifest: %w", err)}
	}
	if _, err = node.runtimeTransportOrder(connection.Policy); err != nil {
		return nil, &runtimePolicyRecoveryError{cause: err}
	}
	if node.httpClient == nil {
		return nil, &runtimePolicyRecoveryError{cause: errors.New("mTLS Runtime HTTP client is unavailable")}
	}
	runtimeClient, err := NewRuntime(
		connection.RuntimeURL,
		WithAgentToken(node.AgentToken),
		WithHTTPClient(node.httpClient),
		WithSDKAgent(runtimeWorkerSDKAgent),
	)
	if err != nil {
		return nil, &runtimePolicyRecoveryError{cause: fmt.Errorf("connect rediscovered Runtime: %w", err)}
	}
	node.transportTransitionMu.Lock()
	defer node.transportTransitionMu.Unlock()
	if err = node.applyRecoveredRuntimeTransportPolicy(connection.Policy); err != nil {
		return nil, &runtimePolicyRecoveryError{cause: err}
	}
	dialer, ok := node.runtimeDialer.(*sdkRuntimeTransportDialer)
	if !ok {
		return nil, &runtimePolicyRecoveryError{cause: errors.New("Runtime dialer cannot apply rediscovery")}
	}
	node.RuntimeURL = connection.RuntimeURL
	node.transport.replaceCallClient(runtimeClient)
	dialer.setRuntime(runtimeClient)
	ready, err := node.startRuntimeTransportLocked(ctx, node.initialResumeComplete)
	if err != nil {
		return nil, &runtimePolicyRecoveryError{cause: err}
	}
	node.stateMu.Lock()
	node.ready = ready
	node.stateMu.Unlock()
	node.signalSpool()
	node.logf("runtime transport policy recovered through canonical rediscovery")
	return ready, nil
}

var _ RuntimeClient = (*policyRecoveringRuntimeClient)(nil)
