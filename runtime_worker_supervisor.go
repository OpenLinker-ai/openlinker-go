package openlinker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

type runtimeFallbackTransition string

const (
	runtimeFallbackPolicySelected               runtimeFallbackTransition = "policy_selected"
	runtimeFallbackSameTransportReconnect       runtimeFallbackTransition = "same_transport_reconnect"
	runtimeFallbackPolicyRediscovery            runtimeFallbackTransition = "policy_rediscovery"
	runtimeFallbackWebSocketToLongPoll          runtimeFallbackTransition = "websocket_to_long_poll"
	runtimeFallbackFailedWSProbeRestoreLongPoll runtimeFallbackTransition = "failed_websocket_probe_restore_long_poll"
	runtimeFallbackLongPollToWebSocket          runtimeFallbackTransition = "long_poll_to_websocket"
)

func (node *RuntimeWorker) startInitialRuntimeTransport(parent context.Context) (*RuntimeReadyPayload, error) {
	return node.startRuntimeTransport(parent, false)
}

func (node *RuntimeWorker) startRuntimeTransport(parent context.Context, reconnect bool) (*RuntimeReadyPayload, error) {
	node.transportTransitionMu.Lock()
	defer node.transportTransitionMu.Unlock()
	return node.startRuntimeTransportLocked(parent, reconnect)
}

func (node *RuntimeWorker) startRuntimeTransportLocked(parent context.Context, reconnect bool) (*RuntimeReadyPayload, error) {
	transition := runtimeFallbackPolicySelected
	if reconnect {
		transition = runtimeFallbackPolicyRediscovery
	}
	reason := node.runtimeFallbackReason(transition)
	begin := func(state RuntimeTransportState, reason string) uint64 {
		epoch, _, previous := node.transport.beginTransition(state)
		if reconnect {
			node.closeTransport(previous, reason)
		}
		return epoch
	}
	mode := RuntimeTransportMode(node.Transport)
	switch mode {
	case RuntimeTransportPull:
		epoch := begin(RuntimeTransportSwitchingPull, "runtime_policy_recovery")
		return node.activatePullWithRetry(parent, reconnect, epoch, reason)
	case RuntimeTransportWebSocket:
		epoch := begin(RuntimeTransportConnectingWS, "runtime_policy_recovery")
		return node.activateWebSocketWithRetry(parent, reconnect, epoch, reason)
	case RuntimeTransportAuto:
		order := node.orderedRuntimeTransports()
		if len(order) == 0 {
			return nil, errors.New("OpenLinker Runtime has no usable transport")
		}
		if order[0] == RuntimeTransportPull {
			epoch := begin(RuntimeTransportSwitchingPull, "runtime_policy_recovery")
			return node.activatePullWithRetry(parent, reconnect, epoch, reason)
		}
		epoch := begin(RuntimeTransportConnectingWS, "runtime_policy_recovery")
		connection, ready, err := node.dialWebSocketOnce(parent, reason)
		if err == nil && reconnect {
			err = node.resumeDurableStateWithClient(parent, connection, true)
		}
		if err == nil {
			if err = node.publishRuntimeTransport(epoch, RuntimeTransportWebSocket, connection); err != nil {
				return nil, err
			}
			node.logf("runtime transport active: ws")
			return ready, nil
		}
		if connection != nil {
			node.closeTransport(connection, "websocket_attach_failed")
		}
		if runtimeErrorIsPermanent(err) && !runtimeAttachErrorIsRetryable(err) {
			return nil, scrubRuntimeError(err)
		}
		if !node.autoAllowsPullFallback() {
			return node.activateWebSocketWithRetry(parent, reconnect, epoch, reason)
		}
		node.logf("runtime WebSocket unavailable; activating HTTPS long-poll: %v", scrubRuntimeError(err))
		epoch = begin(RuntimeTransportSwitchingPull, "runtime_policy_recovery")
		return node.activatePullWithRetry(parent, reconnect, epoch, node.runtimeFallbackReason(runtimeFallbackWebSocketToLongPoll))
	default:
		return nil, errors.New("invalid runtime transport mode")
	}
}

func (node *RuntimeWorker) startTransportSupervisor() {
	if node.transport == nil || node.runtimeDialer == nil || RuntimeTransportMode(node.Transport) == RuntimeTransportPull {
		return
	}
	ctx, cancel := context.WithCancel(node.runtimeCtx)
	node.transportStop = cancel
	node.loops.Add(1)
	go func() {
		defer node.loops.Done()
		node.transportSupervisorLoop(ctx)
	}()
}

func (node *RuntimeWorker) transportSupervisorLoop(ctx context.Context) {
	probeAttempt := 0
	for ctx.Err() == nil {
		policyRevision, kind, _, active := node.runtimePolicyTransportSnapshot()
		switch kind {
		case RuntimeTransportWebSocket:
			connection, ok := active.(RuntimeDuplexClient)
			if !ok {
				node.reportFatal(errors.New("active WebSocket transport has no disconnect signal"))
				return
			}
			select {
			case <-ctx.Done():
				return
			case <-connection.Done():
			}
			if ctx.Err() != nil {
				return
			}
			disconnectErr := connection.Err()
			node.logf("runtime WebSocket disconnected: %v", scrubRuntimeError(disconnectErr))
			if runtimePolicyRecoverySignal(disconnectErr) {
				if _, err := node.recoverRuntimePolicy(ctx, policyRevision); err != nil && ctx.Err() == nil {
					node.reportFatal(scrubRuntimeError(err))
					return
				}
				probeAttempt = 0
				continue
			}
			var err error
			if RuntimeTransportMode(node.Transport) == RuntimeTransportWebSocket || !node.autoAllowsPullFallback() {
				err = node.reconnectWebSocket(ctx, policyRevision)
			} else {
				err = node.switchToPull(ctx, policyRevision)
			}
			if runtimePolicyRecoverySignal(err) {
				_, err = node.recoverRuntimePolicy(ctx, policyRevision)
			}
			if err != nil && ctx.Err() == nil {
				node.reportFatal(scrubRuntimeError(err))
				return
			}
			probeAttempt = 0
		case RuntimeTransportPull:
			retryMinimum, _ := node.runtimeRetryPolicy()
			if RuntimeTransportMode(node.Transport) == RuntimeTransportAuto && !node.autoPrefersWebSocket() {
				if sleepContext(ctx, retryMinimum) != nil {
					return
				}
				continue
			}
			delay, probeTimeout := node.runtimeProbePolicy()
			if delay <= 0 {
				delay = node.retryDelay(probeAttempt)
				if node.jitter != nil {
					delay = node.jitter(delay)
				} else {
					delay = jitterDuration(delay)
				}
			}
			if sleepContext(ctx, delay) != nil {
				return
			}
			if !node.setRuntimeTransportStateIfPolicyCurrent(policyRevision, RuntimeTransportProbingWS) {
				continue
			}
			probeCtx, cancel := context.WithTimeout(ctx, probeTimeout)
			probeCtx = withRuntimeFallbackReason(probeCtx, node.runtimeFallbackReason(runtimeFallbackLongPollToWebSocket))
			err := node.runtimeDialer.ProbeRuntimeWebSocket(probeCtx)
			cancel()
			if err != nil {
				if !node.setRuntimeTransportStateIfPolicyCurrent(policyRevision, RuntimeTransportPullActive) {
					continue
				}
				if runtimePolicyRecoverySignal(err) {
					if _, recoveryErr := node.recoverRuntimePolicy(ctx, policyRevision); recoveryErr != nil && ctx.Err() == nil {
						node.reportFatal(scrubRuntimeError(recoveryErr))
						return
					}
					probeAttempt = 0
					continue
				}
				if runtimeErrorIsPermanent(err) && !runtimeAttachErrorIsRetryable(err) {
					node.reportFatal(scrubRuntimeError(err))
					return
				}
				probeAttempt++
				continue
			}
			if err = node.switchToWebSocket(ctx, policyRevision); err != nil {
				if ctx.Err() != nil {
					return
				}
				if runtimePolicyRecoverySignal(err) {
					if _, recoveryErr := node.recoverRuntimePolicy(ctx, policyRevision); recoveryErr != nil {
						node.reportFatal(scrubRuntimeError(recoveryErr))
						return
					}
					probeAttempt = 0
					continue
				}
				if runtimeErrorIsPermanent(err) && !runtimeAttachErrorIsRetryable(err) {
					node.reportFatal(scrubRuntimeError(err))
					return
				}
				node.logf("runtime WebSocket recovery deferred: %v", scrubRuntimeError(err))
				probeAttempt++
				continue
			}
			probeAttempt = 0
		default:
			retryMinimum, _ := node.runtimeRetryPolicy()
			if sleepContext(ctx, retryMinimum) != nil {
				return
			}
		}
	}
}

// runtimePolicyTransportSnapshot binds an established transport to the policy
// generation that created it. Recovery takes the same locks in the same order,
// so the supervisor cannot pair an old WebSocket close with a newly incremented
// generation and trigger a duplicate canonical rediscovery.
func (node *RuntimeWorker) runtimePolicyTransportSnapshot() (uint64, RuntimeTransportMode, RuntimeTransportState, RuntimeClient) {
	node.policyRecoveryMu.Lock()
	defer node.policyRecoveryMu.Unlock()
	node.transportTransitionMu.Lock()
	defer node.transportTransitionMu.Unlock()
	kind, state, active := node.transport.snapshot()
	return node.policyRevision, kind, state, active
}

func (node *RuntimeWorker) setRuntimeTransportStateIfPolicyCurrent(observedRevision uint64, state RuntimeTransportState) bool {
	node.policyRecoveryMu.Lock()
	defer node.policyRecoveryMu.Unlock()
	if node.policyTerminalError != nil || node.policyRevision != observedRevision {
		return false
	}
	node.transportTransitionMu.Lock()
	defer node.transportTransitionMu.Unlock()
	node.transport.setState(state)
	return true
}

// lockRuntimeTransportTransitionAtPolicyRevision prevents a stale supervisor
// decision from overwriting the transport installed by a concurrent recovery.
// A false result means that recovery already consumed this generation.
func (node *RuntimeWorker) lockRuntimeTransportTransitionAtPolicyRevision(observedRevision uint64) (bool, error) {
	node.policyRecoveryMu.Lock()
	if node.policyTerminalError != nil {
		err := node.policyTerminalError
		node.policyRecoveryMu.Unlock()
		return false, err
	}
	if node.policyRevision != observedRevision {
		node.policyRecoveryMu.Unlock()
		return false, nil
	}
	node.transportTransitionMu.Lock()
	node.policyRecoveryMu.Unlock()
	return true, nil
}

func (node *RuntimeWorker) switchToPull(ctx context.Context, observedRevision uint64) error {
	locked, err := node.lockRuntimeTransportTransitionAtPolicyRevision(observedRevision)
	if err != nil || !locked {
		return err
	}
	defer node.transportTransitionMu.Unlock()
	epoch, _, previous := node.transport.beginTransition(RuntimeTransportSwitchingPull)
	node.closeTransport(previous, "transport_switch_to_pull")
	ready, err := node.attachPullWithRetry(ctx, true, node.runtimeFallbackReason(runtimeFallbackWebSocketToLongPoll))
	if err != nil {
		return err
	}
	if err = node.publishRuntimeTransport(epoch, RuntimeTransportPull, node.transport.callRuntimeClient()); err != nil {
		return err
	}
	node.stateMu.Lock()
	node.ready = ready
	node.stateMu.Unlock()
	node.signalSpool()
	node.logf("runtime transport active: pull")
	return nil
}

func (node *RuntimeWorker) switchToWebSocket(ctx context.Context, observedRevision uint64) error {
	locked, err := node.lockRuntimeTransportTransitionAtPolicyRevision(observedRevision)
	if err != nil || !locked {
		return err
	}
	defer node.transportTransitionMu.Unlock()
	epoch, _, previous := node.transport.beginTransition(RuntimeTransportSwitchingWS)
	node.closeTransport(previous, "transport_switch_to_ws")
	connection, ready, err := node.dialWebSocketOnce(ctx, node.runtimeFallbackReason(runtimeFallbackLongPollToWebSocket))
	if err == nil {
		err = node.resumeDurableStateWithClient(ctx, connection, true)
	}
	if err == nil {
		if err = node.publishRuntimeTransport(epoch, RuntimeTransportWebSocket, connection); err != nil {
			return err
		}
		node.stateMu.Lock()
		node.ready = ready
		node.stateMu.Unlock()
		node.signalSpool()
		node.logf("runtime transport active: ws")
		return nil
	}
	if connection != nil {
		node.closeTransport(connection, "transport_switch_failed")
	}
	if runtimeErrorIsPermanent(err) && !runtimeAttachErrorIsRetryable(err) {
		return err
	}
	// Pull was deliberately detached before the WS attach attempt. Restore it
	// before returning so the RuntimeWorker never remains transportless after a failed
	// recovery probe.
	ready, restoreErr := node.attachPullWithRetry(ctx, true, node.runtimeFallbackReason(runtimeFallbackFailedWSProbeRestoreLongPoll))
	if restoreErr != nil {
		return errors.Join(err, restoreErr)
	}
	if publishErr := node.publishRuntimeTransport(epoch, RuntimeTransportPull, node.transport.callRuntimeClient()); publishErr != nil {
		return errors.Join(err, publishErr)
	}
	node.stateMu.Lock()
	node.ready = ready
	node.stateMu.Unlock()
	node.signalSpool()
	return err
}

func (node *RuntimeWorker) reconnectWebSocket(ctx context.Context, observedRevision uint64) error {
	locked, err := node.lockRuntimeTransportTransitionAtPolicyRevision(observedRevision)
	if err != nil || !locked {
		return err
	}
	defer node.transportTransitionMu.Unlock()
	epoch, _, previous := node.transport.beginTransition(RuntimeTransportConnectingWS)
	node.closeTransport(previous, "websocket_reconnect")
	returnValue, err := node.activateWebSocketWithRetry(ctx, true, epoch, node.runtimeFallbackReason(runtimeFallbackSameTransportReconnect))
	if err == nil {
		node.stateMu.Lock()
		node.ready = returnValue
		node.stateMu.Unlock()
	}
	return err
}

func (node *RuntimeWorker) runtimeFallbackReason(transition runtimeFallbackTransition) runtimeFallbackReason {
	switch transition {
	case runtimeFallbackWebSocketToLongPoll, runtimeFallbackFailedWSProbeRestoreLongPoll:
		return runtimeFallbackWebSocketUnavailable
	case runtimeFallbackLongPollToWebSocket:
		return runtimeFallbackRecovery
	case runtimeFallbackPolicySelected, runtimeFallbackSameTransportReconnect, runtimeFallbackPolicyRediscovery:
		if RuntimeTransportMode(node.Transport) == RuntimeTransportAuto {
			return runtimeFallbackPolicyForced
		}
		return runtimeFallbackExplicit
	default:
		return ""
	}
}

func (node *RuntimeWorker) activatePullWithRetry(parent context.Context, reconnect bool, epoch uint64, reason runtimeFallbackReason) (*RuntimeReadyPayload, error) {
	ready, err := node.attachPullWithRetry(parent, reconnect, reason)
	if err != nil {
		return nil, err
	}
	if err = node.publishRuntimeTransport(epoch, RuntimeTransportPull, node.transport.callRuntimeClient()); err != nil {
		return nil, err
	}
	node.logf("runtime transport active: pull")
	return ready, nil
}

func (node *RuntimeWorker) attachPullWithRetry(parent context.Context, reconnect bool, reason runtimeFallbackReason) (*RuntimeReadyPayload, error) {
	ready, err := node.createSessionWithRetryClient(withRuntimeFallbackReason(parent, reason), node.transport.callRuntimeClient())
	if err != nil {
		return nil, err
	}
	if reconnect {
		if err = node.resumeDurableStateWithClient(parent, node.transport.callRuntimeClient(), true); err != nil {
			return nil, err
		}
	}
	return ready, nil
}

func (node *RuntimeWorker) activateWebSocketWithRetry(parent context.Context, reconnect bool, epoch uint64, reason runtimeFallbackReason) (*RuntimeReadyPayload, error) {
	for attempt := 0; ; attempt++ {
		connection, ready, err := node.dialWebSocketOnce(parent, reason)
		if err == nil && reconnect {
			err = node.resumeDurableStateWithClient(parent, connection, true)
		}
		if err == nil {
			if err = node.publishRuntimeTransport(epoch, RuntimeTransportWebSocket, connection); err != nil {
				return nil, err
			}
			node.signalSpool()
			node.logf("runtime transport active: ws")
			return ready, nil
		}
		if connection != nil {
			node.closeTransport(connection, "websocket_attach_failed")
		}
		if runtimeErrorIsPermanent(err) && !runtimeAttachErrorIsRetryable(err) {
			return nil, err
		}
		if waitErr := node.waitRetry(parent, attempt); waitErr != nil {
			return nil, waitErr
		}
	}
}

func (node *RuntimeWorker) dialWebSocketOnce(parent context.Context, reason runtimeFallbackReason) (RuntimeDuplexClient, *RuntimeReadyPayload, error) {
	if node.runtimeDialer == nil {
		return nil, nil, errors.New("runtime WebSocket dialer is unavailable")
	}
	callCtx, cancel := context.WithTimeout(withRuntimeFallbackReason(parent, reason), 20*time.Second)
	connection, err := node.runtimeDialer.DialRuntimeWebSocket(callCtx, node.runtimeHello())
	cancel()
	if err != nil {
		return nil, nil, err
	}
	ready, err := connection.CreateRuntimeSession(parent, node.runtimeHello())
	if err != nil || ready == nil {
		if err == nil {
			err = fmt.Errorf("%w: WebSocket ready response", ErrRuntimeProtocolMismatch)
		}
		return connection, nil, err
	}
	return connection, ready, nil
}

func (node *RuntimeWorker) closeTransport(client RuntimeClient, reason string) {
	if client == nil || node.store == nil {
		return
	}
	identity := node.store.Identity()
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	_ = client.CloseRuntimeSession(ctx, RuntimeSessionCloseRequest{
		NodeID: node.NodeID, AgentID: node.AgentID, WorkerID: identity.WorkerID,
		RuntimeSessionID: identity.RuntimeSessionID, SessionEpoch: identity.SessionEpoch,
		Status: "offline", Reason: reason,
	})
	cancel()
}

func (node *RuntimeWorker) publishRuntimeTransport(epoch uint64, kind RuntimeTransportMode, client RuntimeClient) error {
	if node.transport.activateIfCurrent(epoch, kind, client) {
		return nil
	}
	node.closeTransport(client, "transport_transition_superseded")
	return errRuntimeTransportTransitionSuperseded
}
