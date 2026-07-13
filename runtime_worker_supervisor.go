package openlinker

import (
	"context"
	"errors"
	"fmt"
	"time"
)

func (node *RuntimeWorker) startInitialRuntimeTransport(parent context.Context) (*RuntimeReadyPayload, error) {
	mode := RuntimeTransportMode(node.Transport)
	switch mode {
	case RuntimeTransportPull:
		return node.activatePullWithRetry(parent, false)
	case RuntimeTransportWebSocket:
		return node.activateWebSocketWithRetry(parent, false)
	case RuntimeTransportAuto:
		node.transport.setState(RuntimeTransportConnectingWS)
		connection, ready, err := node.dialWebSocketOnce(parent)
		if err == nil {
			node.transport.activate(RuntimeTransportWebSocket, connection)
			node.logf("runtime transport active: ws")
			return ready, nil
		}
		if runtimeErrorIsPermanent(err) {
			return nil, scrubRuntimeError(err)
		}
		node.logf("runtime WebSocket unavailable; activating HTTPS long-poll: %v", scrubRuntimeError(err))
		return node.activatePullWithRetry(parent, false)
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
		kind, _, active := node.transport.snapshot()
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
			node.logf("runtime WebSocket disconnected: %v", scrubRuntimeError(connection.Err()))
			var err error
			if RuntimeTransportMode(node.Transport) == RuntimeTransportWebSocket {
				err = node.reconnectWebSocket(ctx)
			} else {
				err = node.switchToPull(ctx)
			}
			if err != nil && ctx.Err() == nil {
				node.reportFatal(scrubRuntimeError(err))
				return
			}
			probeAttempt = 0
		case RuntimeTransportPull:
			delay := node.retryDelay(probeAttempt)
			if node.jitter != nil {
				delay = node.jitter(delay)
			} else {
				delay = jitterDuration(delay)
			}
			if sleepContext(ctx, delay) != nil {
				return
			}
			node.transport.setState(RuntimeTransportProbingWS)
			probeCtx, cancel := context.WithTimeout(ctx, 10*time.Second)
			err := node.runtimeDialer.ProbeRuntimeWebSocket(probeCtx)
			cancel()
			if err != nil {
				node.transport.setState(RuntimeTransportPullActive)
				if runtimeErrorIsPermanent(err) {
					node.reportFatal(scrubRuntimeError(err))
					return
				}
				probeAttempt++
				continue
			}
			if err = node.switchToWebSocket(ctx); err != nil {
				if ctx.Err() != nil {
					return
				}
				if runtimeErrorIsPermanent(err) {
					node.reportFatal(scrubRuntimeError(err))
					return
				}
				node.logf("runtime WebSocket recovery deferred: %v", scrubRuntimeError(err))
				probeAttempt++
				continue
			}
			probeAttempt = 0
		default:
			if sleepContext(ctx, node.RetryMinimum) != nil {
				return
			}
		}
	}
}

func (node *RuntimeWorker) switchToPull(ctx context.Context) error {
	_, previous := node.transport.beginTransition(RuntimeTransportSwitchingPull)
	node.closeTransport(previous, "transport_switch_to_pull")
	ready, err := node.attachPullWithRetry(ctx, true)
	if err != nil {
		return err
	}
	node.stateMu.Lock()
	node.ready = ready
	node.stateMu.Unlock()
	node.transport.activate(RuntimeTransportPull, node.transport.callClient)
	node.signalSpool()
	node.logf("runtime transport active: pull")
	return nil
}

func (node *RuntimeWorker) switchToWebSocket(ctx context.Context) error {
	_, previous := node.transport.beginTransition(RuntimeTransportSwitchingWS)
	node.closeTransport(previous, "transport_switch_to_ws")
	connection, ready, err := node.dialWebSocketOnce(ctx)
	if err == nil {
		err = node.resumeDurableStateWithClient(ctx, connection, true)
	}
	if err == nil {
		node.stateMu.Lock()
		node.ready = ready
		node.stateMu.Unlock()
		node.transport.activate(RuntimeTransportWebSocket, connection)
		node.signalSpool()
		node.logf("runtime transport active: ws")
		return nil
	}
	if connection != nil {
		node.closeTransport(connection, "transport_switch_failed")
	}
	if runtimeErrorIsPermanent(err) {
		return err
	}
	// Pull was deliberately detached before the WS attach attempt. Restore it
	// before returning so the RuntimeWorker never remains transportless after a failed
	// recovery probe.
	ready, restoreErr := node.attachPullWithRetry(ctx, true)
	if restoreErr != nil {
		return errors.Join(err, restoreErr)
	}
	node.stateMu.Lock()
	node.ready = ready
	node.stateMu.Unlock()
	node.transport.activate(RuntimeTransportPull, node.transport.callClient)
	node.signalSpool()
	return err
}

func (node *RuntimeWorker) reconnectWebSocket(ctx context.Context) error {
	_, previous := node.transport.beginTransition(RuntimeTransportConnectingWS)
	node.closeTransport(previous, "websocket_reconnect")
	returnValue, err := node.activateWebSocketWithRetry(ctx, true)
	if err == nil {
		node.stateMu.Lock()
		node.ready = returnValue
		node.stateMu.Unlock()
	}
	return err
}

func (node *RuntimeWorker) activatePullWithRetry(parent context.Context, reconnect bool) (*RuntimeReadyPayload, error) {
	ready, err := node.attachPullWithRetry(parent, reconnect)
	if err != nil {
		return nil, err
	}
	node.transport.activate(RuntimeTransportPull, node.transport.callClient)
	node.logf("runtime transport active: pull")
	return ready, nil
}

func (node *RuntimeWorker) attachPullWithRetry(parent context.Context, reconnect bool) (*RuntimeReadyPayload, error) {
	ready, err := node.createSessionWithRetryClient(parent, node.transport.callClient)
	if err != nil {
		return nil, err
	}
	if reconnect {
		if err = node.resumeDurableStateWithClient(parent, node.transport.callClient, true); err != nil {
			return nil, err
		}
	}
	return ready, nil
}

func (node *RuntimeWorker) activateWebSocketWithRetry(parent context.Context, reconnect bool) (*RuntimeReadyPayload, error) {
	for attempt := 0; ; attempt++ {
		connection, ready, err := node.dialWebSocketOnce(parent)
		if err == nil && reconnect {
			err = node.resumeDurableStateWithClient(parent, connection, true)
		}
		if err == nil {
			node.transport.activate(RuntimeTransportWebSocket, connection)
			node.signalSpool()
			node.logf("runtime transport active: ws")
			return ready, nil
		}
		if connection != nil {
			node.closeTransport(connection, "websocket_attach_failed")
		}
		if runtimeErrorIsPermanent(err) {
			return nil, err
		}
		if waitErr := node.waitRetry(parent, attempt); waitErr != nil {
			return nil, waitErr
		}
	}
}

func (node *RuntimeWorker) dialWebSocketOnce(parent context.Context) (RuntimeDuplexClient, *RuntimeReadyPayload, error) {
	if node.runtimeDialer == nil {
		return nil, nil, errors.New("runtime WebSocket dialer is unavailable")
	}
	callCtx, cancel := context.WithTimeout(parent, 20*time.Second)
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
