package openlinker

import (
	"errors"
	"fmt"
	"sync"
	"time"
)

// runtimeCapacityReattachMinInterval bounds capacity-triggered WebSocket
// reattaches so spool usage oscillating around the backpressure threshold
// cannot cause a reconnect storm.
const runtimeCapacityReattachMinInterval = 30 * time.Second

// runtimeWebSocketCapacityChangedError reports that the WebSocket Session was
// attached with another capacity. Core reads WebSocket capacity only from the
// attach hello, so the current value takes effect on the next attach.
type runtimeWebSocketCapacityChangedError struct {
	Attached int64
	Current  int64
}

func (err *runtimeWebSocketCapacityChangedError) Error() string {
	return fmt.Sprintf(
		"openlinker: runtime WebSocket was attached with capacity %d; current capacity %d applies on the next attach",
		err.Attached, err.Current,
	)
}

// runtimeWebSocketCapacityState remembers the hello Core received when the
// current WebSocket attached, so the Worker can tell when that attach still
// advertises zero capacity after local admission has reopened.
type runtimeWebSocketCapacityState struct {
	mu sync.Mutex
	// client is the connection that attached with hello.
	client RuntimeClient
	hello  RuntimeHelloPayload
	// restoredSeen is true after one heartbeat check saw restored capacity on
	// this connection; a reattach needs two consecutive observations.
	restoredSeen bool
	lastReattach time.Time
}

func runtimeHelloEqualExceptCapacity(left, right RuntimeHelloPayload) bool {
	left.Capacity, right.Capacity = 0, 0
	return runtimeHelloEqual(left, right)
}

// recordWebSocketAttachment stores the exact hello sent while dialing client.
func (node *RuntimeWorker) recordWebSocketAttachment(client RuntimeClient, hello RuntimeHelloPayload) {
	state := &node.webSocketCapacity
	state.mu.Lock()
	state.client = client
	state.hello = hello
	state.hello.Features = append([]string(nil), hello.Features...)
	state.restoredSeen = false
	state.mu.Unlock()
}

// webSocketCapacityReattachTarget returns the active WebSocket that should be
// reattached because it was attached with capacity 0 while current, the hello
// the Worker would send now, advertises capacity again. It returns nil for any
// identity difference other than capacity, before restored capacity was seen
// on two consecutive checks, and within runtimeCapacityReattachMinInterval of
// the previous capacity reattach. It never takes stateMu.
func (node *RuntimeWorker) webSocketCapacityReattachTarget(now time.Time, current RuntimeHelloPayload) RuntimeDuplexClient {
	transport := node.transport
	if transport == nil {
		return nil
	}
	kind, transportState, active := transport.snapshot()
	connection, duplex := active.(RuntimeDuplexClient)
	state := &node.webSocketCapacity
	state.mu.Lock()
	defer state.mu.Unlock()
	if kind != RuntimeTransportWebSocket || transportState != RuntimeTransportWebSocketActive || !duplex ||
		state.client != active || state.hello.Capacity != 0 || current.Capacity <= 0 ||
		!runtimeHelloEqualExceptCapacity(state.hello, current) {
		state.restoredSeen = false
		return nil
	}
	if !state.restoredSeen {
		state.restoredSeen = true
		return nil
	}
	if !state.lastReattach.IsZero() && now.Sub(state.lastReattach) < runtimeCapacityReattachMinInterval {
		return nil
	}
	state.lastReattach = now
	state.restoredSeen = false
	return connection
}

// reattachWebSocketIfCapacityRestored closes a WebSocket that still advertises
// zero capacity after local admission reopened. Closing hands the connection
// to the existing transport supervisor exactly like a network disconnect, so
// durable resume, lease, spool and Result/Event delivery follow the same,
// already-tested reconnect or fallback path. No protocol message is added.
func (node *RuntimeWorker) reattachWebSocketIfCapacityRestored(now time.Time) bool {
	connection, capacity := node.retireWebSocketForCapacity(now)
	if connection == nil {
		return false
	}
	node.logf(
		"runtime WebSocket was attached with capacity 0 during local spool backpressure; capacity is now %d, reattaching so OpenLinker can dispatch work",
		capacity,
	)
	// The network close can block on a stalled writer, so it runs without any
	// Worker lock: Drain and Stop are never delayed by it.
	node.closeTransport(connection, "capacity_restored")
	select {
	case <-connection.Done():
	default:
		// Defensive: a client whose close did not disconnect it stays usable.
		node.transport.restoreActive(connection)
	}
	return true
}

// retireWebSocketForCapacity makes the capacity decision and logically closes
// the chosen WebSocket in one step that is atomic with admission fencing.
//
// Drain, Stop and a Core drain command fence admission by taking stateMu
// exclusively. Store snapshots (admission and identity) take the store mutex,
// which spool writes hold across fsync, and admission also calls Statfs, so
// they are read before stateMu is taken. Under stateMu shared, the Worker only
// re-checks draining/stopping and marks the socket closing in memory: after
// that the transport refuses new operations on it, as during any transition.
// So either a fence came first (nothing is retired), or every operation after
// the fence, including a Drain request, already sees the socket as closed and
// waits for its replacement. The network close happens later, unlocked.
func (node *RuntimeWorker) retireWebSocketForCapacity(now time.Time) (RuntimeDuplexClient, int64) {
	store, transport := node.store, node.transport
	if store == nil || transport == nil {
		return nil, 0
	}
	// Same admission rule as capacitySnapshot; draining is re-checked below.
	capacity := int64(0)
	if store.AcceptsNewRuns() {
		capacity = node.Capacity
	}
	current := node.runtimeHelloWithCapacity(capacity)

	node.stateMu.RLock()
	defer node.stateMu.RUnlock()
	if node.draining || node.stopping {
		return nil, 0
	}
	connection := node.webSocketCapacityReattachTarget(now, current)
	if connection == nil || !transport.retireActive(connection) {
		return nil, 0
	}
	if node.capacityReattachRetired != nil {
		node.capacityReattachRetired()
	}
	return connection, capacity
}

// logHeartbeatRetry keeps heartbeat retry diagnostics without logging the
// expected capacity difference while a drain has already informed Core.
func (node *RuntimeWorker) logHeartbeatRetry(err error) {
	var capacityErr *runtimeWebSocketCapacityChangedError
	if errors.As(err, &capacityErr) && node.isDraining() {
		return
	}
	node.logRuntimeRetry("heartbeat", err)
}
