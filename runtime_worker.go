package openlinker

import (
	"context"
	"errors"
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"
	"time"
)

const (
	RuntimeWorkerDefaultShutdownTimeout         = 10 * time.Second
	RuntimeWorkerDefaultCapacity          int64 = 1
	RuntimeWorkerDefaultClaimWait               = 25 * time.Second
	RuntimeWorkerDefaultCommandWait             = 25 * time.Second
	RuntimeWorkerDefaultHeartbeatInterval       = 5 * time.Second
	RuntimeWorkerDefaultRetryMinimum            = 250 * time.Millisecond
	RuntimeWorkerDefaultRetryMaximum            = 15 * time.Second
)

// RuntimeWorker runs one reliable Runtime session. A worker is single-use;
// construct a new worker after Start returns.
type RuntimeWorker struct {
	PlatformURL string
	RuntimeURL  string
	Transport   RuntimeTransportMode
	NodeID      string
	NodeVersion string
	AgentID     string
	AgentToken  string
	DataDir     string
	MTLS        RuntimeMTLSConfig

	Capacity          int64
	ClaimWait         time.Duration
	CommandWait       time.Duration
	HeartbeatInterval time.Duration
	RetryMinimum      time.Duration
	RetryMaximum      time.Duration

	Handler RuntimeHandler
	Store   RuntimeStore
	Logger  *log.Logger
	OnReady func(RuntimeReadyPayload)

	runtimeClient     RuntimeClient
	runtimeDialer     RuntimeTransportDialer
	credentialManager *runtimeCredentialManager

	lifecycleMu            sync.Mutex
	started                bool
	completed              bool
	done                   chan struct{}
	runtimeCtx             context.Context
	runtimeStop            context.CancelFunc
	httpClient             *http.Client
	transport              *switchingRuntimeClient
	transportStop          context.CancelFunc
	transportTransitionMu  sync.Mutex
	store                  RuntimeStore
	ready                  *RuntimeReadyPayload
	transportPolicyMu      sync.RWMutex
	transportOrder         []RuntimeTransportMode
	sessionStaleAfter      time.Duration
	webSocketProbeInterval time.Duration
	webSocketProbeTimeout  time.Duration
	policyHeartbeat        time.Duration
	policyRetryMinimum     time.Duration
	policyRetryMaximum     time.Duration
	policySessionStale     time.Duration
	policyProbeInterval    time.Duration
	policyProbeTimeout     time.Duration
	policyRecoveryMu       sync.Mutex
	policyRevision         uint64
	policyLastObserved     uint64
	policyLastReady        *RuntimeReadyPayload
	policyLastError        error
	policyTerminalError    error
	initialResumeComplete  bool
	runtimeDiscovery       func(context.Context, string) (runtimeConnectionInformation, error)

	stateMu       sync.RWMutex
	draining      bool
	stopping      bool
	active        map[string]*activeRuntimeAttempt
	reservations  map[string]struct{}
	assignmentOps int
	cancellations map[string]struct{}
	spoolAllowed  map[string]spoolPermission
	wakeSpool     chan struct{}
	fatal         chan error
	stopRequest   chan struct{}
	stopRequested bool
	drainOwnsStop bool
	loops         sync.WaitGroup
	executions    sync.WaitGroup

	drainMu   sync.Mutex
	drainDone chan struct{}
	drainErr  error

	// Tests use this barrier to prove the final Stop/Drain linearization. It is
	// nil in production and deliberately not part of the public API.
	drainBeforeStop func()

	jitter func(time.Duration) time.Duration
}

func (node *RuntimeWorker) Start(parent context.Context) (retErr error) {
	if parent == nil {
		parent = context.Background()
	}
	if err := node.beginLifecycle(); err != nil {
		return err
	}
	defer func() {
		shutdownCtx, cancel := context.WithTimeout(context.Background(), RuntimeWorkerDefaultShutdownTimeout)
		defer cancel()
		shutdownErr := node.shutdown(shutdownCtx)
		if retErr == nil && shutdownErr != nil {
			retErr = shutdownErr
		}
	}()
	if err := node.applyDefaultsAndValidate(); err != nil {
		return err
	}
	connection := runtimeConnectionInformation{MTLSRequired: true}
	if node.runtimeClient == nil {
		var err error
		connection, err = resolveRuntimeConnection(parent, node.PlatformURL, node.RuntimeURL)
		if err != nil {
			return err
		}
		node.RuntimeURL = connection.RuntimeURL
		if err = node.applyRuntimeTransportPolicy(connection.Policy); err != nil {
			return err
		}
	}
	startupCtx, cancelStartup := context.WithCancel(parent)
	defer cancelStartup()
	go func() {
		select {
		case <-node.stopRequest:
			cancelStartup()
		case <-startupCtx.Done():
		}
	}()

	var err error
	if node.Store == nil {
		store, err := OpenFileRuntimeStore(node.DataDir)
		if err != nil {
			return err
		}
		node.Store = store
	}
	node.store = node.Store

	if node.runtimeClient == nil {
		explicitMTLS := node.MTLS.CertFile != "" && node.MTLS.KeyFile != "" && node.MTLS.CAFile != ""
		if !explicitMTLS || !connection.MTLSRequired {
			credentialEndpoint := connection.CredentialEndpoint
			if credentialEndpoint == "" && node.PlatformURL != "" {
				platformOrigin, platformErr := validatePlatformOrigin(node.PlatformURL)
				if platformErr != nil {
					return platformErr
				}
				credentialEndpoint = platformOrigin + "/api/v1/runtime-credentials"
			}
			manager, managerErr := newRuntimeCredentialManager(
				node.DataDir, credentialEndpoint, node.AgentToken, node.NodeID, node.AgentID,
				node.NodeVersion, node.Capacity, node.Logger,
			)
			if managerErr != nil {
				return managerErr
			}
			if managerErr = manager.Ensure(startupCtx, false); managerErr != nil {
				return managerErr
			}
			node.NodeID, node.AgentID = manager.Identity()
			node.credentialManager = manager
			node.MTLS.credentialManager = manager
			node.MTLS.Disabled = !connection.MTLSRequired
			if connection.MTLSRequired {
				node.MTLS.tlsConfig, managerErr = manager.TLSConfig()
				if managerErr != nil {
					return managerErr
				}
			}
			manager.Start(startupCtx)
		} else {
			node.MTLS.Disabled = false
		}
		runtimeClient, httpClient, err := newRuntimeClient(node.RuntimeURL, node.AgentToken, node.MTLS)
		if err != nil {
			return err
		}
		node.runtimeClient = runtimeClient
		node.runtimeDialer = &sdkRuntimeTransportDialer{runtime: runtimeClient, credentials: node.credentialManager}
		node.httpClient = httpClient
	}
	if node.runtimeDialer != nil {
		transport := newSwitchingRuntimeClient(node.runtimeClient)
		node.lifecycleMu.Lock()
		node.transport = transport
		node.lifecycleMu.Unlock()
		node.runtimeClient = &policyRecoveringRuntimeClient{node: node, transport: transport}
	}

	var ready *RuntimeReadyPayload
	if node.transport != nil {
		ready, err = node.startInitialRuntimeTransport(startupCtx)
		if runtimePolicyRecoverySignal(err) {
			ready, err = node.recoverRuntimePolicy(startupCtx, node.currentPolicyRevision())
		}
	} else {
		ready, err = node.createSessionWithRetry(startupCtx)
	}
	if err != nil {
		return err
	}
	node.stateMu.Lock()
	node.ready = ready
	node.stateMu.Unlock()
	if err := node.resumeDurableState(startupCtx); err != nil {
		return err
	}
	node.policyRecoveryMu.Lock()
	node.initialResumeComplete = true
	node.policyRecoveryMu.Unlock()
	if node.OnReady != nil {
		node.OnReady(*ready)
	}

	node.startTransportSupervisor()
	node.startRuntimeLoops()
	select {
	case <-parent.Done():
		return nil
	case <-node.stopRequest:
		return nil
	case err := <-node.fatal:
		return err
	}
}

// Run is the facade-friendly alias for Start.
func (node *RuntimeWorker) Run(ctx context.Context) error { return node.Start(ctx) }

func (node *RuntimeWorker) Stop(ctx context.Context) error {
	if ctx == nil {
		ctx = context.Background()
	}
	node.lifecycleMu.Lock()
	if !node.started {
		node.lifecycleMu.Unlock()
		return nil
	}
	done := node.done
	node.lifecycleMu.Unlock()
	node.requestStop()

	select {
	case <-done:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}

func (node *RuntimeWorker) beginLifecycle() error {
	node.lifecycleMu.Lock()
	defer node.lifecycleMu.Unlock()
	if node.started {
		return errors.New("runtime worker is already started")
	}
	if node.completed {
		return errors.New("runtime worker cannot be restarted")
	}
	node.started = true
	node.done = make(chan struct{})
	node.runtimeCtx, node.runtimeStop = context.WithCancel(context.Background())
	node.active = make(map[string]*activeRuntimeAttempt)
	node.reservations = make(map[string]struct{})
	node.assignmentOps = 0
	node.cancellations = make(map[string]struct{})
	node.spoolAllowed = make(map[string]spoolPermission)
	node.wakeSpool = make(chan struct{}, 1)
	node.fatal = make(chan error, 1)
	node.stopRequest = make(chan struct{})
	node.stopRequested = false
	node.drainOwnsStop = false
	node.draining = false
	node.stopping = false
	node.ready = nil
	node.drainDone = nil
	node.drainErr = nil
	return nil
}

func (node *RuntimeWorker) applyDefaultsAndValidate() error {
	node.PlatformURL = strings.TrimSpace(node.PlatformURL)
	node.RuntimeURL = strings.TrimSpace(node.RuntimeURL)
	node.NodeVersion = strings.TrimSpace(node.NodeVersion)
	if node.NodeVersion == "" {
		node.NodeVersion = runtimeWorkerSDKAgent
	}
	if node.runtimeClient == nil {
		if node.RuntimeURL != "" {
			if _, err := validateRuntimeOrigin(node.RuntimeURL); err != nil {
				return err
			}
		} else {
			if node.PlatformURL == "" {
				return errors.New("OpenLinker address is required")
			}
			if _, err := validatePlatformOrigin(node.PlatformURL); err != nil {
				return err
			}
		}
	}
	if node.Transport == "" {
		node.Transport = RuntimeTransportAuto
	}
	switch RuntimeTransportMode(strings.ToLower(strings.TrimSpace(string(node.Transport)))) {
	case RuntimeTransportAuto, RuntimeTransportWebSocket, RuntimeTransportPull:
		node.Transport = RuntimeTransportMode(strings.ToLower(strings.TrimSpace(string(node.Transport))))
	default:
		return errors.New("transport must be auto, ws, or pull")
	}
	explicitMTLS := node.MTLS.CertFile != "" || node.MTLS.KeyFile != "" || node.MTLS.CAFile != ""
	if explicitMTLS && (node.MTLS.CertFile == "" || node.MTLS.KeyFile == "" || node.MTLS.CAFile == "") {
		return errors.New("runtime mTLS cert, key, and CA files must be configured together")
	}
	if node.NodeID != "" && !validRuntimeUUID(node.NodeID) {
		return errors.New("RuntimeWorker ID must be a non-zero lowercase UUID")
	}
	if node.AgentID != "" && !validRuntimeUUID(node.AgentID) {
		return errors.New("Agent ID must be a non-zero lowercase UUID")
	}
	if explicitMTLS && (!validRuntimeUUID(node.NodeID) || !validRuntimeUUID(node.AgentID)) {
		return errors.New("RuntimeWorker ID and Agent ID are required with explicit mTLS files")
	}
	if node.AgentToken == "" && node.runtimeClient == nil {
		return errors.New("Agent Token is required")
	}
	if node.Store == nil && node.DataDir == "" {
		return errors.New("runtime data directory is required")
	}
	if node.Handler == nil {
		return errors.New("runtime handler is required")
	}
	if node.Capacity == 0 {
		node.Capacity = RuntimeWorkerDefaultCapacity
	}
	if node.Capacity < 1 || node.Capacity > RuntimeMaxNodeCapacity {
		return fmt.Errorf("capacity must be between 1 and %d", RuntimeMaxNodeCapacity)
	}
	if node.ClaimWait <= 0 {
		node.ClaimWait = RuntimeWorkerDefaultClaimWait
	}
	if node.CommandWait <= 0 {
		node.CommandWait = RuntimeWorkerDefaultCommandWait
	}
	if node.HeartbeatInterval <= 0 {
		node.HeartbeatInterval = RuntimeWorkerDefaultHeartbeatInterval
	}
	if node.RetryMinimum <= 0 {
		node.RetryMinimum = RuntimeWorkerDefaultRetryMinimum
	}
	if node.RetryMaximum <= 0 {
		node.RetryMaximum = RuntimeWorkerDefaultRetryMaximum
	}
	if node.RetryMaximum < node.RetryMinimum {
		return errors.New("retry maximum must not be less than retry minimum")
	}
	if node.webSocketProbeTimeout <= 0 {
		node.webSocketProbeTimeout = 10 * time.Second
	}
	return nil
}

func (node *RuntimeWorker) startRuntimeLoops() {
	for _, loop := range []func(){node.claimLoop, node.commandLoop, node.heartbeatLoop, node.spoolLoop} {
		node.loops.Add(1)
		go func(run func()) {
			defer node.loops.Done()
			run()
		}(loop)
	}
}

func (node *RuntimeWorker) shutdown(ctx context.Context) error {
	node.requestStop()
	if node.transportStop != nil {
		node.transportStop()
	}
	heartbeatCtx, cancelHeartbeat := context.WithTimeout(ctx, 2*time.Second)
	_ = node.heartbeatOnce(heartbeatCtx)
	cancelHeartbeat()

	executionsDone := make(chan struct{})
	go func() {
		node.executions.Wait()
		close(executionsDone)
	}()
	select {
	case <-executionsDone:
	case <-ctx.Done():
		node.cancelAllActive()
		forceTimer := time.NewTimer(2 * time.Second)
		select {
		case <-executionsDone:
			forceTimer.Stop()
		case <-forceTimer.C:
		}
	}

	if node.store != nil && node.runtimeClient != nil {
		identity := node.store.Identity()
		closeClient := node.runtimeClient
		if node.transport != nil {
			_, closeClient = node.transport.stop()
		}
		if closeClient != nil {
			_ = closeClient.CloseRuntimeSession(ctx, RuntimeSessionCloseRequest{
				NodeID:           node.NodeID,
				AgentID:          node.AgentID,
				WorkerID:         identity.WorkerID,
				RuntimeSessionID: identity.RuntimeSessionID,
				SessionEpoch:     identity.SessionEpoch,
				Status:           "closed",
				Reason:           "node_shutdown",
			})
		}
	}
	node.cancelAllActive()
	if node.runtimeStop != nil {
		node.runtimeStop()
	}
	node.loops.Wait()

	var firstErr error
	if node.store != nil {
		if err := node.store.Close(); firstErr == nil {
			firstErr = err
		}
		node.store = nil
	}
	if node.httpClient != nil {
		if transport, ok := node.httpClient.Transport.(interface{ CloseIdleConnections() }); ok {
			transport.CloseIdleConnections()
		}
	}

	node.lifecycleMu.Lock()
	if node.started {
		node.started = false
		node.completed = true
		close(node.done)
	}
	node.transport = nil
	node.transportStop = nil
	node.lifecycleMu.Unlock()
	return firstErr
}

func (node *RuntimeWorker) setDraining(value bool) {
	node.stateMu.Lock()
	node.draining = value
	node.stateMu.Unlock()
}

func (node *RuntimeWorker) setStopping() {
	node.stateMu.Lock()
	node.draining = true
	node.stopping = true
	node.stateMu.Unlock()
}

func (node *RuntimeWorker) requestStop() {
	node.lifecycleMu.Lock()
	node.setStopping()
	if node.started && !node.stopRequested {
		close(node.stopRequest)
		node.stopRequested = true
		node.drainOwnsStop = false
	}
	node.lifecycleMu.Unlock()
}

func (node *RuntimeWorker) requestDrainStop() bool {
	node.lifecycleMu.Lock()
	defer node.lifecycleMu.Unlock()
	if !node.started || node.completed || node.stopRequested {
		return false
	}
	node.setStopping()
	close(node.stopRequest)
	node.stopRequested = true
	node.drainOwnsStop = true
	return true
}

func (node *RuntimeWorker) isDraining() bool {
	node.stateMu.RLock()
	defer node.stateMu.RUnlock()
	return node.draining
}

func (node *RuntimeWorker) capacitySnapshot() (capacity, inflight int64) {
	node.stateMu.RLock()
	draining := node.draining
	inflight = int64(len(node.active))
	for attemptID := range node.reservations {
		if node.active[attemptID] == nil {
			inflight++
		}
	}
	node.stateMu.RUnlock()
	accepting := node.store == nil || node.store.AcceptsNewRuns()
	if !draining && accepting {
		capacity = node.Capacity
	}
	return capacity, inflight
}

func (node *RuntimeWorker) signalSpool() {
	select {
	case node.wakeSpool <- struct{}{}:
	default:
	}
}

func (node *RuntimeWorker) reportFatal(err error) {
	if err == nil {
		return
	}
	select {
	case node.fatal <- err:
	default:
	}
}

func (node *RuntimeWorker) logf(format string, args ...any) {
	if node.Logger != nil {
		node.Logger.Printf(format, args...)
		return
	}
	log.Printf(format, args...)
}

func sleepContext(ctx context.Context, duration time.Duration) error {
	timer := time.NewTimer(duration)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
		return nil
	}
}
