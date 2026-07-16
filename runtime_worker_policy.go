package openlinker

import (
	"errors"
	"fmt"
	"strings"
	"time"
)

const runtimeTransportPolicyVersion = 1

type runtimeTransportPolicy struct {
	Allowed                   []RuntimeTransportMode
	Default                   RuntimeTransportMode
	HeartbeatInterval         time.Duration
	HeartbeatIntervalSet      bool
	SessionStaleAfter         time.Duration
	SessionStaleAfterSet      bool
	RetryMinimum              time.Duration
	RetryMinimumSet           bool
	RetryMaximum              time.Duration
	RetryMaximumSet           bool
	WebSocketProbeInterval    time.Duration
	WebSocketProbeIntervalSet bool
	WebSocketProbeTimeout     time.Duration
	WebSocketProbeTimeoutSet  bool
}

func legacyRuntimeTransportPolicy() runtimeTransportPolicy {
	return runtimeTransportPolicy{
		Allowed:                []RuntimeTransportMode{RuntimeTransportWebSocket, RuntimeTransportPull},
		Default:                RuntimeTransportAuto,
		HeartbeatInterval:      RuntimeWorkerDefaultHeartbeatInterval,
		RetryMinimum:           RuntimeWorkerDefaultRetryMinimum,
		RetryMaximum:           RuntimeWorkerDefaultRetryMaximum,
		WebSocketProbeInterval: 15 * time.Second,
		WebSocketProbeTimeout:  10 * time.Second,
	}
}

func runtimeTransportPolicyFromManifest(
	transports []string,
	defaultTransport *string,
	manifest *openLinkerManifestTransportPolicy,
) (runtimeTransportPolicy, error) {
	policy := legacyRuntimeTransportPolicy()
	if transports != nil {
		policy.Allowed = nil
		seen := make(map[RuntimeTransportMode]struct{}, len(transports))
		for _, value := range transports {
			mode, ok := runtimeManifestTransportMode(value)
			if !ok {
				continue
			}
			if _, duplicate := seen[mode]; duplicate {
				continue
			}
			seen[mode] = struct{}{}
			policy.Allowed = append(policy.Allowed, mode)
		}
		if len(policy.Allowed) == 0 {
			return runtimeTransportPolicy{}, errors.New("OpenLinker Runtime does not allow a transport supported by this SDK")
		}
	}
	if defaultTransport != nil {
		mode, ok := runtimeManifestTransportMode(*defaultTransport)
		if strings.EqualFold(strings.TrimSpace(*defaultTransport), string(RuntimeTransportAuto)) {
			mode, ok = RuntimeTransportAuto, true
		}
		if !ok {
			return runtimeTransportPolicy{}, fmt.Errorf("OpenLinker Runtime default transport %q is unsupported", strings.TrimSpace(*defaultTransport))
		}
		policy.Default = mode
	}
	if policy.Default != RuntimeTransportAuto && !runtimePolicyAllows(policy, policy.Default) {
		return runtimeTransportPolicy{}, fmt.Errorf("OpenLinker Runtime default transport %q is outside its allowlist", policy.Default)
	}
	if manifest == nil {
		return policy, nil
	}
	if manifest.Version != nil && *manifest.Version != runtimeTransportPolicyVersion {
		return runtimeTransportPolicy{}, fmt.Errorf("OpenLinker Runtime transport policy version %d is unsupported", *manifest.Version)
	}
	var err error
	if policy.HeartbeatInterval, err = policyDurationSeconds(manifest.HeartbeatIntervalSeconds, policy.HeartbeatInterval, "heartbeat_interval_seconds"); err != nil {
		return runtimeTransportPolicy{}, err
	}
	policy.HeartbeatIntervalSet = manifest.HeartbeatIntervalSeconds != nil
	if policy.SessionStaleAfter, err = policyDurationSeconds(manifest.SessionStaleAfterSeconds, policy.SessionStaleAfter, "session_stale_after_seconds"); err != nil {
		return runtimeTransportPolicy{}, err
	}
	policy.SessionStaleAfterSet = manifest.SessionStaleAfterSeconds != nil
	if policy.RetryMinimum, err = policyDurationMilliseconds(manifest.RetryMinimumMS, policy.RetryMinimum, "retry_minimum_ms"); err != nil {
		return runtimeTransportPolicy{}, err
	}
	policy.RetryMinimumSet = manifest.RetryMinimumMS != nil
	if policy.RetryMaximum, err = policyDurationMilliseconds(manifest.RetryMaximumMS, policy.RetryMaximum, "retry_maximum_ms"); err != nil {
		return runtimeTransportPolicy{}, err
	}
	policy.RetryMaximumSet = manifest.RetryMaximumMS != nil
	if policy.WebSocketProbeInterval, err = policyDurationMilliseconds(manifest.WebSocketProbeIntervalMS, policy.WebSocketProbeInterval, "websocket_probe_interval_ms"); err != nil {
		return runtimeTransportPolicy{}, err
	}
	policy.WebSocketProbeIntervalSet = manifest.WebSocketProbeIntervalMS != nil
	if policy.WebSocketProbeTimeout, err = policyDurationMilliseconds(manifest.WebSocketProbeTimeoutMS, policy.WebSocketProbeTimeout, "websocket_probe_timeout_ms"); err != nil {
		return runtimeTransportPolicy{}, err
	}
	policy.WebSocketProbeTimeoutSet = manifest.WebSocketProbeTimeoutMS != nil
	if policy.RetryMaximum < policy.RetryMinimum {
		return runtimeTransportPolicy{}, errors.New("OpenLinker Runtime retry maximum is below retry minimum")
	}
	if policy.SessionStaleAfter > 0 && policy.HeartbeatInterval >= policy.SessionStaleAfter {
		return runtimeTransportPolicy{}, errors.New("OpenLinker Runtime heartbeat interval must be below the Session stale interval")
	}
	return policy, nil
}

func runtimeManifestTransportMode(value string) (RuntimeTransportMode, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "websocket", "ws":
		return RuntimeTransportWebSocket, true
	case "long_poll", "pull":
		return RuntimeTransportPull, true
	default:
		return "", false
	}
}

func policyDurationSeconds(value *int64, fallback time.Duration, field string) (time.Duration, error) {
	if value == nil {
		return fallback, nil
	}
	if *value < 1 || *value > int64((24*time.Hour)/time.Second) {
		return 0, fmt.Errorf("OpenLinker Runtime %s is outside the supported range", field)
	}
	return time.Duration(*value) * time.Second, nil
}

func policyDurationMilliseconds(value *int64, fallback time.Duration, field string) (time.Duration, error) {
	if value == nil {
		return fallback, nil
	}
	if *value < 1 || *value > int64((24*time.Hour)/time.Millisecond) {
		return 0, fmt.Errorf("OpenLinker Runtime %s is outside the supported range", field)
	}
	return time.Duration(*value) * time.Millisecond, nil
}

func runtimePolicyAllows(policy runtimeTransportPolicy, mode RuntimeTransportMode) bool {
	for _, candidate := range policy.Allowed {
		if candidate == mode {
			return true
		}
	}
	return false
}

func (node *RuntimeWorker) applyRuntimeTransportPolicy(policy runtimeTransportPolicy) error {
	configured := RuntimeTransportMode(node.Transport)
	if configured != RuntimeTransportAuto && !runtimePolicyAllows(policy, configured) {
		return fmt.Errorf("configured Runtime transport %q is not allowed by OpenLinker", configured)
	}
	effective := configured
	if effective == RuntimeTransportAuto {
		effective = policy.Default
	}
	if effective != RuntimeTransportAuto {
		if !runtimePolicyAllows(policy, effective) {
			return fmt.Errorf("OpenLinker Runtime default transport %q is not allowed", effective)
		}
		node.Transport = effective
		node.transportOrder = []RuntimeTransportMode{effective}
	} else {
		node.transportOrder = append([]RuntimeTransportMode(nil), policy.Allowed...)
	}
	if policy.HeartbeatIntervalSet {
		node.HeartbeatInterval = policy.HeartbeatInterval
	}
	if policy.RetryMinimumSet {
		node.RetryMinimum = policy.RetryMinimum
	}
	if policy.RetryMaximumSet {
		node.RetryMaximum = policy.RetryMaximum
	}
	if policy.SessionStaleAfterSet {
		node.sessionStaleAfter = policy.SessionStaleAfter
	}
	if policy.WebSocketProbeIntervalSet {
		node.webSocketProbeInterval = policy.WebSocketProbeInterval
	}
	if policy.WebSocketProbeTimeoutSet {
		node.webSocketProbeTimeout = policy.WebSocketProbeTimeout
	}
	if node.RetryMaximum < node.RetryMinimum {
		return errors.New("OpenLinker Runtime retry maximum is below retry minimum")
	}
	if node.sessionStaleAfter > 0 && node.HeartbeatInterval >= node.sessionStaleAfter {
		return errors.New("OpenLinker Runtime heartbeat interval must be below the Session stale interval")
	}
	return nil
}

func (node *RuntimeWorker) orderedRuntimeTransports() []RuntimeTransportMode {
	if RuntimeTransportMode(node.Transport) != RuntimeTransportAuto {
		return []RuntimeTransportMode{RuntimeTransportMode(node.Transport)}
	}
	if len(node.transportOrder) == 0 {
		return []RuntimeTransportMode{RuntimeTransportWebSocket, RuntimeTransportPull}
	}
	return node.transportOrder
}

func (node *RuntimeWorker) autoPrefersWebSocket() bool {
	order := node.orderedRuntimeTransports()
	return RuntimeTransportMode(node.Transport) == RuntimeTransportAuto && len(order) > 0 && order[0] == RuntimeTransportWebSocket
}

func (node *RuntimeWorker) autoAllowsPullFallback() bool {
	if !node.autoPrefersWebSocket() {
		return false
	}
	for _, mode := range node.orderedRuntimeTransports()[1:] {
		if mode == RuntimeTransportPull {
			return true
		}
	}
	return false
}
