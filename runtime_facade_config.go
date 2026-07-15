package openlinker

import (
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

// TransportMode is the application-facing transport selection. TransportHTTP
// maps to the Runtime pull transport used by the protocol implementation.
type TransportMode = RuntimeTransportMode

const (
	TransportAuto      TransportMode = RuntimeTransportAuto
	TransportWebSocket TransportMode = RuntimeTransportWebSocket
	TransportHTTP      TransportMode = RuntimeTransportPull
)

const (
	EnvRuntimeBase       = "OPENLINKER_RUNTIME_BASE"
	EnvAPIBase           = "OPENLINKER_API_BASE"
	EnvNodeID            = "OPENLINKER_NODE_ID"
	EnvAgentID           = "OPENLINKER_AGENT_ID"
	EnvAgentToken        = "OPENLINKER_AGENT_TOKEN"
	EnvRuntimeTransport  = "OPENLINKER_RUNTIME_TRANSPORT"
	EnvRuntimeCapacity   = "OPENLINKER_RUNTIME_CAPACITY"
	EnvRuntimeDataDir    = "OPENLINKER_RUNTIME_DATA_DIR"
	EnvRuntimeStatePath  = "OPENLINKER_RUNTIME_STATE_PATH" // Deprecated: use EnvRuntimeDataDir.
	EnvNodeCertFile      = "OPENLINKER_NODE_CERT_FILE"
	EnvNodeKeyFile       = "OPENLINKER_NODE_KEY_FILE"
	EnvRuntimeCAFile     = "OPENLINKER_RUNTIME_CA_FILE"
	EnvRuntimeServerName = "OPENLINKER_RUNTIME_SERVER_NAME"
)

// RuntimeTLSConfig is the environment-oriented form of RuntimeMTLSConfig.
type RuntimeTLSConfig struct {
	CertificateFile string
	PrivateKeyFile  string
	CAFile          string
	ServerName      string
}

func (config RuntimeTLSConfig) runtimeMTLS() RuntimeMTLSConfig {
	return RuntimeMTLSConfig{
		CertFile: config.CertificateFile, KeyFile: config.PrivateKeyFile,
		CAFile: config.CAFile, ServerName: config.ServerName,
	}
}

// RuntimeConfigError reports every missing or invalid facade setting together.
type RuntimeConfigError struct{ Problems []string }

func (err *RuntimeConfigError) Error() string {
	if err == nil || len(err.Problems) == 0 {
		return "openlinker: Runtime configuration is invalid"
	}
	return "openlinker: Runtime configuration is incomplete:\n- " + strings.Join(err.Problems, "\n- ")
}

// LoadRuntimeWorkerConfig resolves the standard environment into the canonical
// RuntimeWorkerConfig used by the reliable worker.
func LoadRuntimeWorkerConfig() (RuntimeWorkerConfig, error) {
	transport, ok := normalizeFacadeTransport(os.Getenv(EnvRuntimeTransport))
	if !ok {
		return RuntimeWorkerConfig{}, &RuntimeConfigError{Problems: []string{EnvRuntimeTransport + " must be auto, websocket/ws, or http/pull"}}
	}
	capacity := RuntimeWorkerDefaultCapacity
	if raw := strings.TrimSpace(os.Getenv(EnvRuntimeCapacity)); raw != "" {
		value, err := strconv.ParseInt(raw, 10, 64)
		if err != nil {
			return RuntimeWorkerConfig{}, &RuntimeConfigError{Problems: []string{EnvRuntimeCapacity + " must be an integer"}}
		}
		capacity = value
	}
	dataDir := strings.TrimSpace(os.Getenv(EnvRuntimeDataDir))
	if dataDir == "" {
		dataDir = legacyRuntimeDataDir(strings.TrimSpace(os.Getenv(EnvRuntimeStatePath)))
	}
	agentID := strings.TrimSpace(os.Getenv(EnvAgentID))
	if dataDir == "" && agentID != "" {
		dataDir = defaultRuntimeDataDir(agentID)
	}
	return RuntimeWorkerConfig{
		PlatformURL: strings.TrimSpace(os.Getenv(EnvAPIBase)),
		RuntimeURL:  strings.TrimSpace(os.Getenv(EnvRuntimeBase)),
		Transport:   transport,
		NodeID:      strings.TrimSpace(os.Getenv(EnvNodeID)),
		AgentID:     agentID,
		AgentToken:  strings.TrimSpace(os.Getenv(EnvAgentToken)),
		DataDir:     dataDir,
		Capacity:    capacity,
		MTLS: RuntimeMTLSConfig{
			CertFile:   strings.TrimSpace(os.Getenv(EnvNodeCertFile)),
			KeyFile:    strings.TrimSpace(os.Getenv(EnvNodeKeyFile)),
			CAFile:     strings.TrimSpace(os.Getenv(EnvRuntimeCAFile)),
			ServerName: strings.TrimSpace(os.Getenv(EnvRuntimeServerName)),
		},
	}, nil
}

// Validate checks facade configuration in one pass. When requireClient is
// false, Runtime connection and mTLS values may be supplied by an injected
// Runtime client.
func (config RuntimeWorkerConfig) Validate(requireClient bool) error {
	problems := make([]string, 0, 10)
	if !validRuntimeUUID(strings.TrimSpace(config.NodeID)) {
		problems = append(problems, EnvNodeID+" must be a non-zero lowercase UUID")
	}
	if !validRuntimeUUID(strings.TrimSpace(config.AgentID)) {
		problems = append(problems, EnvAgentID+" must be a non-zero lowercase UUID")
	}
	if config.Capacity < 1 || config.Capacity > RuntimeMaxNodeCapacity {
		problems = append(problems, EnvRuntimeCapacity+" must be between 1 and 1024")
	}
	if _, ok := normalizeFacadeTransport(string(config.Transport)); !ok {
		problems = append(problems, EnvRuntimeTransport+" must be auto, websocket/ws, or http/pull")
	}
	if config.Store == nil && strings.TrimSpace(config.DataDir) == "" {
		problems = append(problems, EnvRuntimeDataDir+" is missing")
	}
	if requireClient {
		if strings.TrimSpace(config.RuntimeURL) == "" && strings.TrimSpace(config.PlatformURL) == "" {
			problems = append(problems, EnvRuntimeBase+" or "+EnvAPIBase+" is missing")
		}
		if strings.TrimSpace(config.AgentToken) == "" {
			problems = append(problems, EnvAgentToken+" is missing")
		}
		if strings.TrimSpace(config.MTLS.CertFile) == "" {
			problems = append(problems, EnvNodeCertFile+" is missing")
		}
		if strings.TrimSpace(config.MTLS.KeyFile) == "" {
			problems = append(problems, EnvNodeKeyFile+" is missing")
		}
		if strings.TrimSpace(config.MTLS.CAFile) == "" {
			problems = append(problems, EnvRuntimeCAFile+" is missing")
		}
	}
	if len(problems) != 0 {
		return &RuntimeConfigError{Problems: problems}
	}
	return nil
}

// NewRuntimeFromEnv builds the low-level Runtime client from standard env.
func NewRuntimeFromEnv() (*Runtime, error) {
	config, err := LoadRuntimeWorkerConfig()
	if err != nil {
		return nil, err
	}
	if err = config.Validate(true); err != nil {
		return nil, err
	}
	client, err := NewRuntimeHTTPClient(RuntimeTLSConfig{
		CertificateFile: config.MTLS.CertFile,
		PrivateKeyFile:  config.MTLS.KeyFile,
		CAFile:          config.MTLS.CAFile,
		ServerName:      config.MTLS.ServerName,
	})
	if err != nil {
		return nil, err
	}
	base := config.RuntimeURL
	if base == "" {
		return nil, errors.New("openlinker: OPENLINKER_RUNTIME_BASE is required for NewRuntimeFromEnv; discovery is managed by RuntimeWorker")
	}
	return NewRuntime(base, WithAgentToken(config.AgentToken), WithHTTPClient(client))
}

// NewRuntimeHTTPClient creates the strict mTLS HTTP client shared by Runtime
// HTTP and WebSocket protocol clients.
func NewRuntimeHTTPClient(config RuntimeTLSConfig) (*http.Client, error) {
	mtls := config.runtimeMTLS()
	if mtls.CertFile == "" || mtls.KeyFile == "" || mtls.CAFile == "" {
		return nil, &RuntimeConfigError{Problems: missingRuntimeTLSProblems(mtls)}
	}
	certificate, err := tls.LoadX509KeyPair(mtls.CertFile, mtls.KeyFile)
	if err != nil {
		return nil, fmt.Errorf("openlinker: load Runtime Node certificate/key: %w", err)
	}
	caPEM, err := os.ReadFile(mtls.CAFile)
	if err != nil {
		return nil, fmt.Errorf("openlinker: read Runtime CA file %q: %w", mtls.CAFile, err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, errors.New("openlinker: Runtime CA file does not contain a valid PEM certificate")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion: tls.VersionTLS13, Certificates: []tls.Certificate{certificate},
		RootCAs: roots, ServerName: strings.TrimSpace(mtls.ServerName),
	}
	return &http.Client{Transport: transport}, nil
}

func missingRuntimeTLSProblems(config RuntimeMTLSConfig) []string {
	problems := make([]string, 0, 3)
	if strings.TrimSpace(config.CertFile) == "" {
		problems = append(problems, EnvNodeCertFile+" is missing")
	}
	if strings.TrimSpace(config.KeyFile) == "" {
		problems = append(problems, EnvNodeKeyFile+" is missing")
	}
	if strings.TrimSpace(config.CAFile) == "" {
		problems = append(problems, EnvRuntimeCAFile+" is missing")
	}
	return problems
}

func normalizeFacadeTransport(value string) (RuntimeTransportMode, bool) {
	switch strings.ToLower(strings.TrimSpace(value)) {
	case "", "auto":
		return RuntimeTransportAuto, true
	case "websocket", "ws":
		return RuntimeTransportWebSocket, true
	case "http", "pull":
		return RuntimeTransportPull, true
	default:
		return "", false
	}
}

func defaultRuntimeDataDir(agentID string) string {
	return filepath.Join(".openlinker", "runtime-"+strings.TrimSpace(agentID))
}

func legacyRuntimeDataDir(statePath string) string {
	if statePath == "" {
		return ""
	}
	extension := filepath.Ext(statePath)
	if extension == ".json" {
		return strings.TrimSuffix(statePath, extension)
	}
	return statePath
}
