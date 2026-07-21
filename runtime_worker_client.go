package openlinker

import (
	"context"
	"crypto/tls"
	"crypto/x509"
	"errors"
	"fmt"
	"net/http"
	"os"
	"strings"
	"time"
)

const runtimeWorkerSDKAgent = "openlinker-go/runtime-worker"

type RuntimeClient interface {
	CreateRuntimeSession(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error)
	HeartbeatRuntimeSession(context.Context, RuntimeHelloPayload) (*RuntimeReadyPayload, error)
	CloseRuntimeSession(context.Context, RuntimeSessionCloseRequest) error
	ClaimRuntimeRun(context.Context, int, RuntimeClaimRequest) (*RuntimeRunAssignedPayload, error)
	AckRuntimeAssignment(context.Context, RuntimeAssignmentAckPayload) (*RuntimeAssignmentConfirmedPayload, error)
	RejectRuntimeAssignment(context.Context, RuntimeAssignmentRejectPayload) (*RuntimeAssignmentRejectedPayload, error)
	RenewRuntimeLease(context.Context, RuntimeLeaseRenewPayload) (*RuntimeLeaseRenewedPayload, error)
	AppendRuntimeEvent(context.Context, RuntimeRunEventPayload) (*RuntimeRunEventAckPayload, error)
	FinalizeRuntimeResult(context.Context, RuntimeRunResultPayload) (*RuntimeRunResultAckPayload, error)
	ResumeRuntimeRuns(context.Context, RuntimeResumePayload) (*RuntimeResumeResponse, error)
	PollRuntimeCommands(context.Context, string, int) (*RuntimeCommandsResponse, error)
	AckRuntimeCancel(context.Context, RuntimeRunCancelAckPayload) (*RuntimeRunCancellationState, error)
	CallRuntimeAgent(context.Context, RuntimeCallAgentAuthorization, RuntimeCallAgentRequest) (*RuntimeRunSummary, error)
}

// runtimeDrainClient is deliberately optional so adding graceful drain does
// not break existing RuntimeClient implementations. Official transports
// implement it; custom clients fail closed only when Drain is actually used.
type runtimeDrainClient interface {
	DrainRuntimeSession(context.Context, string, RuntimeDrainPayload) (*RuntimeDrainPayload, error)
}

type RuntimeMTLSConfig struct {
	CertFile   string
	KeyFile    string
	CAFile     string
	ServerName string
	// Disabled is set automatically from discovery when Core explicitly uses
	// Agent Token-only HTTPS transport.
	Disabled          bool
	tlsConfig         *tls.Config
	credentialManager *runtimeCredentialManager
}

func newRuntimeClient(runtimeAddress, agentToken string, config RuntimeMTLSConfig) (*Runtime, *http.Client, error) {
	runtimeURL, err := validateRuntimeOriginForPolicy(runtimeAddress, config.Disabled)
	if err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(agentToken) == "" {
		return nil, nil, errors.New("Agent Token is required")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	if !config.Disabled {
		var tlsConfig *tls.Config
		if config.tlsConfig != nil {
			tlsConfig = config.tlsConfig.Clone()
		} else {
			if config.CertFile == "" || config.KeyFile == "" || config.CAFile == "" {
				return nil, nil, errors.New("runtime mTLS credential is unavailable")
			}
			certificate, loadErr := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
			if loadErr != nil {
				return nil, nil, fmt.Errorf("load runtime mTLS client certificate: %w", loadErr)
			}
			caPEM, readErr := os.ReadFile(config.CAFile)
			if readErr != nil {
				return nil, nil, fmt.Errorf("read runtime mTLS CA: %w", readErr)
			}
			roots := x509.NewCertPool()
			if !roots.AppendCertsFromPEM(caPEM) {
				return nil, nil, errors.New("runtime mTLS CA file contains no certificates")
			}
			tlsConfig = &tls.Config{Certificates: []tls.Certificate{certificate}, RootCAs: roots}
		}
		tlsConfig.MinVersion = tls.VersionTLS13
		if serverName := strings.TrimSpace(config.ServerName); serverName != "" {
			tlsConfig.ServerName = serverName
		}
		transport.TLSClientConfig = tlsConfig
	} else {
		transport.TLSClientConfig = &tls.Config{MinVersion: tls.VersionTLS12}
	}
	transport.ResponseHeaderTimeout = 35 * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.IdleConnTimeout = 90 * time.Second
	var roundTripper http.RoundTripper = transport
	if config.credentialManager != nil {
		config.credentialManager.SetCloseIdleConnections(transport.CloseIdleConnections)
		roundTripper = &runtimeCredentialRenewingTransport{base: transport, credentials: config.credentialManager}
	}
	httpClient := &http.Client{
		Transport: roundTripper,
		CheckRedirect: func(*http.Request, []*http.Request) error {
			// Runtime credentials and the client certificate are bound to the
			// configured Core origin. Runtime endpoints must not redirect them.
			return http.ErrUseLastResponse
		},
	}
	runtimeClient, err := NewRuntime(
		runtimeURL,
		WithAgentToken(agentToken),
		WithHTTPClient(httpClient),
		WithSDKAgent(runtimeWorkerSDKAgent),
	)
	if err != nil {
		transport.CloseIdleConnections()
		return nil, nil, err
	}
	return runtimeClient, httpClient, nil
}

type runtimeCredentialRenewingTransport struct {
	base        http.RoundTripper
	credentials *runtimeCredentialManager
}

func (transport *runtimeCredentialRenewingTransport) CloseIdleConnections() {
	if closer, ok := transport.base.(interface{ CloseIdleConnections() }); ok {
		closer.CloseIdleConnections()
	}
}

func (transport *runtimeCredentialRenewingTransport) RoundTrip(request *http.Request) (*http.Response, error) {
	if transport == nil || transport.base == nil || transport.credentials == nil {
		return nil, errors.New("runtime credential transport is unavailable")
	}
	if err := transport.credentials.Ensure(request.Context(), false); err != nil {
		return nil, err
	}
	response, err := transport.base.RoundTrip(request)
	if err == nil {
		return response, nil
	}
	if !runtimeCredentialTLSFailure(err) {
		return nil, err
	}
	if request.GetBody == nil && request.Body != nil {
		return nil, err
	}
	if renewErr := transport.credentials.Ensure(request.Context(), true); renewErr != nil {
		return nil, errors.Join(err, renewErr)
	}
	retry := request.Clone(request.Context())
	if request.GetBody != nil {
		retry.Body, err = request.GetBody()
		if err != nil {
			return nil, err
		}
	}
	return transport.base.RoundTrip(retry)
}

func runtimeCredentialTLSFailure(err error) bool {
	if err == nil {
		return false
	}
	message := strings.ToLower(err.Error())
	for _, marker := range []string{"tls", "x509", "certificate", "unknown authority"} {
		if strings.Contains(message, marker) {
			return true
		}
	}
	return false
}
