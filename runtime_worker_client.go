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

type RuntimeMTLSConfig struct {
	CertFile   string
	KeyFile    string
	CAFile     string
	ServerName string
}

func newRuntimeClient(runtimeAddress, agentToken string, config RuntimeMTLSConfig) (*Runtime, *http.Client, error) {
	runtimeURL, err := validateRuntimeOrigin(runtimeAddress)
	if err != nil {
		return nil, nil, err
	}
	if strings.TrimSpace(agentToken) == "" {
		return nil, nil, errors.New("Agent Token is required")
	}
	if config.CertFile == "" || config.KeyFile == "" || config.CAFile == "" {
		return nil, nil, errors.New("runtime mTLS cert, key, and CA files are required")
	}
	certificate, err := tls.LoadX509KeyPair(config.CertFile, config.KeyFile)
	if err != nil {
		return nil, nil, fmt.Errorf("load runtime mTLS client certificate: %w", err)
	}
	caPEM, err := os.ReadFile(config.CAFile)
	if err != nil {
		return nil, nil, fmt.Errorf("read runtime mTLS CA: %w", err)
	}
	roots := x509.NewCertPool()
	if !roots.AppendCertsFromPEM(caPEM) {
		return nil, nil, errors.New("runtime mTLS CA file contains no certificates")
	}
	transport := http.DefaultTransport.(*http.Transport).Clone()
	transport.TLSClientConfig = &tls.Config{
		MinVersion:   tls.VersionTLS13,
		Certificates: []tls.Certificate{certificate},
		RootCAs:      roots,
		ServerName:   strings.TrimSpace(config.ServerName),
	}
	transport.ResponseHeaderTimeout = 35 * time.Second
	transport.TLSHandshakeTimeout = 10 * time.Second
	transport.IdleConnTimeout = 90 * time.Second
	httpClient := &http.Client{
		Transport: transport,
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
