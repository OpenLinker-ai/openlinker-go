package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net/http"
	"os"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

type config struct {
	RuntimeBase string
	NodeID      string
	AgentID     string
	AgentToken  string
	HTTPClient  *http.Client
	TLS         openlinker.RuntimeTLSConfig
}

func main() {
	ctx, stop := exampleutil.SignalContext()
	defer stop()
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	if err = run(ctx, cfg, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func loadConfig() (config, error) {
	runtimeBase, err := exampleutil.RequiredEnv("OPENLINKER_RUNTIME_BASE")
	if err != nil {
		return config{}, err
	}
	nodeID, err := exampleutil.RequiredEnv("OPENLINKER_NODE_ID")
	if err != nil {
		return config{}, err
	}
	agentID, err := exampleutil.RequiredEnv("OPENLINKER_AGENT_ID")
	if err != nil {
		return config{}, err
	}
	agentToken, err := exampleutil.RequiredEnv("OPENLINKER_AGENT_TOKEN")
	if err != nil {
		return config{}, err
	}
	return config{RuntimeBase: runtimeBase, NodeID: nodeID, AgentID: agentID, AgentToken: agentToken, TLS: openlinker.RuntimeTLSConfig{
		CertificateFile: os.Getenv("OPENLINKER_NODE_CERT_FILE"), PrivateKeyFile: os.Getenv("OPENLINKER_NODE_KEY_FILE"),
		CAFile: os.Getenv("OPENLINKER_RUNTIME_CA_FILE"), ServerName: os.Getenv("OPENLINKER_RUNTIME_SERVER_NAME"),
	}}, nil
}

func run(ctx context.Context, cfg config, output io.Writer) error {
	httpClient := cfg.HTTPClient
	var err error
	if httpClient == nil {
		httpClient, err = openlinker.NewRuntimeHTTPClient(cfg.TLS)
		if err != nil {
			return err
		}
	}
	runtimeClient, err := openlinker.NewRuntime(cfg.RuntimeBase,
		openlinker.WithAgentToken(cfg.AgentToken), openlinker.WithHTTPClient(httpClient),
		openlinker.WithSDKAgent("openlinker-go/example/runtime/protocol-http"))
	if err != nil {
		return err
	}
	workerID, err := exampleutil.NewUUID()
	if err != nil {
		return err
	}
	sessionID, err := exampleutil.NewUUID()
	if err != nil {
		return err
	}
	hello := openlinker.RuntimeHelloPayload{
		NodeID: cfg.NodeID, AgentID: cfg.AgentID, WorkerID: workerID, RuntimeSessionID: sessionID,
		SessionEpoch: 1, NodeVersion: "openlinker-go/example/protocol-http", Capacity: 1,
		Features: openlinker.RuntimeRequiredFeatures(), ContractDigest: openlinker.RuntimeContractDigest,
	}
	ready, err := runtimeClient.CreateRuntimeSession(ctx, hello)
	if err != nil {
		return fmt.Errorf("create session: %w", err)
	}
	defer func() {
		_ = runtimeClient.CloseRuntimeSession(context.Background(), openlinker.RuntimeSessionCloseRequest{
			NodeID: cfg.NodeID, AgentID: cfg.AgentID, WorkerID: workerID, RuntimeSessionID: sessionID,
			SessionEpoch: 1, Status: "closed", Reason: "protocol_example_complete",
		})
	}()
	if _, err = runtimeClient.ResumeRuntimeRuns(ctx, openlinker.RuntimeResumePayload{
		NodeID: cfg.NodeID, AgentID: cfg.AgentID, WorkerID: workerID, RuntimeSessionID: sessionID, Attempts: []openlinker.RuntimeResumeAttempt{},
	}); err != nil {
		return fmt.Errorf("resume: %w", err)
	}
	assignment, err := runtimeClient.ClaimRuntimeRun(ctx, 1, openlinker.RuntimeClaimRequest{RuntimeSessionID: sessionID, Capacity: 1})
	if err != nil {
		return fmt.Errorf("claim: %w", err)
	}
	if assignment == nil {
		return exampleutil.PrintJSON(output, map[string]any{"attachment_id": ready.AttachmentID, "assignment": nil})
	}
	rejected, err := runtimeClient.RejectRuntimeAssignment(ctx, openlinker.RuntimeAssignmentRejectPayload{
		AttemptIdentity: assignment.AttemptIdentity, ReasonCode: openlinker.RuntimeRejectNodeDraining, Capacity: 0, Inflight: 0,
	})
	if err != nil {
		return fmt.Errorf("reject assignment: %w", err)
	}
	return exampleutil.PrintJSON(output, map[string]any{
		"attachment_id": ready.AttachmentID, "run_id": assignment.AttemptIdentity.RunID,
		"outcome": rejected.Outcome, "warning": "底层协议调用方必须自行实现 durable journal、lease、spool、resume 和 reconnect",
	})
}
