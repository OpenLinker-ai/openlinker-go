package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

type config struct {
	RuntimeBase   string
	NodeID        string
	AgentID       string
	AgentToken    string
	TargetAgentID string
	DataDir       string
	HTTPClient    *http.Client
	MaxRuns       int
}

func main() {
	ctx, stop := exampleutil.SignalContext()
	defer stop()
	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	if err = run(ctx, cfg); err != nil && !errors.Is(err, context.Canceled) {
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
	targetAgentID, err := exampleutil.RequiredEnv("OPENLINKER_TARGET_AGENT_ID")
	if err != nil {
		return config{}, err
	}
	return config{RuntimeBase: runtimeBase, NodeID: nodeID, AgentID: agentID, AgentToken: agentToken, TargetAgentID: targetAgentID,
		DataDir: exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_RUNTIME_DATA_DIR"), ".openlinker/runtime-native-delegation")}, nil
}

func run(ctx context.Context, cfg config) error {
	runner := openlinker.Native(func(ctx context.Context, run openlinker.NativeRun) (any, error) {
		summary, err := run.CallAgentWithRequest(ctx, openlinker.RuntimeCallAgentRequest{
			TargetAgentID: cfg.TargetAgentID, Input: map[string]any{"text": run.Text()},
			Reason: "example delegated task", Metadata: map[string]any{"parent_run_id": run.RunID()},
		}, "example-delegation-"+run.AttemptID())
		if err != nil {
			return openlinker.RetryableFailure("DELEGATION_FAILED", err), nil
		}
		return openlinker.Success(map[string]any{"child_run": summary}), nil
	}).WithRuntimeBase(cfg.RuntimeBase).
		WithNodeID(cfg.NodeID).WithAgentID(cfg.AgentID).WithAgentToken(cfg.AgentToken).
		WithDataDir(cfg.DataDir).WithTransportMode(openlinker.TransportHTTP).
		WithSDKAgent("openlinker-go/example/runtime/native-delegation")
	if cfg.HTTPClient != nil {
		runner.WithHTTPClient(cfg.HTTPClient)
	}
	if cfg.MaxRuns > 0 {
		runner.WithMaxRuns(cfg.MaxRuns)
	}
	return runner.Run(ctx)
}
