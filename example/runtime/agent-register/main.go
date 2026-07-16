package main

import (
	"context"
	"errors"
	"log"
	"net/http"
	"os"
	"strings"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

type registeredAgent struct{}

func (registeredAgent) Run(_ context.Context, input string) (string, error) {
	return "registered agent received: " + strings.TrimSpace(input), nil
}

type config struct {
	APIBase     string
	RuntimeBase string
	UserToken   string
	NodeID      string
	DataDir     string
	StatePath   string
	HTTPClient  *http.Client
	MaxRuns     int
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
	apiBase, err := exampleutil.RequiredEnv("OPENLINKER_API_BASE")
	if err != nil {
		return config{}, err
	}
	nodeID, err := exampleutil.RequiredEnv("OPENLINKER_NODE_ID")
	if err != nil {
		return config{}, err
	}
	return config{
		APIBase: apiBase, RuntimeBase: os.Getenv("OPENLINKER_RUNTIME_BASE"), UserToken: os.Getenv("OPENLINKER_USER_TOKEN"), NodeID: nodeID,
		DataDir:   exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_RUNTIME_DATA_DIR"), ".openlinker/runtime-register-agent"),
		StatePath: exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_REGISTRATION_STATE_PATH"), ".openlinker/registration-agent.env"),
	}, nil
}

func run(ctx context.Context, cfg config) error {
	runner := openlinker.WithAgent(registeredAgent{}).
		WithAPIBase(cfg.APIBase).
		WithRuntimeBase(cfg.RuntimeBase).
		WithUserToken(cfg.UserToken).
		WithNodeID(cfg.NodeID).
		WithDataDir(cfg.DataDir).
		WithTransportMode(openlinker.TransportHTTP).
		WithSDKAgent("openlinker-go/example/runtime/agent-register")
	if cfg.HTTPClient != nil {
		runner.WithHTTPClient(cfg.HTTPClient)
	}
	if cfg.MaxRuns > 0 {
		runner.WithMaxRuns(cfg.MaxRuns)
	}
	return runner.RunOrRegister(ctx, openlinker.AgentSpec{
		Slug: "example-register-agent", Name: "Example Register Agent",
		Description: "Explicit RunOrRegister example", Tags: []string{"agent", "runtime", "example"}, Visibility: "private",
	},
		openlinker.WithRegistrationAPIBase(cfg.APIBase),
		openlinker.WithRegistrationUserToken(cfg.UserToken),
		openlinker.WithRegistrationStore(openlinker.NewEnvRegistrationStore(cfg.StatePath)),
	)
}
