package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

type config struct {
	APIBase        string
	UserToken      string
	AgentID        string
	Input          string
	IdempotencyKey string
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
	apiBase, err := exampleutil.RequiredEnv("OPENLINKER_API_BASE")
	if err != nil {
		return config{}, err
	}
	userToken, err := exampleutil.RequiredEnv("OPENLINKER_USER_TOKEN")
	if err != nil {
		return config{}, err
	}
	agentID, err := exampleutil.RequiredEnv("OPENLINKER_AGENT_ID")
	if err != nil {
		return config{}, err
	}
	idempotencyKey, err := exampleutil.RequiredEnv("OPENLINKER_IDEMPOTENCY_KEY")
	if err != nil {
		return config{}, err
	}
	return config{
		APIBase: apiBase, UserToken: userToken, AgentID: agentID,
		Input: exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_RUN_INPUT"), "hello"), IdempotencyKey: idempotencyKey,
	}, nil
}

func run(ctx context.Context, cfg config, output io.Writer) error {
	client, err := openlinker.NewClient(
		cfg.APIBase,
		openlinker.WithUserToken(cfg.UserToken),
		openlinker.WithSDKAgent("openlinker-go/example/client/run-sync"),
	)
	if err != nil {
		return err
	}
	result, err := client.RunAgent(ctx, openlinker.RunAgentRequest{
		AgentID:        cfg.AgentID,
		Input:          openlinker.JSON{"text": cfg.Input},
		IdempotencyKey: cfg.IdempotencyKey,
	})
	if err != nil {
		return fmt.Errorf("同步运行 Agent: %w", err)
	}
	return exampleutil.PrintJSON(output, result)
}
