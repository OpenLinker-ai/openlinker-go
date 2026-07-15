package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

type config struct {
	APIBase        string
	UserToken      string
	AgentID        string
	Input          string
	IdempotencyKey string
	Timeout        time.Duration
	PollInterval   time.Duration
}

func main() {
	parent, stop := exampleutil.SignalContext()
	defer stop()

	cfg, err := loadConfig()
	if err != nil {
		log.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(parent, cfg.Timeout)
	defer cancel()
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
	timeout, err := exampleutil.EnvDuration("OPENLINKER_RUN_TIMEOUT", 30*time.Second)
	if err != nil {
		return config{}, err
	}
	pollInterval, err := exampleutil.EnvDuration("OPENLINKER_POLL_INTERVAL", time.Second)
	if err != nil {
		return config{}, err
	}
	return config{
		APIBase: apiBase, UserToken: userToken, AgentID: agentID,
		Input: exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_RUN_INPUT"), "hello"), IdempotencyKey: idempotencyKey,
		Timeout: timeout, PollInterval: pollInterval,
	}, nil
}

func run(ctx context.Context, cfg config, output io.Writer) error {
	client, err := openlinker.NewClient(
		cfg.APIBase,
		openlinker.WithUserToken(cfg.UserToken),
		openlinker.WithSDKAgent("openlinker-go/example/client/run-async"),
	)
	if err != nil {
		return err
	}
	started, err := client.StartAgentRun(ctx, openlinker.RunAgentRequest{
		AgentID: cfg.AgentID, Input: openlinker.JSON{"text": cfg.Input}, IdempotencyKey: cfg.IdempotencyKey,
	})
	if err != nil {
		return fmt.Errorf("异步提交 Agent Run: %w", err)
	}
	if _, err = fmt.Fprintf(output, "已提交 Run %s，初始状态 %s\n", started.RunID, started.Status); err != nil {
		return err
	}

	ticker := time.NewTicker(cfg.PollInterval)
	defer ticker.Stop()
	for {
		current, err := client.GetRun(ctx, started.RunID)
		if err != nil {
			return fmt.Errorf("查询 Run %s: %w", started.RunID, err)
		}
		if exampleutil.IsTerminalRunStatus(current.Status) {
			return exampleutil.PrintJSON(output, current)
		}
		select {
		case <-ctx.Done():
			return fmt.Errorf("等待 Run %s: %w", started.RunID, ctx.Err())
		case <-ticker.C:
		}
	}
}
