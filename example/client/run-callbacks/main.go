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
	return config{
		APIBase: apiBase, UserToken: userToken, AgentID: agentID,
		Input:          exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_RUN_INPUT"), "hello"),
		IdempotencyKey: idempotencyKey, Timeout: timeout,
	}, nil
}

func run(ctx context.Context, cfg config, output io.Writer) error {
	client, err := openlinker.NewClient(
		cfg.APIBase,
		openlinker.WithUserToken(cfg.UserToken),
		openlinker.WithSDKAgent("openlinker-go/example/client/run-callbacks"),
	)
	if err != nil {
		return err
	}
	result, err := client.RunAgentWithCallbacks(ctx, openlinker.RunAgentRequest{
		AgentID: cfg.AgentID, Input: openlinker.JSON{"text": cfg.Input}, IdempotencyKey: cfg.IdempotencyKey,
	}, openlinker.PlatformCallbackOptions{
		EventTypes: []string{"run.message.delta"},
		OnEvent: func(event openlinker.StreamRunEvent) error {
			_, err := fmt.Fprintf(output, "message delta: %s\n", event.Data)
			return err
		},
		OnTerminal: func(event openlinker.StreamRunEvent) error {
			_, err := fmt.Fprintf(output, "terminal callback: %s %s\n", event.Event, event.Data)
			return err
		},
		OnClose: func() error {
			_, err := fmt.Fprintln(output, "callback stream closed")
			return err
		},
	})
	if err != nil {
		return fmt.Errorf("使用 platform callback 运行 Agent: %w", err)
	}
	return exampleutil.PrintJSON(output, result)
}
