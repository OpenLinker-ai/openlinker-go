package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log"
	"os"
	"time"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

var errTerminalEvent = errors.New("terminal run event received")

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
		openlinker.WithSDKAgent("openlinker-go/example/client/run-stream"),
	)
	if err != nil {
		return err
	}
	started, err := client.StartAgentRun(ctx, openlinker.RunAgentRequest{
		AgentID: cfg.AgentID, Input: openlinker.JSON{"text": cfg.Input}, IdempotencyKey: cfg.IdempotencyKey,
	})
	if err != nil {
		return fmt.Errorf("提交流式 Agent Run: %w", err)
	}
	if _, err = fmt.Fprintf(output, "开始读取 Run %s 的 SSE 事件\n", started.RunID); err != nil {
		return err
	}

	terminal := false
	err = client.StreamRunEvents(ctx, started.RunID, openlinker.StreamRunEventsOptions{}, func(event openlinker.StreamRunEvent) error {
		if _, writeErr := fmt.Fprintf(output, "[%s] %s %s\n", event.ID, event.Event, event.Data); writeErr != nil {
			return writeErr
		}
		switch event.Event {
		case "run.completed", "run.failed", "run.canceled":
			terminal = true
			return errTerminalEvent
		default:
			return nil
		}
	})
	if errors.Is(err, errTerminalEvent) {
		return nil
	}
	if err != nil {
		return fmt.Errorf("读取 Run %s SSE: %w", started.RunID, err)
	}
	if !terminal {
		return fmt.Errorf("Run %s 的 SSE 在终态事件前断开", started.RunID)
	}
	return nil
}
