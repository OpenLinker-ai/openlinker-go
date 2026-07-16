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

type config struct{ Endpoint, Token, Input, MessageID string }

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
	endpoint, err := exampleutil.RequiredEnv("OPENLINKER_A2A_ENDPOINT")
	if err != nil {
		return config{}, err
	}
	messageID, err := exampleutil.NewUUID()
	if err != nil {
		return config{}, err
	}
	return config{Endpoint: endpoint, Token: os.Getenv("OPENLINKER_A2A_TOKEN"), Input: exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_A2A_INPUT"), "hello A2A"), MessageID: messageID}, nil
}

func run(ctx context.Context, cfg config, output io.Writer) error {
	client, err := openlinker.NewA2AClient(cfg.Endpoint,
		openlinker.WithA2AToken(cfg.Token), openlinker.WithA2ASDKAgent("openlinker-go/example/a2a/jsonrpc"))
	if err != nil {
		return err
	}
	response, err := client.SendMessageResponse(ctx, openlinker.NewA2ATextMessageParams(cfg.MessageID, cfg.Input, nil))
	if err != nil {
		return fmt.Errorf("A2A JSON-RPC SendMessage: %w", err)
	}
	return exampleutil.PrintJSON(output, response)
}
