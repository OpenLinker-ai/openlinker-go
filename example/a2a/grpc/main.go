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
	Endpoint, Tenant, Token, Input, MessageID string
	Options                                   []openlinker.A2AGRPCClientOption
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
	endpoint, err := exampleutil.RequiredEnv("OPENLINKER_A2A_GRPC_ENDPOINT")
	if err != nil {
		return config{}, err
	}
	tenant, err := exampleutil.RequiredEnv("OPENLINKER_A2A_TENANT")
	if err != nil {
		return config{}, err
	}
	messageID, err := exampleutil.NewUUID()
	if err != nil {
		return config{}, err
	}
	return config{Endpoint: endpoint, Tenant: tenant, Token: os.Getenv("OPENLINKER_A2A_TOKEN"), Input: exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_A2A_INPUT"), "hello A2A gRPC"), MessageID: messageID}, nil
}

func run(ctx context.Context, cfg config, output io.Writer) error {
	options := []openlinker.A2AGRPCClientOption{
		openlinker.WithA2AGRPCToken(cfg.Token), openlinker.WithA2AGRPCSDKAgent("openlinker-go/example/a2a/grpc"),
	}
	options = append(options, cfg.Options...)
	client, err := openlinker.NewA2AGRPCClient(cfg.Endpoint, cfg.Tenant, options...)
	if err != nil {
		return err
	}
	defer client.Close()
	response, err := client.SendMessageResponse(ctx, openlinker.NewA2ATextMessageParams(cfg.MessageID, cfg.Input, nil))
	if err != nil {
		return fmt.Errorf("A2A gRPC SendMessage: %w", err)
	}
	return exampleutil.PrintJSON(output, response)
}
