package main

import (
	"context"
	"errors"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	openlinkerblades "github.com/OpenLinker-ai/openlinker-go/contrib/blades"
	"github.com/go-kratos/blades"
	"github.com/go-kratos/blades/contrib/openai"
)

const sdkAgent = "openlinker-go/example/native-llm-worker"

func main() {
	cfg, err := loadConfig()
	if err != nil {
		log.Fatalf("config error: %v", err)
	}

	model := openai.NewModel(cfg.OpenAIModel, openai.Config{
		APIKey:  cfg.OpenAIAPIKey,
		BaseURL: cfg.OpenAIBaseURL,
	})

	agent, err := blades.NewAgent(
		"Chat Agent",
		blades.WithModel(model),
		blades.WithInstruction("You are a helpful assistant that provides detailed and accurate information."),
	)
	if err != nil {
		log.Fatalf("agent init failed: %v", err)
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	log.Printf("native llm worker starting model=%s", cfg.OpenAIModel)

	if err := openlinkerblades.WithAgent(agent).
		WithModel(cfg.OpenAIModel).
		WithSDKAgent(sdkAgent).
		Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("native runtime failed: %v", err)
	}
	log.Print("native llm worker stopped")
}

type config struct {
	OpenAIModel   string
	OpenAIAPIKey  string
	OpenAIBaseURL string
}

func loadConfig() (config, error) {
	cfg := config{
		OpenAIModel:   strings.TrimSpace(os.Getenv("OPENAI_MODEL")),
		OpenAIAPIKey:  strings.TrimSpace(os.Getenv("OPENAI_API_KEY")),
		OpenAIBaseURL: strings.TrimSpace(os.Getenv("OPENAI_BASE_URL")),
	}
	if cfg.OpenAIModel == "" {
		return cfg, errors.New("OPENAI_MODEL is required")
	}
	if cfg.OpenAIAPIKey == "" {
		return cfg, errors.New("OPENAI_API_KEY is required")
	}
	return cfg, nil
}
