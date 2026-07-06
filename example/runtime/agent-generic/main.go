package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strings"
	"syscall"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

const sdkAgent = "openlinker-go/example/agent-generic"

type GenericAgent struct {
	Name   string
	Prefix string
}

func (a GenericAgent) Run(ctx context.Context, input string) (string, error) {
	select {
	case <-ctx.Done():
		return "", ctx.Err()
	default:
	}

	text := strings.TrimSpace(input)
	if text == "" {
		text = "hello"
	}

	name := strings.TrimSpace(a.Name)
	if name == "" {
		name = "Generic Agent"
	}

	prefix := strings.TrimSpace(a.Prefix)
	if prefix != "" {
		return fmt.Sprintf("%s %s", prefix, text), nil
	}
	return fmt.Sprintf("%s received: %s", name, text), nil
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	agent := GenericAgent{
		Name:   firstNonEmpty(os.Getenv("GENERIC_AGENT_NAME"), "Generic Agent"),
		Prefix: strings.TrimSpace(os.Getenv("GENERIC_AGENT_PREFIX")),
	}

	log.Printf("generic native agent starting name=%q", agent.Name)
	if err := openlinker.WithAgent(agent).
		WithSDKAgent(sdkAgent).
		Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("generic native agent failed: %v", err)
	}
	log.Print("generic native agent stopped")
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
