package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"strings"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

const sdkAgent = "openlinker-go/example/agent-generic"

type GenericAgent struct {
	Name   string
	Prefix string
	Panic  bool
}

func (a GenericAgent) Run(ctx context.Context, input string) (string, error) {
	if a.Panic {
		panic("generic agent panic requested by GENERIC_AGENT_PANIC")
	}

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
	ctx, stop := exampleutil.SignalContext()
	defer stop()

	agent := GenericAgent{
		Name:   exampleutil.FirstNonEmpty(os.Getenv("GENERIC_AGENT_NAME"), "Generic Agent"),
		Prefix: strings.TrimSpace(os.Getenv("GENERIC_AGENT_PREFIX")),
		Panic:  exampleutil.EnvBool("GENERIC_AGENT_PANIC"),
	}

	log.Printf("generic native agent starting name=%q", agent.Name)
	if err := openlinker.WithAgent(agent).
		WithSDKAgent(sdkAgent).
		Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("generic native agent failed: %v", err)
	}
	log.Print("generic native agent stopped")
}
