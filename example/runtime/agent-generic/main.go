package main

import (
	"context"
	"errors"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"syscall"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
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
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	agent := GenericAgent{
		Name:   firstNonEmpty(os.Getenv("GENERIC_AGENT_NAME"), "Generic Agent"),
		Prefix: strings.TrimSpace(os.Getenv("GENERIC_AGENT_PREFIX")),
		Panic:  envBool("GENERIC_AGENT_PANIC"),
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

func envBool(key string) bool {
	value := strings.TrimSpace(os.Getenv(key))
	if value == "" {
		return false
	}
	enabled, err := strconv.ParseBool(value)
	return err == nil && enabled
}
