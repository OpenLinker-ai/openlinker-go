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

const sdkAgent = "openlinker-go/example/agent-a2a"

type AgentCaller interface {
	CallAgent(context.Context, openlinker.CallAgentRequest) (*openlinker.RunResponse, error)
}

type A2AAgent struct {
	Caller        AgentCaller
	TargetAgentID string
}

func (a A2AAgent) Run(ctx context.Context, input string) (string, error) {
	if a.Caller == nil {
		return "", errors.New("agent caller is required")
	}
	if strings.TrimSpace(a.TargetAgentID) == "" {
		return "", errors.New("A2A_TARGET_AGENT_ID is required")
	}

	run, ok := openlinker.NativeRunFromContext(ctx)
	if !ok {
		return "", errors.New("openlinker native run context is required")
	}

	task := strings.TrimSpace(input)
	if task == "" {
		task = "hello from parent agent"
	}

	child, err := a.Caller.CallAgent(ctx, openlinker.CallAgentRequest{
		ParentRunID:   run.Assignment.RunID,
		CurrentRunID:  run.Assignment.RunID,
		TargetAgentID: strings.TrimSpace(a.TargetAgentID),
		Reason:        "deterministic A2A example",
		Input: openlinker.JSON{
			"task": task,
		},
		TraceID: traceID(run),
	})
	if err != nil {
		return "", err
	}
	if child == nil {
		return "", errors.New("call agent returned empty response")
	}

	return fmt.Sprintf("called child agent, run_id=%s status=%s", child.RunID, child.Status), nil
}

func traceID(run openlinker.NativeRun) string {
	if run.Assignment.A2A == nil {
		return ""
	}
	return strings.TrimSpace(run.Assignment.A2A.TraceID)
}

func main() {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	client, err := newRuntimeClient()
	if err != nil {
		log.Fatal(err)
	}

	agent := A2AAgent{
		Caller:        client,
		TargetAgentID: os.Getenv("A2A_TARGET_AGENT_ID"),
	}

	log.Print("A2A example agent starting")
	if err := openlinker.WithAgent(agent).WithSDKAgent(sdkAgent).Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
		log.Fatalf("A2A example agent failed: %v", err)
	}
	log.Print("A2A example agent stopped")
}

func newRuntimeClient() (*openlinker.Client, error) {
	token := strings.TrimSpace(firstNonEmpty(os.Getenv("OPENLINKER_RUNTIME_TOKEN"), os.Getenv("OPENLINKER_AGENT_TOKEN")))
	if token == "" {
		return nil, errors.New("OPENLINKER_RUNTIME_TOKEN is required")
	}
	apiBase := firstNonEmpty(os.Getenv("OPENLINKER_API_BASE"), "https://api.openlinker.ai")
	return openlinker.NewClient(apiBase, openlinker.WithRuntimeToken(token), openlinker.WithSDKAgent(sdkAgent))
}

func firstNonEmpty(values ...string) string {
	for _, value := range values {
		if strings.TrimSpace(value) != "" {
			return strings.TrimSpace(value)
		}
	}
	return ""
}
