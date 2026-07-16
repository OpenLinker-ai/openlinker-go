package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"strconv"
	"strings"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

type config struct {
	APIBase       string
	UserToken     string
	RunID         string
	AfterSequence int32
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
	apiBase, err := exampleutil.RequiredEnv("OPENLINKER_API_BASE")
	if err != nil {
		return config{}, err
	}
	userToken, err := exampleutil.RequiredEnv("OPENLINKER_USER_TOKEN")
	if err != nil {
		return config{}, err
	}
	runID, err := exampleutil.RequiredEnv("OPENLINKER_RUN_ID")
	if err != nil {
		return config{}, err
	}
	var afterSequence int32
	if raw := strings.TrimSpace(os.Getenv("OPENLINKER_AFTER_SEQUENCE")); raw != "" {
		value, parseErr := strconv.ParseInt(raw, 10, 32)
		if parseErr != nil || value < 0 {
			return config{}, fmt.Errorf("OPENLINKER_AFTER_SEQUENCE 必须是非负整数: %q", raw)
		}
		afterSequence = int32(value)
	}
	return config{APIBase: apiBase, UserToken: userToken, RunID: runID, AfterSequence: afterSequence}, nil
}

func run(ctx context.Context, cfg config, output io.Writer) error {
	client, err := openlinker.NewClient(
		cfg.APIBase,
		openlinker.WithUserToken(cfg.UserToken),
		openlinker.WithSDKAgent("openlinker-go/example/client/run-history"),
	)
	if err != nil {
		return err
	}
	events, err := client.ListRunEvents(ctx, cfg.RunID, openlinker.ListRunEventsParams{AfterSequence: cfg.AfterSequence, Limit: 50})
	if err != nil {
		return fmt.Errorf("读取 Run Events: %w", err)
	}
	messages, err := client.ListRunMessages(ctx, cfg.RunID)
	if err != nil {
		return fmt.Errorf("读取 Run Messages: %w", err)
	}
	artifacts, err := client.ListRunArtifacts(ctx, cfg.RunID)
	if err != nil {
		return fmt.Errorf("读取 Run Artifacts: %w", err)
	}
	children, err := client.ListRunChildren(ctx, cfg.RunID)
	if err != nil {
		return fmt.Errorf("读取 Run Children: %w", err)
	}

	return exampleutil.PrintJSON(output, struct {
		RunID     string                           `json:"run_id"`
		Events    []openlinker.RunEventResponse    `json:"events"`
		EventPage openlinker.RunEventPageMeta      `json:"event_page"`
		Messages  []openlinker.RunMessageResponse  `json:"messages"`
		Artifacts []openlinker.RunArtifactResponse `json:"artifacts"`
		Children  []openlinker.RunChildResponse    `json:"children"`
	}{
		RunID: cfg.RunID, Events: events.Items, EventPage: events.Meta,
		Messages: messages.Items, Artifacts: artifacts.Items, Children: children.Items,
	})
}
