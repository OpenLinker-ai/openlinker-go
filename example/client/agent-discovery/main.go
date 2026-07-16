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
	APIBase   string
	UserToken string
	Slug      string
	Query     string
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
	slug, err := exampleutil.RequiredEnv("OPENLINKER_AGENT_SLUG")
	if err != nil {
		return config{}, err
	}
	return config{APIBase: apiBase, UserToken: userToken, Slug: slug, Query: os.Getenv("OPENLINKER_AGENT_QUERY")}, nil
}

func run(ctx context.Context, cfg config, output io.Writer) error {
	client, err := openlinker.NewClient(
		cfg.APIBase,
		openlinker.WithUserToken(cfg.UserToken),
		openlinker.WithSDKAgent("openlinker-go/example/client/agent-discovery"),
	)
	if err != nil {
		return err
	}

	list, err := client.ListAgents(ctx, openlinker.ListAgentsParams{
		Query: cfg.Query, Page: 1, Size: 10, CallableOnly: true,
	})
	if err != nil {
		return fmt.Errorf("列出 Agent: %w", err)
	}
	detail, err := client.GetAgent(ctx, cfg.Slug)
	if err != nil {
		return fmt.Errorf("读取 Agent %q: %w", cfg.Slug, err)
	}
	card, err := client.GetAgentCard(ctx, cfg.Slug, false)
	if err != nil {
		return fmt.Errorf("读取 Agent Card %q: %w", cfg.Slug, err)
	}

	return exampleutil.PrintJSON(output, struct {
		Total    int32                           `json:"total_callable_agents"`
		Agents   []openlinker.MarketListItem     `json:"agents"`
		Selected *openlinker.AgentDetailResponse `json:"selected"`
		Card     *openlinker.AgentCardResponse   `json:"agent_card"`
	}{Total: list.Total, Agents: list.Items, Selected: detail, Card: card})
}
