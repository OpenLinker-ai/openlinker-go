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
	APIBase     string
	UserToken   string
	Slug        string
	Name        string
	Description string
	Tags        []string
	StatePath   string
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
	slug, err := exampleutil.RequiredEnv("OPENLINKER_AGENT_SLUG")
	if err != nil {
		return config{}, err
	}
	name, err := exampleutil.RequiredEnv("OPENLINKER_AGENT_NAME")
	if err != nil {
		return config{}, err
	}
	return config{
		APIBase: apiBase, UserToken: os.Getenv("OPENLINKER_USER_TOKEN"), Slug: slug, Name: name,
		Description: os.Getenv("OPENLINKER_AGENT_DESCRIPTION"),
		Tags:        exampleutil.SplitCSV(exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_AGENT_TAGS"), "agent,runtime")),
		StatePath:   exampleutil.FirstNonEmpty(os.Getenv("OPENLINKER_REGISTRATION_STATE_PATH"), ".openlinker/registration.env"),
	}, nil
}

func run(ctx context.Context, cfg config, output io.Writer) error {
	registration, err := openlinker.EnsureAgent(ctx, openlinker.AgentSpec{
		Slug: cfg.Slug, Name: cfg.Name, Description: cfg.Description,
		Tags: cfg.Tags, Visibility: "private", ConnectionMode: "runtime",
	},
		openlinker.WithRegistrationAPIBase(cfg.APIBase),
		openlinker.WithRegistrationUserToken(cfg.UserToken),
		openlinker.WithRegistrationPolicy(openlinker.RegisterPolicyReuseExisting),
		openlinker.WithRegistrationStore(openlinker.NewEnvRegistrationStore(cfg.StatePath)),
		openlinker.WithRegistrationToken(cfg.Name+" runtime", []string{"agent:pull", "agent:call"}, 0),
	)
	if err != nil {
		return fmt.Errorf("创建或复用 Agent: %w", err)
	}

	// AgentToken is intentionally omitted. The registration store keeps it in a
	// private local file for later Runtime startup.
	return exampleutil.PrintJSON(output, struct {
		AgentID     string `json:"agent_id"`
		AgentSlug   string `json:"agent_slug"`
		AgentName   string `json:"agent_name"`
		TokenID     string `json:"token_id"`
		TokenPrefix string `json:"token_prefix"`
		StatePath   string `json:"state_path"`
	}{
		AgentID: registration.AgentID, AgentSlug: registration.AgentSlug, AgentName: registration.AgentName,
		TokenID: registration.TokenID, TokenPrefix: registration.TokenPrefix, StatePath: cfg.StatePath,
	})
}
