package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/OpenLinker-ai/openlinker-go/example/internal/exampleutil"
)

type config struct {
	APIBase          string
	UserToken        string
	Action           string
	ConfirmWrite     bool
	AgentID          string
	TokenID          string
	TokenName        string
	Scopes           []string
	ExpiresInMinutes int32
}

type tokenSummary struct {
	ID        string   `json:"id"`
	AgentID   *string  `json:"agent_id,omitempty"`
	Name      string   `json:"name"`
	Prefix    string   `json:"prefix"`
	Status    string   `json:"status"`
	Scopes    []string `json:"scopes"`
	ExpiresAt *string  `json:"expires_at,omitempty"`
	CreatedAt string   `json:"created_at"`
}

func main() {
	cfg, err := parseConfig(os.Args[1:], os.Getenv)
	if err != nil {
		log.Fatal(err)
	}
	ctx, stop := exampleutil.SignalContext()
	defer stop()
	if err = run(ctx, cfg, os.Stdout); err != nil {
		log.Fatal(err)
	}
}

func parseConfig(args []string, getenv func(string) string) (config, error) {
	flags := flag.NewFlagSet("token-management", flag.ContinueOnError)
	flags.SetOutput(io.Discard)
	action := flags.String("action", "list", "list, create or revoke")
	confirmWrite := flags.Bool("confirm-write", false, "allow create or revoke")
	agentID := flags.String("agent-id", strings.TrimSpace(getenv("OPENLINKER_AGENT_ID")), "Agent ID")
	tokenID := flags.String("token-id", "", "Token ID for revoke")
	tokenName := flags.String("name", "example runtime token", "Token name for create")
	scopes := flags.String("scopes", "agent:pull,agent:call", "comma-separated scopes")
	expires := flags.Int("expires-minutes", 0, "token lifetime in minutes; 0 uses platform default")
	if err := flags.Parse(args); err != nil {
		return config{}, err
	}
	if flags.NArg() != 0 {
		return config{}, fmt.Errorf("不支持位置参数: %s", strings.Join(flags.Args(), " "))
	}
	apiBase := strings.TrimSpace(getenv("OPENLINKER_API_BASE"))
	userToken := strings.TrimSpace(getenv("OPENLINKER_USER_TOKEN"))
	if apiBase == "" || userToken == "" {
		return config{}, errors.New("OPENLINKER_API_BASE 和 OPENLINKER_USER_TOKEN 为必填环境变量")
	}
	if *agentID == "" {
		return config{}, errors.New("OPENLINKER_AGENT_ID 或 --agent-id 为必填")
	}
	if *expires < 0 || int64(*expires) > int64(1<<31-1) {
		return config{}, errors.New("--expires-minutes 必须在 0 到 2147483647 之间")
	}
	value := config{
		APIBase: apiBase, UserToken: userToken, Action: strings.ToLower(strings.TrimSpace(*action)),
		ConfirmWrite: *confirmWrite, AgentID: strings.TrimSpace(*agentID), TokenID: strings.TrimSpace(*tokenID),
		TokenName: strings.TrimSpace(*tokenName), Scopes: exampleutil.SplitCSV(*scopes), ExpiresInMinutes: int32(*expires),
	}
	switch value.Action {
	case "list":
		return value, nil
	case "create":
		if !value.ConfirmWrite {
			return config{}, errors.New("创建 Token 必须同时传入 --confirm-write")
		}
		if value.TokenName == "" || len(value.Scopes) == 0 {
			return config{}, errors.New("创建 Token 需要非空 --name 和 --scopes")
		}
		return value, nil
	case "revoke":
		if !value.ConfirmWrite {
			return config{}, errors.New("撤销 Token 必须同时传入 --confirm-write")
		}
		if value.TokenID == "" {
			return config{}, errors.New("撤销 Token 需要 --token-id")
		}
		return value, nil
	default:
		return config{}, fmt.Errorf("未知 --action %q；支持 list、create、revoke", value.Action)
	}
}

func run(ctx context.Context, cfg config, output io.Writer) error {
	client, err := openlinker.NewClient(
		cfg.APIBase,
		openlinker.WithUserToken(cfg.UserToken),
		openlinker.WithSDKAgent("openlinker-go/example/registration/token-management"),
	)
	if err != nil {
		return err
	}
	switch cfg.Action {
	case "list":
		response, err := client.ListAgentTokens(ctx, openlinker.ListAgentTokensParams{
			AgentID: cfg.AgentID, Limit: 50, SortBy: "created_at", SortDir: "desc",
		})
		if err != nil {
			return fmt.Errorf("列出 Agent Token: %w", err)
		}
		items := make([]tokenSummary, len(response.Items))
		for index, token := range response.Items {
			items[index] = summarizeToken(token)
		}
		return exampleutil.PrintJSON(output, struct {
			Items   []tokenSummary `json:"items"`
			Total   int32          `json:"total"`
			HasMore bool           `json:"has_more"`
		}{Items: items, Total: response.Total, HasMore: response.HasMore})
	case "create":
		token, err := client.CreateAgentToken(ctx, openlinker.CreateAgentTokenRequest{
			Name: cfg.TokenName, AgentID: cfg.AgentID, Scopes: cfg.Scopes, ExpiresInMinutes: cfg.ExpiresInMinutes,
		})
		if err != nil {
			return fmt.Errorf("创建 Agent Token: %w", err)
		}
		return exampleutil.PrintJSON(output, struct {
			Notice         string       `json:"notice"`
			Token          tokenSummary `json:"token"`
			PlaintextToken string       `json:"plaintext_token"`
		}{
			Notice: "plaintext_token 通常只显示一次，请立即保存到安全位置",
			Token:  summarizeToken(*token), PlaintextToken: token.PlaintextToken,
		})
	case "revoke":
		if err := client.RevokeAgentToken(ctx, cfg.TokenID); err != nil {
			return fmt.Errorf("撤销 Agent Token: %w", err)
		}
		return exampleutil.PrintJSON(output, map[string]string{"status": "revoked", "token_id": cfg.TokenID})
	default:
		return fmt.Errorf("unsupported action %q", cfg.Action)
	}
}

func summarizeToken(token openlinker.AgentTokenResponse) tokenSummary {
	return tokenSummary{
		ID: token.ID, AgentID: token.AgentID, Name: token.Name, Prefix: token.Prefix,
		Status: token.Status, Scopes: token.Scopes, ExpiresAt: token.ExpiresAt, CreatedAt: token.CreatedAt,
	}
}
