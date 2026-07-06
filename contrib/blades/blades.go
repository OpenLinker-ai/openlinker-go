package openlinkerblades

import (
	"context"
	"fmt"
	"strings"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
	"github.com/go-kratos/blades"
)

const defaultSDKAgent = "openlinker-go/contrib/blades"

type Agent struct {
	agent blades.Agent
}

func NewAgent(agent blades.Agent) Agent {
	return Agent{agent: agent}
}

func (a Agent) Run(ctx context.Context, input string) (string, error) {
	runner := blades.NewRunner(a.agent)
	result, err := runner.Run(ctx, blades.UserMessage(input))
	if err != nil {
		return "", fmt.Errorf("llm run failed: %w", err)
	}
	answer := strings.TrimSpace(result.Text())
	if answer == "" {
		return "", fmt.Errorf("llm returned empty response")
	}
	return answer, nil
}

func WithAgent(agent blades.Agent) *openlinker.NativeAgentRunner {
	return openlinker.WithAgent(NewAgent(agent)).
		WithSDKAgent(defaultSDKAgent)
}
