package openlinker_test

import (
	"context"
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

type layoutContractAgent struct{}

func (layoutContractAgent) Handle(ctx context.Context, run openlinker.NativeRun) (openlinker.NativeResult, error) {
	identity := run.Identity()
	_ = run.Metadata()
	_, _ = run.Deadline()
	if ctx.Err() != nil {
		return openlinker.Failure("LAYOUT_CANCELED", ctx.Err()), nil
	}
	return openlinker.Success(map[string]any{
		"run_id":     identity.RunID,
		"attempt_id": identity.AttemptID,
	}), nil
}

func TestLayoutHandlerCanBindDirectlyToNative(t *testing.T) {
	t.Parallel()
	agent := layoutContractAgent{}
	worker := openlinker.Native(agent.Handle).
		WithRegistration(openlinker.AgentSpec{
			Slug: "layout-contract", Name: "Layout Contract", Visibility: "private",
		})
	if worker == nil {
		t.Fatal("Native layout worker is nil")
	}
}
