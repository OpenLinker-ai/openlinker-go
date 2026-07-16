package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-go/example/internal/runtimetest"
)

func TestRunDelegatesWithinAssignment(t *testing.T) {
	server := runtimetest.New()
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, config{
			RuntimeBase: server.URL(), NodeID: runtimetest.NodeID, AgentID: runtimetest.AgentID, AgentToken: runtimetest.AgentToken,
			TargetAgentID: runtimetest.TargetAgentID, DataDir: filepath.Join(t.TempDir(), "runtime"), HTTPClient: server.Client(),
		})
	}()
	waitCtx, waitCancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer waitCancel()
	result, err := server.WaitResult(waitCtx)
	cancel()
	if runErr := <-errCh; runErr != nil {
		t.Fatal(runErr)
	}
	if err != nil {
		t.Fatal(err)
	}
	calls := server.DelegatedCalls()
	if result.Status != "success" || len(calls) != 1 || calls[0].TargetAgentID != runtimetest.TargetAgentID || server.Err() != nil {
		t.Fatalf("result=%#v calls=%#v server err=%v", result, calls, server.Err())
	}
}
