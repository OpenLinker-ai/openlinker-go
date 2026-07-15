package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-go/example/internal/runtimetest"
)

func TestRunRegistersThenExecutesOneAssignment(t *testing.T) {
	t.Setenv("OPENLINKER_AGENT_TOKEN", "")
	server := runtimetest.New()
	defer server.Close()
	cfg := config{
		APIBase: server.URL(), RuntimeBase: server.URL(), UserToken: runtimetest.UserToken, NodeID: runtimetest.NodeID,
		DataDir: filepath.Join(t.TempDir(), "runtime"), StatePath: filepath.Join(t.TempDir(), "registration.env"),
		HTTPClient: server.Client(),
	}
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() { errCh <- run(ctx, cfg) }()
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
	if result.Status != "success" || server.RegistrationCalls() != 3 || server.Err() != nil {
		t.Fatalf("result=%#v registration calls=%d server err=%v", result, server.RegistrationCalls(), server.Err())
	}
}
