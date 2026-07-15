package main

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-go/example/internal/runtimetest"
)

func TestRunEmitsNativeEventsAndResult(t *testing.T) {
	server := runtimetest.New()
	defer server.Close()
	ctx, cancel := context.WithCancel(context.Background())
	errCh := make(chan error, 1)
	go func() {
		errCh <- run(ctx, config{
			RuntimeBase: server.URL(), NodeID: runtimetest.NodeID, AgentID: runtimetest.AgentID, AgentToken: runtimetest.AgentToken,
			DataDir: filepath.Join(t.TempDir(), "runtime"), HTTPClient: server.Client(),
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
	events := server.Events()
	if result.Status != "success" || len(events) != 3 || events[0].EventType != "run.message.delta" || events[1].EventType != "run.progress.changed" || events[2].EventType != "example.trace.completed" || server.Err() != nil {
		t.Fatalf("result=%#v events=%#v server err=%v", result, events, server.Err())
	}
}
