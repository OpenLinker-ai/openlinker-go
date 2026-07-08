package openlinker

import (
	"context"
	"testing"
)

func TestNativeRunFromContext(t *testing.T) {
	_, ok := NativeRunFromContext(context.Background())
	if ok {
		t.Fatal("NativeRunFromContext() ok = true, want false")
	}

	ctx := ContextWithNativeRun(context.Background(), NativeRun{
		Assignment: RuntimeAssignment{RunID: "run-1", AgentID: "agent-1"},
	})
	run, ok := NativeRunFromContext(ctx)
	if !ok {
		t.Fatal("NativeRunFromContext() ok = false, want true")
	}
	if run.Assignment.RunID != "run-1" || run.Assignment.AgentID != "agent-1" {
		t.Fatalf("run = %#v", run.Assignment)
	}
}
