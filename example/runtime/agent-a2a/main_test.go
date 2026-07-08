package main

import (
	"context"
	"strings"
	"testing"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

type recordingCaller struct {
	request openlinker.CallAgentRequest
}

func (c *recordingCaller) CallAgent(ctx context.Context, req openlinker.CallAgentRequest) (*openlinker.RunResponse, error) {
	c.request = req
	return &openlinker.RunResponse{RunID: "child-run", Status: "running", ParentRunID: req.ParentRunID}, nil
}

func TestA2AAgentCallsChildAgent(t *testing.T) {
	caller := &recordingCaller{}
	agent := A2AAgent{
		Caller:        caller,
		TargetAgentID: "agent-child",
	}

	ctx := openlinker.ContextWithNativeRun(context.Background(), openlinker.NativeRun{
		Assignment: openlinker.RuntimeAssignment{
			RunID: "parent-run",
			A2A:   &openlinker.AgentA2AContext{TraceID: "trace-1"},
		},
	})
	got, err := agent.Run(ctx, "delegate this")
	if err != nil {
		t.Fatal(err)
	}

	if !strings.Contains(got, "child-run") {
		t.Fatalf("Run() = %q", got)
	}
	if caller.request.ParentRunID != "parent-run" {
		t.Fatalf("parent_run_id = %q", caller.request.ParentRunID)
	}
	if caller.request.CurrentRunID != "parent-run" {
		t.Fatalf("current_run_id = %q", caller.request.CurrentRunID)
	}
	if caller.request.TargetAgentID != "agent-child" {
		t.Fatalf("target_agent_id = %q", caller.request.TargetAgentID)
	}
	if caller.request.TraceID != "trace-1" {
		t.Fatalf("trace_id = %q", caller.request.TraceID)
	}
	input := caller.request.Input.(openlinker.JSON)
	if input["task"] != "delegate this" {
		t.Fatalf("input = %#v", input)
	}
}

func TestA2AAgentRequiresNativeRunContext(t *testing.T) {
	agent := A2AAgent{
		Caller:        &recordingCaller{},
		TargetAgentID: "agent-child",
	}

	_, err := agent.Run(context.Background(), "task")
	if err == nil || !strings.Contains(err.Error(), "native run context") {
		t.Fatalf("error = %v", err)
	}
}
