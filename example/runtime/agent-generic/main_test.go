package main

import (
	"context"
	"strings"
	"testing"
)

func TestGenericAgentRun(t *testing.T) {
	got, err := (GenericAgent{Name: "Test Agent"}).Run(context.Background(), " summarize this ")
	if err != nil {
		t.Fatal(err)
	}
	want := "Test Agent received: summarize this"
	if got != want {
		t.Fatalf("Run() = %q, want %q", got, want)
	}
}

func TestGenericAgentPrefix(t *testing.T) {
	got, err := (GenericAgent{Prefix: "handled"}).Run(context.Background(), "task")
	if err != nil {
		t.Fatal(err)
	}
	want := "handled task"
	if got != want {
		t.Fatalf("Run() = %q, want %q", got, want)
	}
}

func TestGenericAgentPanic(t *testing.T) {
	defer func() {
		recovered := recover()
		if recovered == nil {
			t.Fatal("expected panic")
		}
		if message := recovered.(string); !strings.Contains(message, "GENERIC_AGENT_PANIC") {
			t.Fatalf("panic = %q", message)
		}
	}()

	_, _ = (GenericAgent{Panic: true}).Run(context.Background(), "task")
}
