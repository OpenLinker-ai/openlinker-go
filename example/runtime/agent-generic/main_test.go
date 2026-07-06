package main

import (
	"context"
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
