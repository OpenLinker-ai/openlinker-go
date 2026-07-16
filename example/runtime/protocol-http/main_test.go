package main

import (
	"bytes"
	"context"
	"strings"
	"testing"
	"time"

	"github.com/OpenLinker-ai/openlinker-go/example/internal/runtimetest"
)

func TestRunUsesHTTPProtocolPrimitives(t *testing.T) {
	server := runtimetest.New()
	defer server.Close()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var output bytes.Buffer
	if err := run(ctx, config{
		RuntimeBase: server.URL(), NodeID: runtimetest.NodeID, AgentID: runtimetest.AgentID,
		AgentToken: runtimetest.AgentToken, HTTPClient: server.Client(),
	}, &output); err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(output.String(), `"outcome": "offer_rejected"`) || !strings.Contains(output.String(), "durable journal") || server.Err() != nil {
		t.Fatalf("output=%s server err=%v", output.String(), server.Err())
	}
}
