# openlinker-go

Go client SDK for OpenLinker Core APIs.

Status: pre-release skeleton. This package is intentionally limited to Core
registry, client runtime, and Agent runtime protocol APIs. Cloud wallet,
billing, task marketplace, commercial dashboard, workflow product APIs, and
adapter implementations are out of scope for this package.

## Usage

```go
package main

import (
	"context"
	"fmt"
	"log"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func main() {
	client, err := openlinker.NewClient(
		"https://core.example.com",
		openlinker.WithAccessToken("ol_live_xxx"),
		openlinker.WithRuntimeToken("ol_live_runtime_xxx"),
	)
	if err != nil {
		log.Fatal(err)
	}

	agents, err := client.ListAgents(context.Background(), openlinker.ListAgentsParams{
		Query:        "data",
		CallableOnly: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(agents.Total)
}
```

Platform-hosted callbacks reuse the run event stream and do not require a
public callback URL:

```go
result, err := client.RunAgentWithCallbacks(context.Background(), openlinker.RunAgentRequest{
	AgentID: agents.Items[0].ID,
	Input:   openlinker.JSON{"query": "Summarize this dataset"},
}, openlinker.PlatformCallbackOptions{
	EventTypes: []string{"run.message.delta"},
	OnEvent: func(event openlinker.StreamRunEvent) error {
		fmt.Println(event.Event, string(event.Data))
		return nil
	},
})
```

External webhook callbacks are available for server integrations. The SDK
builds the callback config, passes the external address and secret to Core, and
provides request verification helpers:

```go
callback, err := openlinker.NewWebhookRunCallback(os.Getenv("OPENLINKER_CALLBACK_URL"), openlinker.WebhookRunCallbackOptions{
	Secret:     os.Getenv("OPENLINKER_CALLBACK_SECRET"),
	EventTypes: []string{"run.completed", "run.failed"},
})
if err != nil {
	log.Fatal(err)
}

_, err = client.StartAgentRun(context.Background(), openlinker.RunAgentRequest{
	AgentID:      agents.Items[0].ID,
	Input:        openlinker.JSON{"query": "Summarize this dataset"},
	TaskCallback: callback,
})
```

Verify the raw callback body before decoding it:

```go
func handleOpenLinkerCallback(w http.ResponseWriter, r *http.Request) {
	body, ok, err := openlinker.VerifyTaskCallbackRequest(r, os.Getenv("OPENLINKER_CALLBACK_SECRET"), 1<<20)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	if !ok {
		http.Error(w, "invalid signature", http.StatusUnauthorized)
		return
	}

	_ = body
}
```

## A2A Transports

The Go SDK supports OpenLinker-hosted A2A over JSON-RPC, HTTP+JSON/SSE, and
gRPC. Use JSON-RPC or HTTP+JSON when you want the broadest HTTP compatibility
or browser-adjacent infrastructure. Use gRPC when the Agent Card advertises a
`GRPC` supported interface and the caller can reach an HTTP/2 gRPC endpoint.
The `tenant` argument is the Agent Card interface tenant, normally the Agent
slug:

```go
a2a, err := openlinker.NewA2AGRPCClient(
	"https://grpc.core.example.com",
	"research-agent",
	openlinker.WithA2AGRPCToken("ol_live_xxx"),
)
if err != nil {
	log.Fatal(err)
}
defer a2a.Close()

task, err := a2a.SendMessage(context.Background(), openlinker.A2AMessageSendParams{
	Message: openlinker.A2AMessage{
		MessageID: "msg-1",
		Role:      "user",
		Parts:     []map[string]any{{"kind": "text", "text": "Summarize this"}},
	},
})
```

gRPC is an A2A transport binding, not a replacement for Agent Node's
`runtime_ws` / `runtime_pull` channels. Core translates every binding into the
same task/run/webhook lifecycle.

## Core Surface

Application-side calls:

- `ListAgents`
- `GetAgent`
- `GetAgentCard`
- `RunAgent`
- `RunAgentWithCallbacks`
- `StartAgentRun`
- `StartAgentRunWithCallbacks`
- `GetRun`
- `ListRunEvents`
- `ListRunArtifacts`
- `ListRunMessages`
- `StreamRunEvents`

Agent runtime protocol:

- `HeartbeatAgent`
- `ClaimRuntimeRun`
- `ClaimRuntimeRunDetailed`
- `CompleteRuntimeRun`
- `CallAgent`
- `CallAgentAt`
- `RuntimePullConnector`
- `RuntimeWSConnector`

A2A protocol helpers:

- JSON-RPC / HTTP+JSON: `A2AClient`
- gRPC: `A2AGRPCClient`

The package includes the base runtime integration layer: pull loop, websocket
connect/reconnect, assignment callbacks, `run.event`, and `run.result`
submission. It does not include adapters such as command, Codex, OpenClaw, or
local HTTP backend runners.

## Development

```bash
go test ./...
```

## Contributing and Security

See [CONTRIBUTING.md](./CONTRIBUTING.md) for development rules and
[CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md) for conduct expectations.
Use [SUPPORT.md](./SUPPORT.md) for help, [SECURITY.md](./SECURITY.md) for
vulnerability reporting, [CHANGELOG.md](./CHANGELOG.md) for release notes, and
[RELEASE.md](./RELEASE.md) for release checks.

## License

Apache-2.0. See [LICENSE](./LICENSE).
