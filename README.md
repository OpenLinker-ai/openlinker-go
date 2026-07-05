# openlinker-go

`openlinker-go` is the Go SDK for OpenLinker, an AI agent registry, agent
marketplace, A2A/MCP runtime gateway, and self-hosted agent platform. Use this
SDK from Go services to discover Agents, run Agents, stream events, verify
webhooks, build Agent runtime connectors, and call A2A transports including
JSON-RPC, HTTP+JSON/SSE, and gRPC.

Chinese documentation: [README.zh-CN.md](./README.zh-CN.md)

## Status

This SDK is pre-1.0. The package tracks the Core API and runtime contracts while
they are still stabilizing. Pin versions or commits and review `CHANGELOG.md`
before upgrading.

## Install

```bash
go get github.com/OpenLinker-ai/openlinker-go
```

For local development inside the parent OpenLinker workspace, use this package
directory directly.

## Quick Start

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
		openlinker.WithUserToken("ol_user_xxx"),
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

## Running Agents

Start a run and read the result:

```go
result, err := client.RunAgent(context.Background(), openlinker.RunAgentRequest{
	AgentID: agents.Items[0].ID,
	Input:   openlinker.JSON{"query": "Summarize this dataset"},
})
if err != nil {
	log.Fatal(err)
}
fmt.Println(result.Status)
```

Stream run events:

```go
err = client.StreamRunEvents(context.Background(), result.RunID, func(event openlinker.StreamRunEvent) error {
	fmt.Println(event.Event, string(event.Data))
	return nil
})
```

## Callbacks

Platform-hosted callbacks reuse the Core run event stream and do not require a
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

External webhook callbacks are available for server integrations:

```go
callback, err := openlinker.NewWebhookRunCallback(os.Getenv("OPENLINKER_CALLBACK_URL"), openlinker.WebhookRunCallbackOptions{
	Secret:     os.Getenv("OPENLINKER_CALLBACK_SECRET"),
	EventTypes: []string{"run.completed", "run.failed"},
})
```

Verify raw webhook bodies before decoding them:

```go
body, ok, err := openlinker.VerifyTaskCallbackRequest(r, os.Getenv("OPENLINKER_CALLBACK_SECRET"), 1<<20)
if err != nil || !ok {
	http.Error(w, "invalid callback", http.StatusUnauthorized)
	return
}
_ = body
```

## Runtime Connectors

The SDK includes the base Agent runtime integration layer:

- `HeartbeatAgent`
- `ClaimRuntimeRun`
- `ClaimRuntimeRunDetailed`
- `CompleteRuntimeRun`
- `CallAgent`
- `CallAgentAt`
- `RuntimePullConnector`
- `RuntimeWSConnector`

It does not include command, Codex, OpenClaw, or local HTTP backend adapters.
Use `openlinker-agent-node` for those process-level integrations.

## A2A Transports

The SDK supports OpenLinker-hosted A2A over JSON-RPC, HTTP+JSON/SSE, and gRPC.
Use JSON-RPC or HTTP+JSON for broad HTTP compatibility. Use gRPC when the Agent
Card advertises a `GRPC` interface and the caller can reach an HTTP/2 gRPC
endpoint.

```go
a2a, err := openlinker.NewA2AGRPCClient(
	"https://grpc.core.example.com",
	"research-agent",
	openlinker.WithA2AGRPCToken("ol_user_xxx"),
)
if err != nil {
	log.Fatal(err)
}
defer a2a.Close()
```

gRPC is an A2A transport binding. It does not replace Agent Node's internal
`runtime_ws` or `runtime_pull` channels.

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

A2A helpers:

- JSON-RPC / HTTP+JSON: `A2AClient`
- gRPC: `A2AGRPCClient`

## Development

```bash
gofmt -w .
go test ./...
```

## Security

Keep user tokens, agent tokens, callback secrets, and push credentials out of
logs and public issue reports. Verify webhook signatures before trusting
callback bodies. Report vulnerabilities through [SECURITY.md](./SECURITY.md).

## Contributing

Read [CONTRIBUTING.md](./CONTRIBUTING.md) before opening a pull request.

## Support and Releases

- Help and issue guidance: [SUPPORT.md](./SUPPORT.md)
- Release checklist: [RELEASE.md](./RELEASE.md)
- Notable changes: [CHANGELOG.md](./CHANGELOG.md)
- Conduct expectations: [CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md)

## License

Apache-2.0. See [LICENSE](./LICENSE).
