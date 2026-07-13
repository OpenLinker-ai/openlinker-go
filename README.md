# openlinker-go

`openlinker-go` is the Go SDK for OpenLinker Core. Use `NewClient` to discover
and invoke Agents, stream run events, verify webhooks, and call A2A transports.
Use `NewRuntimeWorker` to run an Agent handler with durable Runtime delivery.
`NewRuntime` remains available for lower-level Runtime protocol access.

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

## Open-source Architecture

The Go SDK keeps caller and Runtime credentials separate. `NewClient` wraps
User Token platform calls. `NewRuntimeWorker` discovers the dedicated mTLS
Runtime origin and owns delivery, recovery, and durable state. Process-level
HTTP, command, Codex, and A2A adapters belong in `openlinker-agent-node`.

```mermaid
flowchart LR
  Service["Go service / CLI / backend"] --> ClientSDK["openlinker-go Client"]
  ClientSDK -->|"REST client with OPENLINKER_USER_TOKEN"| Core["openlinker-core<br/>registry / runs / events"]
  ClientSDK -->|"A2A JSON-RPC / HTTP+JSON / gRPC"| Core
  AppHandler["Go RuntimeHandler"] --> RuntimeSDK["openlinker-go RuntimeWorker"]
  AgentNode["openlinker-agent-node<br/>optional Adapter shell"] --> RuntimeSDK
  RuntimeSDK -->|"mTLS + Agent Token / WebSocket or HTTP long-poll"| Core

  HostedBridge["Hosted Bridge<br/>optional deployment adapter"] -.->|"same Core API contract"| Core

  Core -->|"direct_http"| HTTPAgent["Public HTTPS Agent"]
  Core -->|"mcp_server"| MCPAgent["Remote MCP / JSON-RPC server"]
  Core -->|"Runtime assignments and cancellation"| RuntimeSDK
```

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
runIntentID := "replace-with-an-application-generated-intent-id"
result, err := client.RunAgent(context.Background(), openlinker.RunAgentRequest{
	AgentID:        agents.Items[0].ID,
	Input:          openlinker.JSON{"query": "Summarize this dataset"},
	IdempotencyKey: runIntentID, // Reuse for retries of this same run intent.
})
if err != nil {
	log.Fatal(err)
}
fmt.Println(result.Status)
```

`RunAgent` and `StartAgentRun` always send `Idempotency-Key`. If the field is
empty, the SDK generates a cryptographically random key for that method call.
Set `IdempotencyKey` when a retry may happen in a later invocation or process,
and reuse it only for the same run intent. `result.Replayed` reports whether
Core returned the existing run.

Stream run events:

```go
err = client.StreamRunEvents(context.Background(), result.RunID, openlinker.StreamRunEventsOptions{}, func(event openlinker.StreamRunEvent) error {
	fmt.Println(event.Event, string(event.Data))
	return nil
})
```

For retained event history, `ListRunEvents` returns `Items` plus `Meta`. The
metadata reports the requested and effective cursors, retention gaps, nullable
available-sequence bounds, terminal state, and whether the returned page
completes the stream.

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

## OpenLinker Runtime

`RuntimeWorker` owns discovery, mTLS, Session lifecycle, WebSocket-to-pull
recovery, assignment confirmation, lease renewal, resume, cancellation, drain,
and the encrypted assignment/Event/Result store. A handler runs only after Core
confirms the assignment.

```go
worker, err := openlinker.NewRuntimeWorker(openlinker.RuntimeWorkerConfig{
	PlatformURL: "https://openlinker.example",
	NodeID:      os.Getenv("OPENLINKER_NODE_ID"),
	AgentID:     os.Getenv("OPENLINKER_AGENT_ID"),
	AgentToken:  os.Getenv("OPENLINKER_AGENT_TOKEN"),
	Transport:   openlinker.RuntimeTransportAuto,
	DataDir:     "/var/lib/my-agent/runtime",
	MTLS: openlinker.RuntimeMTLSConfig{
		CertFile: "/run/openlinker/node.crt",
		KeyFile:  "/run/openlinker/node.key",
		CAFile:   "/run/openlinker/core-ca.crt",
	},
	Handler: openlinker.RuntimeHandlerFunc(func(ctx context.Context, run openlinker.RuntimeContext) (openlinker.RuntimeResult, error) {
		if err := run.Emit("run.message.delta", map[string]any{"text": "working"}); err != nil {
			return openlinker.RuntimeResult{}, err
		}
		return openlinker.RuntimeResult{Output: map[string]any{"answer": 42}}, nil
	}),
})
if err != nil {
	log.Fatal(err)
}
if err := worker.Start(context.Background()); err != nil {
	log.Fatal(err)
}
```

Production workers should use the default `FileRuntimeStore` through `DataDir`,
or inject another durable `RuntimeStore`. An in-memory store is suitable only
for explicit tests. `NodeVersion` defaults to `openlinker-go/runtime-worker` and
can be set when the host binary has its own enrolled version.

The canonical WebSocket endpoint is `/api/v1/agent-runtime/ws`; HTTP methods
use the `/api/v1/agent-runtime/` prefix. Protocol negotiation remains in the
handshake contract, not in public API names or URLs.

`NewRuntime` exposes the strict HTTP and WebSocket protocol client beneath the
worker. Applications that need those primitives can build on:

- `DialRuntimeWebSocket`, typed assignment/command channels, correlated ACKs,
  lease renewal, Event/Result submission, resume, and cancellation ACK
- HTTP `CreateRuntimeSession`, heartbeat, close, long-poll claim, explicit
  assignment ACK/reject, command poll, and lease renewal
- durable Event and Result submission with caller-supplied stable IDs
- resume and explicit-session cancellation command polling/acknowledgement
- assignment-scoped delegated calls with exact-body invocation proofs

`openlinker-agent-node` is an optional Adapter shell. It injects an HTTP,
command, Codex, or A2A handler into this SDK worker; it does not own a second
Runtime state machine.

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

gRPC is an A2A transport binding. It does not replace OpenLinker Runtime.


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
logs and public issue reports. Use `OPENLINKER_USER_TOKEN` with `NewClient` and
`OPENLINKER_AGENT_TOKEN` with `NewRuntimeWorker`. Protect the Runtime client key
and its encrypted spool key separately. Verify webhook signatures before trusting
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
