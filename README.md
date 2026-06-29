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
[SECURITY.md](./SECURITY.md) for vulnerability reporting.

## License

Apache-2.0. See [LICENSE](./LICENSE).
