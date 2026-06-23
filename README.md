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

## Core Surface

Application-side calls:

- `ListAgents`
- `GetAgent`
- `GetAgentCard`
- `RunAgent`
- `StartAgentRun`
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
