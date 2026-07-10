# OpenLinker Generic Native Agent

This is a minimal OpenLinker native Agent. It demonstrates the simplest SDK
shape:

```go
openlinker.WithAgent(agent).Run(ctx)
```

The Agent only implements:

```go
Run(context.Context, string) (string, error)
```

OpenLinker runtime setup, assignment handling, events, result mapping, and
completion are handled by the SDK.

## Run

```bash
OPENLINKER_API_BASE=https://api.openlinker.ai \
OPENLINKER_AGENT_TOKEN=ol_agent_xxx \
OPENLINKER_WORKER_CONNECTOR=runtime_pull \
go run .
```

Use WebSocket mode when the registered Agent uses `runtime_ws`:

```bash
OPENLINKER_WORKER_CONNECTOR=runtime_ws go run .
```

Stop after one assignment for local verification:

```bash
OPENLINKER_WORKER_MAX_RUNS=1 go run .
```

## Optional Agent Settings

| Variable | Default | Description |
| --- | --- | --- |
| `GENERIC_AGENT_NAME` | `Generic Agent` | Name used in the text response. |
| `GENERIC_AGENT_PREFIX` | empty | If set, replies with `<prefix> <input>`. |
| `GENERIC_AGENT_PANIC` | `false` | Set to `1` or `true` to panic inside the Agent for runtime failure testing. |

## OpenLinker Settings

| Variable | Default | Description |
| --- | --- | --- |
| `OPENLINKER_AGENT_TOKEN` | required | Agent token issued for this Agent. |
| `OPENLINKER_API_BASE` | `https://api.openlinker.ai` | OpenLinker API base URL. |
| `OPENLINKER_WORKER_CONNECTOR` | `runtime_pull` | `runtime_pull` or `runtime_ws`. |
| `OPENLINKER_WORKER_PULL_WAIT` | `25s` | Long-poll wait duration for pull mode. |
| `OPENLINKER_WORKER_MAX_RUNS` | `0` | Stop after this many completed runs. `0` means run forever. |

## Input

The SDK reads the first non-empty value from `text`, `query`, `task`, or
`prompt`. It also accepts a plain string input.

```json
{
  "text": "summarize this"
}
```

## Panic Recovery Test

Use this mode to verify that OpenLinker native runtime recovers Agent panics and
marks the run as failed instead of crashing the worker:

```bash
OPENLINKER_WORKER_MAX_RUNS=1 \
GENERIC_AGENT_PANIC=1 \
go run .
```

Trigger one run from OpenLinker. The worker should stay alive until the run is
completed, and the run result should contain:

```json
{
  "status": "failed",
  "error": {
    "code": "AGENT_RUNTIME_PANIC",
    "message": "agent panic: generic agent panic requested by GENERIC_AGENT_PANIC"
  }
}
```
