# OpenLinker Generic Minimal Agent

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
OPENLINKER_NODE_ID=11111111-1111-4111-8111-111111111111 \
OPENLINKER_AGENT_ID=22222222-2222-4222-8222-222222222222 \
OPENLINKER_AGENT_TOKEN=ol_agent_xxx \
OPENLINKER_NODE_CERT_FILE=/run/openlinker/node.crt \
OPENLINKER_NODE_KEY_FILE=/run/openlinker/node.key \
OPENLINKER_RUNTIME_CA_FILE=/run/openlinker/runtime-ca.crt \
OPENLINKER_RUNTIME_TRANSPORT=pull \
go run .
```

Use strict WebSocket mode when required:

```bash
OPENLINKER_RUNTIME_TRANSPORT=ws go run .
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
| `OPENLINKER_NODE_ID` | required | Registered Node UUID. |
| `OPENLINKER_AGENT_ID` | required | Agent UUID. |
| `OPENLINKER_AGENT_TOKEN` | required | Agent Runtime credential. |
| `OPENLINKER_API_BASE` | required unless Runtime base is explicit | Public platform origin used for Runtime discovery. |
| `OPENLINKER_RUNTIME_BASE` | empty | Explicit dedicated Runtime origin override. |
| `OPENLINKER_RUNTIME_DATA_DIR` | `.openlinker/runtime-<agent-id>` | Encrypted identity/journal/spool directory. |
| `OPENLINKER_RUNTIME_TRANSPORT` | `auto` | `auto`, `ws`, or `pull`. |
| `OPENLINKER_NODE_CERT_FILE` | required | Node mTLS certificate. |
| `OPENLINKER_NODE_KEY_FILE` | required | Node mTLS private key. |
| `OPENLINKER_RUNTIME_CA_FILE` | required | Runtime server CA. |

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
GENERIC_AGENT_PANIC=1 \
go run .
```

Trigger one run from OpenLinker. The worker remains available for later runs,
and the failed result should contain:

```json
{
  "status": "failed",
  "error": {
    "code": "AGENT_RUNTIME_PANIC",
    "message": "agent panic: generic agent panic requested by GENERIC_AGENT_PANIC"
  }
}
```
