# Smallest OpenLinker Agent

[简体中文](README.zh-CN.md)

This is the recommended SDK entry for a normal Agent:

```go
openlinker.WithAgent(agent).Run(ctx)
```

The Agent implements only:

```go
Run(context.Context, string) (string, error)
```

The SDK manages environment configuration, mTLS, Runtime Session, tasks,
leases, durable pending delivery, resume, reconnect, and Result mapping.

## Run

```bash
OPENLINKER_API_BASE=https://api.openlinker.ai \
OPENLINKER_NODE_ID=11111111-1111-4111-8111-111111111111 \
OPENLINKER_AGENT_ID=22222222-2222-4222-8222-222222222222 \
OPENLINKER_AGENT_TOKEN=ol_agent_xxx \
OPENLINKER_NODE_CERT_FILE=/run/openlinker/node.crt \
OPENLINKER_NODE_KEY_FILE=/run/openlinker/node.key \
OPENLINKER_RUNTIME_CA_FILE=/run/openlinker/runtime-ca.crt \
OPENLINKER_RUNTIME_TRANSPORT=auto \
go run ./runtime/agent-generic
```

Run the command from the shared `example/` module. To require WebSocket:

```bash
OPENLINKER_RUNTIME_TRANSPORT=ws go run ./runtime/agent-generic
```

## Agent settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `GENERIC_AGENT_NAME` | `Generic Agent` | Agent name included in the text response. |
| `GENERIC_AGENT_PREFIX` | empty | Return `<prefix> <input>` when set. |
| `GENERIC_AGENT_PANIC` | `false` | Set to `1` or `true` to test panic recovery. |

## Runtime settings

| Variable | Default | Meaning |
| --- | --- | --- |
| `OPENLINKER_NODE_ID` | required | Registered Runtime Node UUID. |
| `OPENLINKER_AGENT_ID` | required | Agent UUID. |
| `OPENLINKER_AGENT_TOKEN` | required | Agent credential used by Runtime. |
| `OPENLINKER_API_BASE` | required without a Runtime base | Platform URL used for Runtime discovery. |
| `OPENLINKER_RUNTIME_BASE` | empty | Explicit Runtime URL override. |
| `OPENLINKER_RUNTIME_DATA_DIR` | `.openlinker/runtime-<agent-id>` | Encrypted identity, journal, and pending delivery directory. |
| `OPENLINKER_RUNTIME_TRANSPORT` | `auto` | `auto`, `ws`, or `pull`. |
| `OPENLINKER_NODE_CERT_FILE` | required | Runtime Node mTLS certificate. |
| `OPENLINKER_NODE_KEY_FILE` | required | Runtime Node private key. |
| `OPENLINKER_RUNTIME_CA_FILE` | required | CA used to verify Runtime. |

The SDK reads the first non-empty `text`, `query`, `task`, or `prompt`
field, and also accepts a plain string.

## Panic recovery

```bash
GENERIC_AGENT_PANIC=1 go run ./runtime/agent-generic
```

After a Run is sent, the SDK maps the panic to a failed Result instead of
crashing the worker. The process can continue with later tasks.
