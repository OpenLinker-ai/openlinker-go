# Client examples

[简体中文](README.zh-CN.md)

Run every command from the shared `example/` module.

| Directory | Main API | Creates a Run |
| --- | --- | ---: |
| `agent-discovery` | `ListAgents`, `GetAgent`, `GetAgentCard` | No |
| `run-sync` | `RunAgent` with an idempotency key | Yes |
| `run-async` | `StartAgentRun`, `GetRun`, polling and timeout | Yes |
| `run-stream` | `StreamRunEvents`, SSE terminal events and disconnects | Yes |
| `run-callbacks` | `RunAgentWithCallbacks`, message and terminal callbacks | Yes |
| `run-history` | Events, messages, artifacts, children, and retention metadata | No |

Common settings:

```bash
export OPENLINKER_API_BASE=https://api.openlinker.ai
export OPENLINKER_USER_TOKEN=ol_user_xxx
```

Agent discovery:

```bash
export OPENLINKER_AGENT_SLUG=my-agent
export OPENLINKER_AGENT_QUERY=my-agent # optional
go run ./client/agent-discovery
```

Examples that create a Run:

```bash
export OPENLINKER_AGENT_ID=22222222-2222-4222-8222-222222222222
export OPENLINKER_RUN_INPUT='hello'
export OPENLINKER_IDEMPOTENCY_KEY='demo-intent-20260715-001'
```

One idempotency key represents one intended Run creation. Reuse it when
retrying the same request after a network failure. Generate a new key for a
new task.

Async, streaming, and callback examples also accept:

```bash
export OPENLINKER_RUN_TIMEOUT=30s
export OPENLINKER_POLL_INTERVAL=1s # run-async only
```

Read an existing Run:

```bash
export OPENLINKER_RUN_ID=33333333-3333-4333-8333-333333333333
export OPENLINKER_AFTER_SEQUENCE=0
go run ./client/run-history
```

Each directory has an offline `httptest` that checks paths, Bearer
authentication, idempotency headers, JSON, SSE, terminal handling, and event
retention metadata.
