# Runtime examples

[简体中文](README.zh-CN.md)

Before running a Runtime example for the first time, follow
[the RuntimeWorker operations guide](../../runtime-worker-end-to-end.md) to
prepare the Agent, Runtime Node, mTLS files, a real Run, cancellation, and
restart checks.

Examples are organized by API level, not by network transport:

- `agent-generic`: smallest `WithAgent(...).Run()` entry for a normal Agent;
- `agent-register`: explicit `RunOrRegister` and `WithRegistration`;
- `native-events`: Native handler, message, progress, Event, and Result helpers;
- `native-delegation`: call another Agent with the current task's capability;
- `worker-managed`: custom store, capacity, logger, and transport;
- `protocol-http`: low-level Runtime HTTP methods;
- `protocol-websocket`: low-level Runtime WebSocket messages.

Start with `agent-generic` unless you are building a framework or Runtime
infrastructure. Low-level protocol examples do not manage Session, lease,
journal, retry, or reconnect for you.

| Directory | Intended user | Main API |
| --- | --- | --- |
| `agent-generic` | Normal Agent | `WithAgent(...).Run()` |
| `agent-register` | First deployment or local demo | `RunOrRegister` |
| `native-events` | Agent framework | `Native`, message, progress, Event |
| `native-delegation` | Multi-Agent framework | task-scoped `CallAgent` |
| `worker-managed` | Agent Node or daemon | `NewRuntimeWorker` |
| `protocol-http` | Protocol implementer | Runtime HTTP methods |
| `protocol-websocket` | Protocol implementer | Runtime WebSocket messages |

Common settings:

```bash
export OPENLINKER_RUNTIME_BASE=https://runtime.openlinker.ai
export OPENLINKER_NODE_ID=11111111-1111-4111-8111-111111111111
export OPENLINKER_AGENT_ID=22222222-2222-4222-8222-222222222222
export OPENLINKER_AGENT_TOKEN=ol_agent_xxx
export OPENLINKER_NODE_CERT_FILE=/run/openlinker/node.crt
export OPENLINKER_NODE_KEY_FILE=/run/openlinker/node.key
export OPENLINKER_RUNTIME_CA_FILE=/run/openlinker/runtime-ca.crt
```

Registration also needs `OPENLINKER_API_BASE`; first registration needs
`OPENLINKER_USER_TOKEN`. Native delegation additionally needs
`OPENLINKER_TARGET_AGENT_ID`.

```bash
go run ./runtime/agent-register
go run ./runtime/native-events
go run ./runtime/native-delegation
go run ./runtime/worker-managed
```

The protocol examples claim and explicitly reject a task. This avoids running
business work after an ACK without a durable journal:

```bash
go run ./runtime/protocol-http
go run ./runtime/protocol-websocket
```

A production protocol implementation must provide stable Worker and Session
identity, assignment journal, lease renewal, Event/Result IDs, durable pending
delivery, resume, cancellation, and reconnect. Normal Agents should use the
high-level worker instead of copying protocol examples.

Offline tests use a fake Core or local protocol server and do not contact a
real deployment.
