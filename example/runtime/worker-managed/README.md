# Managed RuntimeWorker example

[简体中文](README.zh-CN.md)

This example shows how infrastructure code can call `NewRuntimeWorker`
directly and select the store, capacity, logger, transport, and handler.

Before running it, prepare an Agent, Agent Token, stable Node and Agent IDs, and
a private durable directory. Runtime security is selected by platform discovery. Follow
[the complete RuntimeWorker guide](../../../runtime-worker-end-to-end.md).

From the `example/` module:

```bash
export OPENLINKER_API_BASE='https://openlinker.example'
export OPENLINKER_NODE_ID='11111111-1111-4111-8111-111111111111'
export OPENLINKER_AGENT_ID='22222222-2222-4222-8222-222222222222'
export OPENLINKER_AGENT_TOKEN='<read from a secret store>'
export OPENLINKER_RUNTIME_DATA_DIR='/var/lib/my-agent/runtime'
export OPENLINKER_RUNTIME_TRANSPORT='auto'

go run ./runtime/worker-managed
```

Do not set certificate files for a token-only Runtime. When discovery requires
mTLS, use SDK-managed enrollment or provide the complete external-PKI group.

The example overrides capacity to 4, so the registered Runtime Node must allow
at least that capacity. Use 1 for a single-concurrency worker.

A Ready log is only the first check. Create a real Run, verify its Result,
cancel a Run, and restart the process before declaring a deployment ready.
