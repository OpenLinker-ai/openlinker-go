# OpenLinker Go Runtime API Modes

This document freezes the public API layering targeted by `v0.2.0`. The API
mode describes how much Runtime behavior the SDK manages; it is independent
from whether the connection uses WebSocket or HTTP.

## 1. Minimal Agent

Use `WithAgent` or `WithFunc`. The SDK owns configuration, mTLS, durable state,
session lifecycle, assignment handling, lease, cancel, resume, reconnect and
graceful shutdown.

Primary entry points:

- `WithAgent(agent).Run(ctx)`
- `WithFunc(fn).Run(ctx)`

## 2. Explicit Registration and Run

Registration is an opt-in capability attached to Minimal Agent or Native
Runtime. Merely setting a User Token never creates platform resources.

Primary entry points:

- `WithAgent(agent).RunOrRegister(ctx, spec, options...)`
- `WithAgent(agent).WithRegistration(spec, options...).Run(ctx)`
- `Native(handler).WithRegistration(spec, options...).Run(ctx)`

## 3. Native Runtime

Use `Native` when the handler needs Assignment metadata, stable attempt/run
identity, custom events, message deltas, delegation, cancellation/deadlines or
custom success/failure results. The SDK still owns the managed worker lifecycle.

Primary entry points:

- `Native(handler).Run(ctx)`
- `Native(handler).RunOrRegister(ctx, spec, options...)`
- `NativeRun.Emit`, `NativeRun.MessageDelta`, `NativeRun.Progress`
- `Success`, `Failure`, `RetryableFailure`

## 4. Managed Worker and Protocol Primitives

`RuntimeWorker` is the advanced embeddable lifecycle engine. `NewRuntime` and
the Runtime HTTP/WebSocket methods are the lowest protocol layer for custom
infrastructure. Protocol users own journal, lease, spool, resume, cancel,
reconnect and transport switching if they bypass `RuntimeWorker`.

## Transport Modes

- `TransportAuto`: WebSocket first, HTTP fallback for transport failures, then
  safe promotion after WebSocket recovery.
- `TransportWebSocket`: strict WebSocket only.
- `TransportHTTP`: strict HTTP long-poll only.

Authentication, mTLS, permission and contract errors never trigger fallback.

## Compatibility Window

The `runtime_ws` / `runtime_pull` values, `RuntimeConnector*` constants and
`WithConnector` remain accepted by the high-level facade during the `v0.2.x`
migration window. New code should use `TransportMode` and `WithTransportMode`.
The protocol surface intentionally uses generation-free `Runtime*` names.
