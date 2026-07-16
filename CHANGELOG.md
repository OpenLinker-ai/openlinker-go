# Changelog

All notable changes to `openlinker-go` will be documented in this file.

This SDK is currently pre-1.0. Breaking changes may happen before the Core API,
runtime connector, callback, and A2A contracts are declared stable.

## [v0.2.0-rc.1] - Unreleased

This release candidate is the breaking preview for the `v0.2.0` Runtime API
layering. Pin the exact RC tag during validation; do not depend on `main`,
`latest`, a committed local `replace`, or a vendored SDK copy.

### Added

- Added the high-level `WithAgent`, `WithFunc`, and `Native` facades over the
  canonical reliable `RuntimeWorker`, including event/progress helpers,
  assignment context, retryable failures, explicit registration, environment
  configuration, and `RunOrRegister`.
- Added creator and pending-token Agent registration clients, `AgentSpec`,
  registration policies, and a durable `.env` registration store.
- Added `RuntimeWorker`, the SDK-owned reliable worker for Runtime discovery,
  mTLS, WebSocket and HTTP pull recovery, Session lifecycle, assignment
  confirmation, lease renewal, resume, cancellation, drain, and encrypted
  durable Event/Result delivery.
- Added a strict Runtime WebSocket client that reuses the configured mTLS
  transport and Agent Token, performs `runtime.hello` / `runtime.ready`, uses a
  single writer, enforces the 4 MiB envelope limit, correlates business ACKs by
  `reply_to_message_id`, supports multi-message resume, and exposes typed
  assignment, cancellation, drain, and lease-revocation pushes.
- Added an authenticated WebSocket reachability probe for durable workers that
  use HTTP long-poll as a restricted-network fallback.
- Added a shared `example` module with focused Client, registration, Runtime,
  A2A, and webhook demos, including one example for each Runtime API layer.

### Changed

- Reconciled the high-level API-mode work with the remote reliable worker. The
  encrypted `RuntimeStore`, transport generation fencing, assignment journal,
  spool, resume, cancel, and drain implementations remain the single canonical
  state machine; the high-level APIs are facades rather than a second worker.
- Breaking: standardized every Runtime HTTP and WebSocket endpoint under
  `/api/v1/agent-runtime/*`; Runtime URLs no longer carry a protocol generation.
  The wire handshake keeps protocol version 2 and the
  `openlinker.runtime.v2` contract ID. Public Go types and methods now use
  generation-free `Runtime*` names. Pull requests after Session creation now
  carry the exact attachment generation so a replaced transport cannot mutate
  Runtime state; `call-agent` remains assignment-capability-bound. The contract
  digest is `3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9`.
- Made Core run creation idempotent by sending `Idempotency-Key` from
  `RunAgent` and `StartAgentRun`. The SDK generates a safe random key when the
  caller does not provide one and exposes Core replay results through
  `RunResponse.Replayed`.
- Aligned `ListRunEvents` with Core's retained event-page contract. Responses
  now expose `Items` and typed retention metadata; the legacy `Events` field
  has been removed.
- Changed the recommended Agent entry point to `WithAgent(...).Run()` and the
  framework entry point to `Native(handler)`. `RuntimeWorker` remains the
  advanced managed lifecycle layer, while `NewRuntime` is reserved for callers
  that deliberately own protocol state.
- Changed the default transport to `TransportAuto`: WebSocket is preferred,
  HTTP long-poll is used only for transport availability, and authentication,
  mTLS, permission, payload, and contract errors never trigger fallback.
- Changed the default durable Runtime data directory to
  `.openlinker/runtime-<agent-id>` when an Agent ID is available. The default
  store is encrypted and persists assignment journal, Event spool, Result
  spool, Worker ID, Runtime Session identity, and resume state.
- Changed Runtime credentials to the standard `OPENLINKER_AGENT_ID` and
  `OPENLINKER_AGENT_TOKEN` names. `OPENLINKER_RUNTIME_STATE_PATH` remains only
  as a facade compatibility input; new code should use
  `OPENLINKER_RUNTIME_DATA_DIR`.
- Changed Agent registration to use Core's transport-neutral
  `connection_mode=runtime`. Legacy `runtime_ws`, `runtime_pull`, and
  `agent_node` registration inputs are normalized to `runtime`; WebSocket/HTTP
  selection remains a separate Runtime transport option.

### Removed

- Breaking: removed the `WithRuntimeToken` compatibility alias. Runtime
  clients now accept Agent credentials only through `WithAgentToken`.
- Breaking: removed the legacy heartbeat, pull claim/result, delegated-call
  API, pull/WebSocket connectors, Blades wrapper, and legacy Runtime examples.
  `Runtime` exposes strict protocol primitives, `RuntimeWorker` owns reliable
  process execution, and the new `Native` facade adapts framework handlers to
  that worker without restoring the legacy state machine.

### Compatibility

- `WithConnector`, `runtime_ws`, `runtime_pull`, and the legacy connector
  constants remain accepted by the high-level facade for the `v0.2.x`
  migration window. New code should use `WithTransportMode` with
  `TransportAuto`, `TransportWebSocket`, or `TransportHTTP`.
- `EnsureAgentRequest` remains as a deprecated registration wrapper for one
  release cycle. New code should use `AgentSpec` plus registration options.
- Runtime v1 routes, types, connectors, and fallback are not restored by this
  compatibility window.

### Migration

- Ordinary Agents: replace connector construction with
  `openlinker.WithAgent(agent).Run(ctx)`.
- Framework Agents: bind the business handler with
  `openlinker.Native(agent.Handle).Run(ctx)` and use `NativeRun` helpers for
  identity, metadata, events, progress, delegation, deadlines, and results.
- Explicit bootstrap: use `RunOrRegister` or `WithRegistration`; merely setting
  a User Token never creates platform resources.
- `openlinker-agent-layout`: let the SDK own Runtime lifecycle, transport,
  resume, and durable Store; retain Harness, trace, approval, artifact, memory,
  and business logic in layout. See
  `docs/openlinker-agent-layout-migration.zh-CN.md`.
- Downstream modules should require `v0.2.0-rc.1`, remove committed vendor
  copies, and remove temporary local `replace` directives after the RC tag is
  available through the normal Go module path.

### Rollback

- Before the RC tag is published, revert the candidate commit and rerun the
  full SDK/example/downstream test matrix.
- After publication, never move or overwrite the RC tag. Publish a newer
  `v0.2.0-rc.N` for fixes and let downstreams pin the previous verified tag if
  rollback is required.
- Preserve Runtime data directories and unacknowledged spool during rollback.
  Do not point an older binary at a newer Store format; use a separate data
  directory for the rolled-back binary.
- Rollback does not re-enable Runtime v1. If a downstream must return to a
  legacy connector, roll back that complete downstream binary and SDK version.

### Documentation

- Split Chinese documentation into dedicated `*.zh-CN.md` files and kept the
  default GitHub-facing documentation English-only.
- Strengthened the README introduction for Go SDK, AI agent registry, agent
  marketplace, A2A/MCP runtime gateway, callbacks, runtime connectors, and gRPC
  discoverability.
- Expanded the README into an English-first open-source entry point with a
  Chinese overview, install instructions, quick start, run examples, callback
  verification, runtime connectors, A2A transports, development, security, and
  contribution guidance.
- Expanded contributing, security, support, and release documents for public Go
  SDK use.
- Documented that process-level adapters belong in `openlinker-agent-node` and
  commercial Cloud APIs are outside this SDK's scope.
- Reworked the first README screen around the minimal Agent facade, documented
  the four API modes and transport orthogonality in English and Chinese, and
  added Chinese layout migration and RC release checklists.

### Repository

- Added open-source governance files, issue templates, pull request template,
  and CI workflow.
- Added Apache-2.0 license, contributing guide, security policy, code of
  conduct, and support guidance.
