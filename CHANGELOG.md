# Changelog

All notable changes to `openlinker-go` will be documented in this file.

This SDK is currently pre-1.0. Breaking changes may happen before the Core API,
runtime connector, callback, and A2A contracts are declared stable.

## Unreleased

### Added

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

### Changed

- Breaking: standardized every Runtime HTTP and WebSocket endpoint under
  `/api/v1/agent-runtime/*`; Runtime URLs no longer carry a protocol generation.
  The wire handshake keeps protocol version 2 and the
  `openlinker.runtime.v2` contract ID. Public Go types and methods now use
  generation-free `Runtime*` names. The contract now binds session heartbeat
  and close, including the close request body and empty `204` response; its digest is
  `fb92bb6ddbc65bd3353b5d7c63ad148dd510e4d0ac0a6ca6110461d91e2dec53`.
- Made Core run creation idempotent by sending `Idempotency-Key` from
  `RunAgent` and `StartAgentRun`. The SDK generates a safe random key when the
  caller does not provide one and exposes Core replay results through
  `RunResponse.Replayed`.
- Aligned `ListRunEvents` with Core's retained event-page contract. Responses
  now expose `Items` and typed retention metadata; the legacy `Events` field
  has been removed.

### Removed

- Breaking: removed the `WithRuntimeToken` compatibility alias. Runtime
  clients now accept Agent credentials only through `WithAgentToken`.
- Breaking: removed the legacy heartbeat, pull claim/result, delegated-call
  API, pull/WebSocket connectors, Native runners, Blades wrapper, and legacy
  Runtime examples. `Runtime` now exposes the strict protocol primitives, while
  `RuntimeWorker` owns reliable process execution.

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

### Repository

- Added open-source governance files, issue templates, pull request template,
  and CI workflow.
- Added Apache-2.0 license, contributing guide, security policy, code of
  conduct, and support guidance.
