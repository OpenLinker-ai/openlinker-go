# Changelog

All notable changes to `openlinker-go` will be documented in this file.

This SDK is currently pre-1.0. Breaking changes may happen before the Core API,
runtime connector, callback, and A2A contracts are declared stable.

## Unreleased

### Added

- Added a strict Runtime v2 WebSocket client that reuses the configured mTLS
  transport and Agent Token, performs `runtime.hello` / `runtime.ready`, uses a
  single writer, enforces the 4 MiB envelope limit, correlates business ACKs by
  `reply_to_message_id`, supports multi-message resume, and exposes typed
  assignment, cancellation, drain, and lease-revocation pushes.
- Added an authenticated WebSocket reachability probe for durable workers that
  use v2 HTTP long-poll as a restricted-network fallback.

### Changed

- Made Core run creation idempotent by sending `Idempotency-Key` from
  `RunAgent` and `StartAgentRun`. The SDK generates a safe random key when the
  caller does not provide one and exposes Core replay results through
  `RunResponse.Replayed`.
- Aligned `ListRunEvents` with Core's retained event-page contract. Responses
  now expose `Items` and typed retention metadata; the legacy `Events` field
  has been removed.

### Removed

- Breaking: removed the pre-v2 heartbeat, pull claim/result, unversioned
  delegated-call API, v1 pull/WebSocket connectors, Native runners,
  Blades wrapper, and legacy runtime examples. `Runtime` now exposes strict v2
  primitives only; reliable process execution belongs in Agent Node.

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
