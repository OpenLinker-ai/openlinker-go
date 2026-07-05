# Contributing to openlinker-go

Chinese documentation: [CONTRIBUTING.zh-CN.md](./CONTRIBUTING.zh-CN.md)

Thanks for helping improve `openlinker-go`, the Go SDK for OpenLinker Core
APIs, runtime connectors, callbacks, and A2A transports.

## Development Setup

```bash
go test ./...
```

Use placeholder tokens in tests and examples. Never commit real user tokens,
agent tokens, callback secrets, private endpoints, or captured customer data.

## Scope Boundaries

Allowed here:

- typed wrappers for open-source Core API surfaces
- runtime pull/WebSocket connector helpers
- callback construction and signature verification helpers
- A2A JSON-RPC, HTTP+JSON, SSE, and gRPC client behavior
- contract tests and generated protobuf artifacts used by this SDK

Out of scope:

- Cloud wallet, billing, Stripe, withdrawal, and commercial dashboard APIs
- hosted marketplace ranking or private recommendation internals
- process-level adapters such as command, Codex, OpenClaw, or local backend
  runners; use `openlinker-agent-node` for those

## Pull Request Expectations

- Keep exported API changes small and documented.
- Add or update tests for client behavior, callbacks, runtime connectors, or
  A2A transports.
- Keep generated protobuf files aligned with `proto/`.
- Update `README.md` and `CHANGELOG.md` for public behavior changes.
- Preserve backwards compatibility unless the change is clearly documented as
  pre-1.0 breaking behavior.

## Checks

```bash
gofmt -w .
go test ./...
```

## Security

Do not open public issues for vulnerabilities. Follow [SECURITY.md](./SECURITY.md).

## License

By contributing, you agree that your contribution is licensed under the
Apache-2.0 license used by this repository.
