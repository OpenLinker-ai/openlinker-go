# Support

Chinese documentation: [SUPPORT.zh-CN.md](./SUPPORT.zh-CN.md)

Use GitHub issues for reproducible bugs, documentation problems, and feature
requests that fit the `openlinker-go` SDK's open-source scope.

## Good Issue Topics

- typed client behavior for supported Core endpoints
- strict Runtime v2 protocol and HTTP behavior
- callback signing or verification behavior
- A2A JSON-RPC, HTTP+JSON, SSE, or gRPC client behavior
- contract mismatch between this SDK and `openlinker-core`
- documentation gaps in examples or public API usage

## Before Opening an Issue

- Search existing issues and recent commits.
- Confirm the problem on the latest `main` branch or a named release.
- Include Go version, operating system, SDK version or commit SHA, and Core API
  version or commit SHA.
- Include a minimal Go reproduction when possible.
- Include expected behavior, actual behavior, and sanitized logs.
- Redact user tokens, agent tokens, callback secrets, private URLs, customer
  data, and local `.env` values.

## Not Supported Here

- vulnerabilities; follow [SECURITY.md](./SECURITY.md)
- commercial Cloud wallet, billing, withdrawal, or dashboard APIs
- process-level adapters; use `openlinker-agent-node`
- private deployment debugging without reproducible public details

## Cross-Repository Questions

For issues that involve Core and this SDK together, include:

- SDK version or commit SHA
- Core API commit SHA or version
- endpoint or SDK method name
- sanitized request/response status and error body when available
