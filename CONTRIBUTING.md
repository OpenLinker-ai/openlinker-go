# Contributing to openlinker-go

`openlinker-go` is the Go SDK for OpenLinker Core APIs and runtime protocol
helpers.

## Setup

```bash
go test ./...
```

## Scope

- Keep this package focused on Core registry, run, A2A, MCP, and runtime
  protocol APIs.
- Do not add Cloud wallet, billing, Stripe, hosted marketplace ranking, or
  commercial dashboard APIs.
- Keep contract files and tests aligned with Core API changes.
- Use placeholders in tests and docs.

## Checks

```bash
gofmt -w .
go test ./...
```

