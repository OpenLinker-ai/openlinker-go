# OpenLinker Go SDK examples

[简体中文](README.zh-CN.md)

Examples are grouped by API area and by one main concept. They share one Go
module and use a local `replace` directive so tests exercise the current SDK
worktree rather than an unpublished version.

## Where to start

| You want to | Start here |
| --- | --- |
| Build a normal Agent | [`runtime/agent-generic`](runtime/agent-generic/) |
| Call an OpenLinker Agent | [`client`](client/) |
| Create or reuse Agent registration | [`registration`](registration/) |
| Integrate an Agent framework | Native examples under [`runtime`](runtime/) |
| Build Runtime infrastructure | Managed and protocol examples under [`runtime`](runtime/) |
| Integrate A2A or Webhooks | [`a2a`](a2a/) or [`webhook`](webhook/) |

## Client calls

```bash
cd example
go run ./client/agent-discovery
go run ./client/run-sync
```

[The Client guide](client/README.md) explains all six examples, their
environment variables, and which ones create Runs.

## Agent registration

`registration/ensure-agent` creates or reuses an Agent.
`registration/token-management` is read-only unless you pass
`--confirm-write`.

```bash
cd example
go run ./registration/ensure-agent
go run ./registration/token-management
```

Read [the registration guide](registration/README.md) before running a command
that can change platform resources.

## Smallest Agent

This example implements only `Run(context.Context, string)`; the SDK manages
the Runtime lifecycle:

```bash
cd example
go run ./runtime/agent-generic
```

See [the generic Agent guide](runtime/agent-generic/README.md) for the Agent
identity, Runtime Node, and mTLS settings.

Framework authors can continue with `runtime/native-events`. Agent Node and
daemon authors can use `runtime/worker-managed`. Only protocol implementers
should begin with `protocol-http` or `protocol-websocket`.

## A2A and Webhooks

```bash
cd example
go run ./a2a/jsonrpc
go run ./a2a/http-json-sse
go run ./a2a/grpc
go run ./webhook/verify-request
```

See [the A2A guide](a2a/README.md) for transport selection and
[the Webhook guide](webhook/README.md) for raw-body signature verification.

## Offline checks

```bash
cd example
go test ./...
go vet ./...
```

These tests use local handlers, `httptest`, or fake transports. They do not
need a real Core. Smoke tests that create resources or contact a test tenant
are opt-in; see [smoke/README.md](smoke/README.md).

## Example rules

- Each leaf directory demonstrates one main concept.
- `internal/exampleutil` handles only repetitive environment, signal, and JSON
  work; it does not hide the SDK call being demonstrated.
- Examples never hard-code real Tokens, Agent IDs, certificate paths, or
  production addresses.
- Registration and Token commands that change platform state require an
  explicit opt-in.
