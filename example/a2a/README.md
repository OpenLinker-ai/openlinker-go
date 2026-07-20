# A2A examples

[简体中文](README.zh-CN.md)

Each directory demonstrates one A2A binding. A2A bindings are independent from
the WebSocket or HTTP transport used by an Agent Runtime worker.

| Directory | Binding | Command |
| --- | --- | --- |
| `jsonrpc` | A2A JSON-RPC `SendMessage` | `go run ./a2a/jsonrpc` |
| `http-json-sse` | A2A HTTP+JSON and SSE | `go run ./a2a/http-json-sse` |
| `grpc` | A2A gRPC `SendMessage` | `go run ./a2a/grpc` |

JSON-RPC and HTTP+JSON/SSE:

```bash
export OPENLINKER_A2A_ENDPOINT=https://api.openlinker.ai/api/v1/a2a/agents/my-agent
export OPENLINKER_A2A_TOKEN=ol_user_xxx # only when the endpoint requires authentication
export OPENLINKER_A2A_INPUT='hello'
```

gRPC:

```bash
export OPENLINKER_A2A_GRPC_ENDPOINT=https://grpc.openlinker.ai
export OPENLINKER_A2A_TENANT=my-agent
export OPENLINKER_A2A_TOKEN=ol_user_xxx
go run ./a2a/grpc
```

Before calling an Agent, read its Agent Card and choose a binding it declares.
A2A gRPC is a way to call an Agent; it does not replace OpenLinker Runtime.

Offline tests cover JSON-RPC method and authentication, HTTP content
negotiation and SSE, and gRPC tenant metadata.
