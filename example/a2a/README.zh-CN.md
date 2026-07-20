# A2A 示例

[English](README.md)

每个目录只覆盖一种 A2A binding，不与 Agent Runtime 的 WebSocket/HTTP transport 混为一谈。

| 目录 | Binding | 命令 |
|---|---|---|
| `jsonrpc` | A2A JSON-RPC `SendMessage` | `go run ./a2a/jsonrpc` |
| `http-json-sse` | A2A HTTP+JSON 请求和 SSE stream | `go run ./a2a/http-json-sse` |
| `grpc` | A2A gRPC `SendMessage` | `go run ./a2a/grpc` |

JSON-RPC 和 HTTP+JSON/SSE 使用：

```bash
export OPENLINKER_A2A_ENDPOINT=https://api.openlinker.ai/api/v1/a2a/agents/my-agent
export OPENLINKER_A2A_TOKEN=ol_user_xxx # 端点要求认证时设置
export OPENLINKER_A2A_INPUT='hello'
```

gRPC 使用：

```bash
export OPENLINKER_A2A_GRPC_ENDPOINT=https://grpc.openlinker.ai
export OPENLINKER_A2A_TENANT=my-agent
export OPENLINKER_A2A_TOKEN=ol_user_xxx
go run ./a2a/grpc
```

实际调用前应从 Agent Card 的 interfaces 中选择对方声明支持的 binding。A2A gRPC 是 Agent 调用 binding，不替代 Agent Runtime。

离线测试分别验证 JSON-RPC method/auth、HTTP content negotiation/SSE，以及 gRPC tenant/metadata。
