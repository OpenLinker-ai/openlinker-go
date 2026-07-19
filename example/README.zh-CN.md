# OpenLinker Go SDK 示例

[English](README.md)

该目录按“API 大类 + 单一概念示例”组织。所有示例共用一个 Go module，并通过本地 `replace` 测试当前工作树中的 SDK，不依赖尚未发布的版本。

## 从哪里开始

| 使用者 | 推荐入口 | 状态 |
|---|---|---|
| 普通 Agent 开发者 | [`runtime/agent-generic`](runtime/agent-generic/) | 可运行 |
| 调用 OpenLinker Agent 的 Client | [`client`](client/) | 可运行 |
| 需要自动注册 Agent | [`registration`](registration/) | 可运行 |
| Agent 框架开发者 | [`runtime`](runtime/) 中的 Native 示例 | 可运行 |
| Runtime 基础设施开发者 | [`runtime`](runtime/) 中的 Managed Worker / Protocol 示例 | 可运行 |
| A2A 或 Webhook 集成开发者 | [`a2a`](a2a/) / [`webhook`](webhook/) | 可运行 |

## 当前可运行示例

### Client 调用

Client 调用方建议依次查看 Agent discovery、同步 Run 和异步/流式 Run：

```bash
cd example
go run ./client/agent-discovery
go run ./client/run-sync
```

六个 Client 示例的用途、环境变量和资源影响见 [`client/README.md`](client/README.md)。

### Agent 注册和 Token 管理

`registration/ensure-agent` 显式创建或复用 Agent；`registration/token-management` 默认只读，写操作需要 `--confirm-write`：

```bash
cd example
go run ./registration/ensure-agent
go run ./registration/token-management
```

运行前请先阅读 [`registration/README.md`](registration/README.md) 中的资源影响和 Token 安全说明。

### 极简 Agent

只实现 `Run(context.Context, string)`，Runtime 生命周期由 SDK 管理：

```bash
cd example
go run ./runtime/agent-generic
```

运行前需要配置 Agent Runtime 身份和 mTLS 文件，完整变量见 [`runtime/agent-generic/README.md`](runtime/agent-generic/README.md)。

框架开发者继续查看 [`runtime/native-events`](runtime/native-events/)；Agent Node/daemon 查看 [`runtime/worker-managed`](runtime/worker-managed/)；只有协议实现者才需要查看 `protocol-http` 和 `protocol-websocket`。完整分层见 [`runtime/README.md`](runtime/README.md)。

### A2A 和 Webhook

```bash
cd example
go run ./a2a/jsonrpc
go run ./a2a/http-json-sse
go run ./a2a/grpc
go run ./webhook/verify-request
```

A2A binding 选择见 [`a2a/README.md`](a2a/README.md)，Webhook 原始请求体验签见 [`webhook/README.md`](webhook/README.md)。

## 离线验证

示例 module 可以独立编译和测试：

```bash
cd example
go test ./...
go vet ./...
```

示例测试使用本地 handler、`httptest` 或 fake transport，不要求连接真实 OpenLinker Core。需要创建平台资源或连接测试租户的 smoke test 会保持显式 opt-in。

发布前测试租户 smoke 流程见 [`smoke/README.md`](smoke/README.md)。脚本要求 `OPENLINKER_EXAMPLE_SMOKE=1`，不会在普通 test/CI 中自动运行。

## 目录约束

- 每个叶子目录只演示一个核心概念，普通示例以单个 `main.go` 为主。
- `internal/exampleutil` 只处理环境变量、signal、JSON 输出等样板代码，不封装 `NewClient`、`RunAgent`、`WithAgent` 等需要直接展示的 SDK API。
- 示例不硬编码 Token、Agent ID、证书路径或生产 endpoint。
- Registration 和 Token 管理等会改变平台状态的操作必须显式启用。
