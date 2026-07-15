# OpenLinker Go SDK 示例

该目录按“API 大类 + 单一概念示例”组织。所有示例共用一个 Go module，并通过本地 `replace` 测试当前工作树中的 SDK，不依赖尚未发布的版本。

## 从哪里开始

| 使用者 | 推荐入口 | 状态 |
|---|---|---|
| 普通 Agent 开发者 | [`runtime/agent-generic`](runtime/agent-generic/) | 可运行 |
| 调用 OpenLinker Agent 的 Client | [`client`](client/) | 后续阶段补齐 |
| 需要自动注册 Agent | [`registration`](registration/) | 后续阶段补齐 |
| Agent 框架开发者 | [`runtime`](runtime/) 中的 Native 示例 | 后续阶段补齐 |
| Runtime 基础设施开发者 | [`runtime`](runtime/) 中的 Managed Worker / Protocol 示例 | 后续阶段补齐 |
| A2A 或 Webhook 集成开发者 | [`a2a`](a2a/) / [`webhook`](webhook/) | 后续阶段补齐 |

## 当前可运行示例

### 极简 Agent

只实现 `Run(context.Context, string)`，Runtime 生命周期由 SDK 管理：

```bash
cd example
go run ./runtime/agent-generic
```

运行前需要配置 Agent Runtime 身份和 mTLS 文件，完整变量见 [`runtime/agent-generic/README.md`](runtime/agent-generic/README.md)。

## 离线验证

示例 module 可以独立编译和测试：

```bash
cd example
go test ./...
go vet ./...
```

示例测试使用本地 handler、`httptest` 或 fake transport，不要求连接真实 OpenLinker Core。需要创建平台资源或连接测试租户的 smoke test 会保持显式 opt-in。

## 目录约束

- 每个叶子目录只演示一个核心概念，普通示例以单个 `main.go` 为主。
- `internal/exampleutil` 只处理环境变量、signal、JSON 输出等样板代码，不封装 `NewClient`、`RunAgent`、`WithAgent` 等需要直接展示的 SDK API。
- 示例不硬编码 Token、Agent ID、证书路径或生产 endpoint。
- Registration 和 Token 管理等会改变平台状态的操作必须显式启用。
