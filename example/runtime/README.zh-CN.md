# Runtime 示例

[English](README.md)

第一次运行任何 Runtime 示例前，先完成
[《从零运行一个 RuntimeWorker：完整操作手册》](../../runtime-worker-end-to-end.zh-CN.md)
中的 Agent 身份、Runtime Node、mTLS、真实 Run、cancel 和重启验证。

Runtime 示例按照 API 层级组织，而不是按照 HTTP/WebSocket 传输方式组织：

- `agent-generic`：极简 `WithAgent(...).Run()`，普通 Agent 的推荐入口。
- `agent-register`：显式 `RunOrRegister` / `WithRegistration`。
- `native-events`：Native handler、MessageDelta、Emit、Progress 和 Result helper。
- `native-delegation`：assignment-scoped Agent-to-Agent delegation。
- `worker-managed`：自定义 Store、capacity、logger 和 transport mode。
- `protocol-http`：底层 Runtime HTTP 原语。
- `protocol-websocket`：底层 Runtime WebSocket 原语。

普通 Agent 项目应从 `agent-generic` 开始；底层 protocol 示例不会替开发者管理 session、lease、spool、resume 和 reconnect。

## 运行顺序

| 目录 | 适用对象 | 核心 API | 推荐程度 |
|---|---|---|---|
| `agent-generic` | 普通 Agent | `WithAgent(...).Run()` | 首选 |
| `agent-register` | 首次部署、本地 Demo | `RunOrRegister` | 显式需要注册时使用 |
| `native-events` | Agent 框架 | `Native`、MessageDelta、Progress、Emit | 高级 |
| `native-delegation` | 多 Agent 框架 | assignment-scoped `CallAgent` | 高级 |
| `worker-managed` | Agent Node/daemon | `NewRuntimeWorker` | 基础设施 |
| `protocol-http` | 协议实现者 | Runtime HTTP primitives | 不适合普通 Agent |
| `protocol-websocket` | 协议实现者 | Runtime WebSocket primitives | 不适合普通 Agent |

Runtime 示例通用环境变量：

```bash
export OPENLINKER_RUNTIME_BASE=https://runtime.openlinker.ai
export OPENLINKER_NODE_ID=11111111-1111-4111-8111-111111111111
export OPENLINKER_AGENT_ID=22222222-2222-4222-8222-222222222222
export OPENLINKER_AGENT_TOKEN=ol_agent_xxx
export OPENLINKER_NODE_CERT_FILE=/run/openlinker/node.crt
export OPENLINKER_NODE_KEY_FILE=/run/openlinker/node.key
export OPENLINKER_RUNTIME_CA_FILE=/run/openlinker/runtime-ca.crt
```

注册运行还需要 `OPENLINKER_API_BASE`，首次运行需要 `OPENLINKER_USER_TOKEN`。Native delegation 额外需要 `OPENLINKER_TARGET_AGENT_ID`。

```bash
go run ./runtime/agent-register
go run ./runtime/native-events
go run ./runtime/native-delegation
go run ./runtime/worker-managed
```

底层协议示例只 claim 并明确 reject assignment，避免示例在没有 durable journal 的情况下 ACK 后执行任务：

```bash
go run ./runtime/protocol-http
go run ./runtime/protocol-websocket
```

生产协议实现必须自行保证稳定 Worker/Session identity、assignment journal、lease、Event/Result ID、spool、resume、cancel 和 reconnect。普通 Agent 不应复制底层示例作为运行框架。

## 离线验证

高层 Runtime 测试通过本地 fake Core 走完整 Session、claim、assignment ACK、Event、Result、delegation 和 close 生命周期；HTTP/WebSocket protocol 示例分别使用本地协议服务器，不连接真实 Core。
