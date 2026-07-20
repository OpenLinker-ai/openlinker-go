# Client 示例

[English](README.md)

本分类包含六个单一概念示例。所有命令都在共享 module 根目录 `example/` 中执行。

## 示例索引

| 目录 | 核心概念 | 是否创建 Run | 命令 |
|---|---|---:|---|
| `agent-discovery` | `ListAgents`、`GetAgent`、`GetAgentCard` | 否 | `go run ./client/agent-discovery` |
| `run-sync` | `RunAgent` 与显式 idempotency key | 是 | `go run ./client/run-sync` |
| `run-async` | `StartAgentRun`、`GetRun`、轮询与 timeout | 是 | `go run ./client/run-async` |
| `run-stream` | `StreamRunEvents`、SSE 终态与断流 | 是 | `go run ./client/run-stream` |
| `run-callbacks` | `RunAgentWithCallbacks`、message delta 与 terminal callback | 是 | `go run ./client/run-callbacks` |
| `run-history` | Events、Messages、Artifacts、Children 与 retention metadata | 否 | `go run ./client/run-history` |

## 通用环境变量

```bash
export OPENLINKER_API_BASE=https://api.openlinker.ai
export OPENLINKER_USER_TOKEN=ol_user_xxx
```

发现 Agent 需要：

```bash
export OPENLINKER_AGENT_SLUG=my-agent
export OPENLINKER_AGENT_QUERY=my-agent # 可选
go run ./client/agent-discovery
```

创建 Run 的四个示例需要：

```bash
export OPENLINKER_AGENT_ID=22222222-2222-4222-8222-222222222222
export OPENLINKER_RUN_INPUT='hello' # 可选，默认 hello
export OPENLINKER_IDEMPOTENCY_KEY='demo-intent-20260715-001'
```

`OPENLINKER_IDEMPOTENCY_KEY` 表示一次明确的创建意图。同一次请求因网络错误重试时应复用原值；真正发起新任务时必须换一个新值。

异步、流式和 callback 示例还支持：

```bash
export OPENLINKER_RUN_TIMEOUT=30s # 默认 30s
export OPENLINKER_POLL_INTERVAL=1s # 仅 run-async 使用
```

读取已有 Run 历史需要：

```bash
export OPENLINKER_RUN_ID=33333333-3333-4333-8333-333333333333
export OPENLINKER_AFTER_SEQUENCE=0 # 可选
go run ./client/run-history
```

## 离线测试覆盖

每个目录都有 `httptest`：验证实际请求路径、Bearer Token、idempotency header、JSON body、SSE、终态处理以及 Event retention metadata，不需要连接真实 Core。
