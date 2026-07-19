# OpenLinker 极简 Agent

[English](README.md)

这个示例只演示最简单、也是普通 Agent 推荐使用的 SDK 入口：

```go
openlinker.WithAgent(agent).Run(ctx)
```

Agent 只需要实现：

```go
Run(context.Context, string) (string, error)
```

环境配置、mTLS、Runtime session、assignment、lease、spool、resume、重连以及 Result 映射都由 SDK 管理。

## 运行

```bash
OPENLINKER_API_BASE=https://api.openlinker.ai \
OPENLINKER_NODE_ID=11111111-1111-4111-8111-111111111111 \
OPENLINKER_AGENT_ID=22222222-2222-4222-8222-222222222222 \
OPENLINKER_AGENT_TOKEN=ol_agent_xxx \
OPENLINKER_NODE_CERT_FILE=/run/openlinker/node.crt \
OPENLINKER_NODE_KEY_FILE=/run/openlinker/node.key \
OPENLINKER_RUNTIME_CA_FILE=/run/openlinker/runtime-ca.crt \
OPENLINKER_RUNTIME_TRANSPORT=auto \
go run ./runtime/agent-generic
```

以上命令需要在共享 module 根目录 `example/` 中执行。需要强制 WebSocket 时：

```bash
OPENLINKER_RUNTIME_TRANSPORT=ws go run ./runtime/agent-generic
```

## 可选 Agent 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `GENERIC_AGENT_NAME` | `Generic Agent` | 文本响应中使用的 Agent 名称。 |
| `GENERIC_AGENT_PREFIX` | 空 | 设置后返回 `<prefix> <input>`。 |
| `GENERIC_AGENT_PANIC` | `false` | 设置为 `1` 或 `true`，用于测试 SDK 的 panic recovery。 |

## OpenLinker Runtime 配置

| 环境变量 | 默认值 | 说明 |
| --- | --- | --- |
| `OPENLINKER_NODE_ID` | 必填 | 已注册的 Node UUID。 |
| `OPENLINKER_AGENT_ID` | 必填 | Agent UUID。 |
| `OPENLINKER_AGENT_TOKEN` | 必填 | Agent Runtime credential。 |
| `OPENLINKER_API_BASE` | 未显式配置 Runtime base 时必填 | 用于发现 Runtime endpoint 的平台地址。 |
| `OPENLINKER_RUNTIME_BASE` | 空 | 显式覆盖 Runtime endpoint。 |
| `OPENLINKER_RUNTIME_DATA_DIR` | `.openlinker/runtime-<agent-id>` | 加密 identity、journal 和 spool 目录。 |
| `OPENLINKER_RUNTIME_TRANSPORT` | `auto` | 支持 `auto`、`ws` 或 `pull`。 |
| `OPENLINKER_NODE_CERT_FILE` | 必填 | Node mTLS 证书。 |
| `OPENLINKER_NODE_KEY_FILE` | 必填 | Node mTLS 私钥。 |
| `OPENLINKER_RUNTIME_CA_FILE` | 必填 | Runtime 服务端 CA。 |

## 输入

SDK 会从 `text`、`query`、`task`、`prompt` 中读取第一个非空字符串，也接受纯字符串输入。

```json
{
  "text": "summarize this"
}
```

## Panic recovery 测试

使用以下方式验证 SDK 会把 Agent panic 映射为失败 Result，而不是让 Worker 进程崩溃：

```bash
GENERIC_AGENT_PANIC=1 \
go run ./runtime/agent-generic
```

从 OpenLinker 触发一次 Run 后，Worker 仍可继续处理后续任务，失败结果中应包含：

```json
{
  "status": "failed",
  "error": {
    "code": "AGENT_RUNTIME_PANIC",
    "message": "agent panic: generic agent panic requested by GENERIC_AGENT_PANIC"
  }
}
```
