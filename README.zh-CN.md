# openlinker-go

`openlinker-go` 是 OpenLinker Core 的 Go SDK。Go 服务使用 `NewClient` 查找和调用 Agent、
监听运行事件、验证 Webhook，并调用 A2A JSON-RPC、HTTP+JSON/SSE 和 gRPC；Agent
Node 通过 `NewRuntime` 使用严格的 Runtime v2 协议原语。两者都适用于自托管 Core
和基于其公开 API 构建的服务。

English documentation: [README.md](./README.md)

## 状态

本 SDK 目前是 pre-1.0。它跟随 Core API 和 runtime 契约演进。升级前请固定版本或
commit，并阅读 [CHANGELOG.md](./CHANGELOG.md)。

本 SDK 不包含钱包、扣费、Stripe、提现、商业 Dashboard 或具体本地 adapter 实现。
默认 client 使用 `OPENLINKER_USER_TOKEN`，runtime 使用 `OPENLINKER_AGENT_TOKEN`。
## 开源架构图

Go SDK 把调用方凭证和 Agent runtime 凭证分开。`NewClient` 封装 user-token 平台调用；
`NewRuntime` 封装 agent-token runtime 调用。进程级本地 adapter 属于
`openlinker-agent-node`。

```mermaid
flowchart LR
  Service["Go service / CLI / backend"] --> ClientSDK["openlinker-go Client"]
  ClientSDK -->|"REST client with OPENLINKER_USER_TOKEN"| Core["openlinker-core<br/>registry / runs / events"]
  ClientSDK -->|"A2A JSON-RPC / HTTP+JSON / gRPC"| Core
  AgentNode["openlinker-agent-node"] --> RuntimeSDK["openlinker-go Runtime v2"]
  RuntimeSDK -->|"mTLS + Agent Token / v2 WebSocket 或 v2 HTTP Pull"| Core

  HostedBridge["Hosted Bridge<br/>可选部署适配层"] -.->|"同一 Core API contract"| Core

  Core -->|"direct_http"| HTTPAgent["公网 HTTPS Agent"]
  Core -->|"mcp_server"| MCPAgent["远程 MCP / JSON-RPC server"]
  Core -->|"Runtime v2 分配与取消"| AgentNode
```

## 安装

```bash
go get github.com/OpenLinker-ai/openlinker-go
```

父 OpenLinker workspace 内本地开发时，可以直接使用此目录。

## 快速开始

```go
package main

import (
	"context"
	"fmt"
	"log"

	openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func main() {
	client, err := openlinker.NewClient(
		"https://core.example.com",
		openlinker.WithUserToken("ol_user_xxx"),
	)
	if err != nil {
		log.Fatal(err)
	}

	agents, err := client.ListAgents(context.Background(), openlinker.ListAgentsParams{
		Query:        "data",
		CallableOnly: true,
	})
	if err != nil {
		log.Fatal(err)
	}

	fmt.Println(agents.Total)
}
```

`NewClient` 会拒绝 agent token；Agent runtime 场景请使用 `NewRuntime`。

## 运行 Agent

启动 run 并读取结果：

```go
runIntentID := "replace-with-an-application-generated-intent-id"
result, err := client.RunAgent(context.Background(), openlinker.RunAgentRequest{
	AgentID:        agents.Items[0].ID,
	Input:          openlinker.JSON{"query": "Summarize this dataset"},
	IdempotencyKey: runIntentID, // 同一次运行意图重试时复用。
})
```

`RunAgent` 和 `StartAgentRun` 始终发送 `Idempotency-Key`。字段为空时，SDK
会为本次方法调用生成密码学随机 key；如果重试可能跨方法调用或进程，请显式设置
`IdempotencyKey`，并且只在同一运行意图中复用。`result.Replayed` 表示 Core
返回的是已经存在的 Run。

监听 run 事件：

```go
err = client.StreamRunEvents(context.Background(), result.RunID, openlinker.StreamRunEventsOptions{}, func(event openlinker.StreamRunEvent) error {
	fmt.Println(event.Event, string(event.Data))
	return nil
})
```

读取已保留的事件历史时，`ListRunEvents` 返回 `Items` 和 `Meta`。元数据会明确给出
请求游标、实际游标、保留缺口、可为空的可用序号边界、终态，以及本页是否已经覆盖
完整事件流。

## Callback

平台托管 callback 复用 Core run event stream，不需要公网 callback URL。外部 webhook
callback 适合服务端集成。处理 webhook 时必须先校验原始请求体签名，再解析 payload。

## Runtime v2

`NewRuntime` 暴露两条严格的 Runtime v2 传输：正常网络默认使用低延迟 WebSocket；
无法稳定维持 WebSocket 的受限网络使用 HTTP long-poll。Runtime 流量必须访问 Core
独立的 mTLS 地址，同时提供已登记的 Node 设备证书和绑定当前 Agent 的 Agent Token。
两条通道共享同一套 Session、Lease、Fencing、Resume、Event ACK、Result ACK 和取消语义，
不存在 v1 fallback。

`DialRuntimeV2WebSocket` 会先鉴权升级，再发送 `runtime.hello`，并且只在收到关联正确的
`runtime.ready` 后返回。连接内部只有一个 writer，严格限制完整消息为 4 MiB；Assignment、
Cancel、Drain 和 Revoke 都以类型化推送交付，业务 ACK 按 `reply_to_message_id` 等待，
多 Attempt Resume 会收齐每一条决定。它与 HTTP client 实现同一组 Runtime v2 方法，
持久化 worker 可以在不改变执行逻辑的前提下切换 transport。

真实 worker 统一使用 `openlinker-agent-node`：它负责持久身份、分配 journal、加密的
Event/Result spool、续租、resume、取消和优雅 drain。自研 Node 可以使用
`DialRuntimeV2WebSocket` 的类型化推送和关联 ACK，也可以使用 HTTP Session、long-poll
claim/command、显式 ACK/reject、renew、Event、Result、resume 与委派调用原语；稳定 ID、
持久化和恢复仍由 Node 自己负责。

通过 `WithHTTPClient` 传入基于 `*http.Transport`、已经配置 Node 客户端证书和 Runtime
服务端 CA 的 `http.Client`；SDK 会把同一份 mTLS 配置用于 `wss://`。本 SDK 不提供
Native runner，也不把 command、Codex、OpenClaw 或本地 HTTP adapter 塞进通用包。

## A2A Transport

SDK 支持 OpenLinker 托管的 A2A JSON-RPC、HTTP+JSON/SSE 和 gRPC。普通 HTTP 兼容场景
优先使用 JSON-RPC 或 HTTP+JSON；当 Agent Card 声明 `GRPC` 接口且调用方可以访问 HTTP/2
gRPC endpoint 时使用 gRPC。

gRPC 是 A2A transport binding，不替代 Agent Node 的 Runtime v2 传输。

## 开发

```bash
gofmt -w .
go test ./...
```

## 安全

不要把 user token、agent token、callback secret 或 push credential 写入日志或公开 Issue。
`OPENLINKER_USER_TOKEN` 用于 `NewClient`，`OPENLINKER_AGENT_TOKEN` 用于 `NewRuntime`；
Node 客户端私钥与加密 spool key 必须分开保护。信任 webhook payload 前必须校验签名。
漏洞请通过 [SECURITY.zh-CN.md](./SECURITY.zh-CN.md) 报告。

## 贡献

提交 PR 前请阅读 [CONTRIBUTING.zh-CN.md](./CONTRIBUTING.zh-CN.md)。SDK 只封装开源 Core
协议，不加入 Cloud 钱包、商业计费或托管市场内部接口。公共 API 变化要同步测试和契约文件。

## 支持和发布

- 支持说明：[SUPPORT.zh-CN.md](./SUPPORT.zh-CN.md)
- 发布清单：[RELEASE.zh-CN.md](./RELEASE.zh-CN.md)
- 英文变更记录：[CHANGELOG.md](./CHANGELOG.md)
- 行为准则：[CODE_OF_CONDUCT.md](./CODE_OF_CONDUCT.md)

## 许可证

Apache-2.0。详见 [LICENSE](./LICENSE)。
