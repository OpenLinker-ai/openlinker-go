# openlinker-go

`openlinker-go` 是 OpenLinker Core 的 Go SDK。Go 服务使用 `NewClient` 查找和调用 Agent、
监听运行事件、验证 Webhook，并调用 A2A；Agent 进程使用 `NewRuntimeWorker` 直接运行
可靠 handler。需要协议原语时仍可使用底层 `NewRuntime`。

English documentation: [README.md](./README.md)

## 状态

本 SDK 目前是 pre-1.0。它跟随 Core API 和 runtime 契约演进。升级前请固定版本或
commit，并阅读 [CHANGELOG.md](./CHANGELOG.md)。

本 SDK 不包含钱包、扣费、Stripe、提现、商业 Dashboard 或具体本地 adapter 实现。
默认 client 使用 `OPENLINKER_USER_TOKEN`，runtime 使用 `OPENLINKER_AGENT_TOKEN`。
## 开源架构图

Go SDK 把调用方凭证和 Runtime 凭证分开。`NewClient` 封装 User Token 平台调用；
`NewRuntimeWorker` 发现专用 mTLS Runtime 地址，并负责交付、恢复和持久化。HTTP、command、
Codex、A2A 等进程级 adapter 属于 `openlinker-agent-node`。

```mermaid
flowchart LR
  Service["Go service / CLI / backend"] --> ClientSDK["openlinker-go Client"]
  ClientSDK -->|"REST client with OPENLINKER_USER_TOKEN"| Core["openlinker-core<br/>registry / runs / events"]
  ClientSDK -->|"A2A JSON-RPC / HTTP+JSON / gRPC"| Core
  Handler["Go RuntimeHandler"] --> RuntimeSDK["openlinker-go RuntimeWorker"]
  AgentNode["openlinker-agent-node<br/>可选 Adapter 壳"] --> RuntimeSDK
  RuntimeSDK -->|"mTLS + Agent Token / WebSocket 或 HTTP 长轮询"| Core

  HostedBridge["Hosted Bridge<br/>可选部署适配层"] -.->|"同一 Core API contract"| Core

  Core -->|"direct_http"| HTTPAgent["公网 HTTPS Agent"]
  Core -->|"mcp_server"| MCPAgent["远程 MCP / JSON-RPC server"]
  Core -->|"Runtime 分配与取消"| RuntimeSDK
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

`NewClient` 会拒绝 Agent Token；Agent Runtime 场景请使用 `NewRuntimeWorker`。

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

## OpenLinker Runtime

`RuntimeWorker` 负责地址发现、mTLS、Session、WebSocket/Pull 自动切换、assignment 确认、
续租、resume、取消、drain，以及 assignment/Event/Result 的加密持久化。只有 Core 明确
确认 assignment 后才会调用 handler。

```go
worker, err := openlinker.NewRuntimeWorker(openlinker.RuntimeWorkerConfig{
	PlatformURL: "https://openlinker.example",
	NodeID:      os.Getenv("OPENLINKER_NODE_ID"),
	AgentID:     os.Getenv("OPENLINKER_AGENT_ID"),
	AgentToken:  os.Getenv("OPENLINKER_AGENT_TOKEN"),
	Transport:   openlinker.RuntimeTransportAuto,
	DataDir:     "/var/lib/my-agent/runtime",
	MTLS: openlinker.RuntimeMTLSConfig{
		CertFile: "/run/openlinker/node.crt",
		KeyFile:  "/run/openlinker/node.key",
		CAFile:   "/run/openlinker/core-ca.crt",
	},
	Handler: openlinker.RuntimeHandlerFunc(func(ctx context.Context, run openlinker.RuntimeContext) (openlinker.RuntimeResult, error) {
		if err := run.Emit("run.message.delta", map[string]any{"text": "working"}); err != nil {
			return openlinker.RuntimeResult{}, err
		}
		return openlinker.RuntimeResult{Output: map[string]any{"answer": 42}}, nil
	}),
})
if err != nil {
	log.Fatal(err)
}
if err := worker.Start(context.Background()); err != nil {
	log.Fatal(err)
}
```

生产环境应通过 `DataDir` 使用默认 `FileRuntimeStore`，也可以注入其他持久化
`RuntimeStore`。内存实现只适合明确的测试。`NodeVersion` 默认是
`openlinker-go/runtime-worker`；宿主程序有独立登记版本时应显式设置。

WebSocket 的标准端点是 `/api/v1/agent-runtime/ws`，HTTP 方法统一使用
`/api/v1/agent-runtime/` 前缀。协议协商保留在握手 contract 内，不进入公开 API 名或 URL。

`NewRuntime` 继续提供 HTTP/WebSocket 协议原语。`openlinker-agent-node` 只是可选 Adapter
壳，把 HTTP、command、Codex 或 A2A handler 注入本 SDK，不再维护第二套 Runtime 状态机。

## A2A Transport

SDK 支持 OpenLinker 托管的 A2A JSON-RPC、HTTP+JSON/SSE 和 gRPC。普通 HTTP 兼容场景
优先使用 JSON-RPC 或 HTTP+JSON；当 Agent Card 声明 `GRPC` 接口且调用方可以访问 HTTP/2
gRPC endpoint 时使用 gRPC。

gRPC 是 A2A transport binding，不替代 OpenLinker Runtime。

## 开发

```bash
gofmt -w .
go test ./...
```

## 安全

不要把 user token、agent token、callback secret 或 push credential 写入日志或公开 Issue。
`OPENLINKER_USER_TOKEN` 用于 `NewClient`，`OPENLINKER_AGENT_TOKEN` 用于 `NewRuntimeWorker`；
Runtime 客户端私钥与加密 spool key 必须分开保护。信任 webhook payload 前必须校验签名。
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
