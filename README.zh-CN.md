# openlinker-go

`openlinker-go` 是 OpenLinker 的 Go SDK。OpenLinker 是 AI Agent 注册中心、Agent
市场、A2A/MCP runtime 网关和自托管 Agent 平台。你可以在 Go 服务中使用本 SDK 查询
Agent、运行 Agent、监听事件、验证 webhook、构建 Agent runtime connector，并调用
A2A JSON-RPC、HTTP+JSON/SSE 和 gRPC transport。

English documentation: [README.md](./README.md)

## 状态

本 SDK 目前是 pre-1.0。它跟随 Core API 和 runtime 契约演进。升级前请固定版本或
commit，并阅读 [CHANGELOG.md](./CHANGELOG.md)。

本 SDK 不包含钱包、扣费、Stripe、提现、商业 Dashboard 或具体本地 adapter 实现。

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

服务端通常用 user token 调用公开 Core API；Agent runtime 场景使用 agent token。

## 运行 Agent

启动 run 并读取结果：

```go
result, err := client.RunAgent(context.Background(), openlinker.RunAgentRequest{
	AgentID: agents.Items[0].ID,
	Input:   openlinker.JSON{"query": "Summarize this dataset"},
})
```

监听 run 事件：

```go
err = client.StreamRunEvents(context.Background(), result.RunID, func(event openlinker.StreamRunEvent) error {
	fmt.Println(event.Event, string(event.Data))
	return nil
})
```

## Callback

平台托管 callback 复用 Core run event stream，不需要公网 callback URL。外部 webhook
callback 适合服务端集成。处理 webhook 时必须先校验原始请求体签名，再解析 payload。

## Runtime Connector

SDK 包含基础 Agent runtime 集成层：

- `HeartbeatAgent`
- `ClaimRuntimeRun`
- `ClaimRuntimeRunDetailed`
- `CompleteRuntimeRun`
- `CallAgent`
- `CallAgentAt`
- `RuntimePullConnector`
- `RuntimeWSConnector`

本包不包含 command、Codex、OpenClaw 或本地 HTTP 后端 adapter。进程级集成请使用
`openlinker-agent-node`。

## A2A Transport

SDK 支持 OpenLinker 托管的 A2A JSON-RPC、HTTP+JSON/SSE 和 gRPC。普通 HTTP 兼容场景
优先使用 JSON-RPC 或 HTTP+JSON；当 Agent Card 声明 `GRPC` 接口且调用方可以访问 HTTP/2
gRPC endpoint 时使用 gRPC。

gRPC 是 A2A transport binding，不替代 Agent Node 内部 `runtime_ws` / `runtime_pull`。

## 开发

```bash
gofmt -w .
go test ./...
```

## 安全

不要把 user token、agent token、callback secret 或 push credential 写入日志或公开 Issue。
信任 webhook payload 前必须校验签名。漏洞请通过 [SECURITY.zh-CN.md](./SECURITY.zh-CN.md)
报告。

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
