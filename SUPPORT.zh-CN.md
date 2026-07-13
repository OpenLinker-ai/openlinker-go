# 支持

English documentation: [SUPPORT.md](./SUPPORT.md)

可用 GitHub Issues 报告可复现 bug、文档问题，以及符合 `openlinker-go` 开源范围的功能请求。

## 适合提交 Issue 的内容

- 支持的 Core endpoint 类型化 client 行为
- 严格的 Runtime 协议与 HTTP 行为
- callback 签名或校验行为
- A2A JSON-RPC、HTTP+JSON、SSE 或 gRPC client 行为
- 本 SDK 与 `openlinker-core` 的契约不一致
- 示例或公共 API 使用文档缺口

## 提交前请确认

- 搜索已有 Issue 和近期 commit。
- 在最新 `main` 或指定 release 上确认问题。
- 提供 Go 版本、操作系统、SDK 版本或 commit SHA，以及 Core API 版本或 commit SHA。
- 尽量提供最小 Go 复现。
- 提供期望行为、实际行为和脱敏日志。
- 删除 user token、agent token、callback secret、私有 URL、客户数据和本地 `.env`。

## 不在这里处理

- 安全漏洞；请看 [SECURITY.zh-CN.md](./SECURITY.zh-CN.md)
- 商业 Cloud 钱包、计费、提现或 Dashboard API
- 进程级 adapter；请使用 `openlinker-agent-node`
- 无法公开复现的私有部署调试

## 跨仓库问题

涉及 Core 和本 SDK 的问题请包含：

- SDK 版本或 commit SHA
- Core API commit SHA 或版本
- endpoint 或 SDK 方法名
- 可用时提供脱敏请求/响应状态和错误体
