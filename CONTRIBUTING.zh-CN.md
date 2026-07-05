# 贡献 openlinker-go

English documentation: [CONTRIBUTING.md](./CONTRIBUTING.md)

感谢你改进 `openlinker-go`，这是 OpenLinker Core API、runtime connector、callback 和
A2A transport 的 Go SDK。

## 开发环境

```bash
go test ./...
```

测试和示例只能使用占位 token。不要提交真实 user token、agent token、callback secret、
私有 endpoint 或捕获的客户数据。

## 范围边界

可以放在这里：

- 开源 Core API 的类型化 wrapper
- runtime pull/WebSocket connector helper
- callback 创建和签名校验 helper
- A2A JSON-RPC、HTTP+JSON、SSE 和 gRPC 客户端行为
- 本 SDK 使用的契约测试和生成 protobuf artifact

不要放在这里：

- Cloud 钱包、计费、Stripe、提现和商业 Dashboard API
- 托管市场排序或私有推荐内部逻辑
- command、Codex、OpenClaw、本地后端 runner 等进程级 adapter；这些属于 `openlinker-agent-node`

## PR 要求

- 导出的 API 变化要小且有文档说明。
- client、callback、runtime connector 或 A2A transport 行为变化需要测试。
- 生成 protobuf 文件要与 `proto/` 保持一致。
- 公共行为变化要更新 `README.md` 和 `CHANGELOG.md`。
- 除非明确说明 pre-1.0 breaking behavior，否则尽量保持向后兼容。

## 检查

```bash
gofmt -w .
go test ./...
```

## 安全

不要公开提交漏洞 Issue。请按照 [SECURITY.zh-CN.md](./SECURITY.zh-CN.md) 处理。

## 许可证

贡献即表示你同意贡献内容使用本仓库的 Apache-2.0 许可证。
