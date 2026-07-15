# openlinker-core main 契约核对记录

**核对日期：** 2026-07-15  
**Core commit：** `987adc540d2366002e6597e2a6a806368b6e81d2`

## 结论

SDK 与本地 `../openlinker-core main` 的 Runtime v2 contract 文件逐字节一致，SHA-256 均为：

```text
3f84df167bbe211efdc6362ad5ec876aeedf881cbfb9677606982af63c7423e9
```

以下内容一致：

- protocol version：`2`
- contract ID：`openlinker.runtime.v2`
- required features：`lease_fence`、`assignment_confirm`、`renew`、`resume`、`event_ack`、`result_ack`、`cancel`、`persistent_spool`
- WebSocket：`/api/v1/agent-runtime/ws`
- HTTP：Session create/heartbeat/close、claim、assignment ack/reject、lease renew、event、result、resume、cancel ack、commands 和 call-agent 路由
- 单消息上限：4 MiB
- Node capacity 上限：1024

Core 的 contract、Runtime HTTP routes、WebSocket hello/ready 和 close-code 定向测试通过：

```bash
go test ./pkg/runtime -run 'TestRuntimeContract|TestRuntimeControllerRegistersLifecycleAndExecutionRoutes|TestRuntimeWebSocketHelloReadyAndDisconnectOrder|TestRuntimeWebSocketHandshakeCloseCodes' -count=1
```

`pkg/agent` 全量测试通过。

## 发现并修复的问题

最新 Core 已将 Agent 接入模式收敛为 transport-neutral 的：

```text
connection_mode=runtime
```

SDK 原注册默认仍是 `runtime_ws`。这会把 API 接入模式和网络传输混在一起，并被当前 Core 拒绝。修复后：

- 新注册默认发送 `connection_mode=runtime`。
- 旧注册输入 `runtime_ws`、`runtime_pull`、`agent_node` 在 SDK 兼容层归一化为 `runtime`。
- WebSocket/HTTP 继续只由 `TransportAuto`、`TransportWebSocket`、`TransportHTTP` 控制。
- registration example 与 `openlinker-agent-layout` 已使用 `connection_mode=runtime`。

## Core 工作树说明

本地 Core 工作树已有用户修改，并存在大量未跟踪 AppleDouble `._*` 文件；`.git/objects/pack/._*.idx` 还会导致 Git 输出 `non-monotonic index`。本次未修改或清理 Core。

Core 的 `go test ./pkg/runtime ./pkg/agent` 中，`pkg/agent` 通过；`pkg/runtime` 仅有 `TestRuntimePublicBoundaryUsesCanonicalNamesAndPaths` 因扫描到 `cmd/api/._bootstrap_admin.go` 的 NUL 字符失败。绕开该文件扫描的 Runtime contract/HTTP/WebSocket 定向测试已通过。
