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

### User Token 自注册必须走两步兑换

真实环境验证发现，Core 的 Agent 管理路由与自注册路由采用不同认证边界：

- `/api/v1/creator/agents*` 面向浏览器创作者后台，只接受网页登录 JWT。
- `/api/v1/creator/agent-tokens` 使用 hybrid 鉴权，接受带授权 grant 的 User Token。
- `/api/v1/agent-registration/agents` 使用待注册 Agent Token，不要求网页登录 JWT。

SDK 原来的首次 `EnsureAgent` 会使用 User Token 查询或创建 `/creator/agents`，合法的 `ol_user_*` 因此被 JWT 中间件拒绝。修复后首次自注册严格使用 Core 已定义的两步流程：

1. 使用 User Token 创建 `pending_registration` Agent Token。
2. 使用该 Agent Token 调用 `/agent-registration/agents`，原子创建 Agent，并把同一枚 Token 兑换为 `active_runtime`。

后续启动直接复用本地保存的 AgentID 和 Agent Token，不再需要 User Token。`rotate_token` 对已有 Agent 只调用 `/creator/agent-tokens`，`validate_only` 通过 Agent Token 列表验证，不再依赖 JWT-only 的 Agent 管理路由。

### Runtime 集群开放前置条件

Runtime mTLS listener 可用不代表集群已经允许创建 Session。真实环境中 Core 曾保持：

```text
mode=hard_maintenance
release_version=local
release_commit=unknown
```

该状态下 `/readyz` 和 Runtime Session create 均返回 503。部署必须设置真实的 `OPENLINKER_RELEASE_ID` 与 `OPENLINKER_GIT_SHA`，再使用 Core 的 `runtime-cutover reopen` CLI 按 control version 和 cutover ID 执行 CAS reopen。恢复到 `mode=normal` 且 `/readyz=200` 后，SDK supervisor 能从连续 503 自动恢复并建立原 Session 生命周期，无需重启 Agent 进程。

## 真实环境 smoke 结果

2026-07-16 在 Core `v0.3.3`、commit `987adc540d2366002e6597e2a6a806368b6e81d2` 上完成：

- User Token 两步自注册成功，registration 文件权限为 `0600`。
- Runtime Node ECDSA P-256 mTLS 认证成功。
- 显式 HTTP transport：Session、heartbeat、claim、assignment ACK、Event、Result 全部成功。
- `TransportAuto`：复用本地 registration，在未提供 User Token 的情况下选择 WebSocket 并成功运行。
- 两笔真实 layout Run 均为 `success`，分别返回 `layout smoke test success` 与 `layout websocket smoke success`。
- HTTP Run 形成连续 sequence 1–7 的事件、用户/Agent 消息和结果 Artifact。
- Runtime Store 的 lock、identity、加密 spool key、snapshot 和 WAL 均以 `0600` 保存。

## Core 工作树说明

本地 Core 工作树已有用户修改，并存在大量未跟踪 AppleDouble `._*` 文件；`.git/objects/pack/._*.idx` 还会导致 Git 输出 `non-monotonic index`。本次未修改或清理 Core 源码；仅在开发 K8s 环境补充发布身份配置并执行 Runtime cluster reopen。

Core 的 `go test ./pkg/runtime ./pkg/agent` 中，`pkg/agent` 通过；`pkg/runtime` 仅有 `TestRuntimePublicBoundaryUsesCanonicalNamesAndPaths` 因扫描到 `cmd/api/._bootstrap_admin.go` 的 NUL 字符失败。绕开该文件扫描的 Runtime contract/HTTP/WebSocket 定向测试已通过。
