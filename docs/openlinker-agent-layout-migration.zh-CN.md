# openlinker-agent-layout 迁移到 Native Runtime

本文说明 `openlinker-agent-layout` 从旧 Runtime connector 接口迁移到 `openlinker-go v0.2.0-rc.1` 的方式。

## 迁移目标

迁移后职责边界如下：

- layout 保留 Harness、模型调用、skills、trace、approval、artifact、memory 和业务逻辑。
- SDK 管理 Runtime discovery、mTLS、Session identity、assignment journal、lease、cancel、deadline、resume、重连、transport fallback/promotion、Event/Result spool 和优雅关闭。
- layout 不再调用 `EnsureRuntimeAgent`、`RuntimePullConnector`、`RuntimeWSConnector`、`CompleteRun` 或自行维护 Runtime 状态机。

## 入口迁移

旧入口使用注册 helper 和 connector builder。新入口直接绑定 handler：

```go
agent := openlinkerruntime.New(cfg)

runner := openlinker.Native(agent.Handle).
    WithAPIBase(cfg.OpenLinker.APIBase).
    WithRuntimeBase(cfg.OpenLinker.RuntimeBase).
    WithNodeID(cfg.OpenLinker.NodeID).
    WithAgentID(cfg.OpenLinker.AgentID).
    WithAgentToken(cfg.OpenLinker.AgentToken).
    WithDataDir(cfg.OpenLinker.DataDir).
    WithCapacity(cfg.OpenLinker.Capacity).
    WithTransportMode(openlinker.TransportAuto)

if cfg.OpenLinker.AutoRegister {
    runner.WithRegistration(spec, registrationOptions...)
}

err := runner.Run(ctx)
```

`Native(agent.Handle)` 可以直接接收：

```go
func (a *Agent) Handle(
    ctx context.Context,
    run openlinker.NativeRun,
) (openlinker.NativeResult, error)
```

## 配置映射

| 旧配置 | 新配置 | 说明 |
|---|---|---|
| `runtime_token` / `OPENLINKER_RUNTIME_TOKEN` | `agent_token` / `OPENLINKER_AGENT_TOKEN` | 旧值仅作为一个迁移周期的读取兼容。 |
| `connector: runtime_ws` | `transport: websocket` | API 模式不再由 connector 名称表达。 |
| `connector: runtime_pull` | `transport: http` | HTTP 指 Runtime v2 long-poll。 |
| 未配置 connector | `transport: auto` | 新默认值，优先 WebSocket。 |
| `pull_wait` | 删除 | claim、probe、backoff 由 SDK Worker 管理。 |
| 自定义 JSON state | `data_dir` | 默认 `.openlinker/runtime-layout`，保存加密 journal 与 spool。 |

Runtime 进程仍需已登记的 `OPENLINKER_NODE_ID`、Agent identity、Agent Token 和 mTLS 文件。自动注册只创建或复用 Agent 与 Agent Token，不替代 Node 登记和证书分发。

## NativeRun 适配

使用 `run.RunID()`、`run.AttemptID()`、`run.AgentID()`、`run.Identity()` 和 `run.Metadata()`，不再读取旧 Assignment 上的 conversation/A2A connector 字段。

layout 的 session key 优先从 metadata 中读取：

- `session_key`
- `protocol_context_id`
- `root_context_id`
- `parent_context_id`
- `trace_id`
- `conversation_id`

没有上下文 metadata 时回退到 `run.RunID()`。

## 事件和结果

调用 `run.Emit`、`run.MessageDelta` 或 `run.Progress` 后，事件已进入 SDK durable spool。最终 `NativeResult` 不应再次携带同一批事件，否则会重复提交。

```go
_ = run.MessageDelta(ctx, answer)
return openlinker.Success(map[string]any{"text": answer}), nil
```

失败使用：

```go
_ = run.Emit(ctx, "layout.error", payload)
return openlinker.Failure("LAYOUT_RUN_FAILED", err), nil
```

## 依赖与验证

RC 发布后，下游固定明确版本并删除本地替换和 vendor：

```bash
go mod edit -require=github.com/OpenLinker-ai/openlinker-go@v0.2.0-rc.1
go mod edit -dropreplace=github.com/OpenLinker-ai/openlinker-go
rm -rf vendor
go mod tidy
go test ./...
```

RC 发布前可以临时保留 `replace github.com/OpenLinker-ai/openlinker-go => ../openlinker-go` 做同工作区联调，但不得提交为正式发布依赖。

## 回滚

如果 RC 出现阻断问题：

1. 停止新版本 layout 进程，保留 `.openlinker/runtime-layout`，不要删除未 ACK spool。
2. 切回上一个已验证的 layout tag 和 SDK tag。
3. 使用旧版本独立的数据目录，避免旧 Store 读取新格式。
4. 不回退 Core Runtime v2 contract，也不恢复 Runtime v1 路由；如必须恢复旧 connector，只能回滚整个下游二进制。
5. 保存 RC 日志、Core run/attempt ID 和 Store 目录副本用于排查，但不得上传 Token、NodeEnvelope 或私钥。
