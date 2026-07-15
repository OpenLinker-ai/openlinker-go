# Native Runtime 与显式注册

本文说明复杂 Agent 框架如何使用 `NativeRun`，以及极简和 Native 两种入口如何显式启用 Agent 注册。

## Native handler

`Native` 支持直接绑定返回任意具体结果类型的 handler，因此框架方法可以直接传入，不需要为了转换成 `any` 再包一层匿名函数：

```go
agent := openlinkerruntime.New(cfg)

err := openlinker.Native(agent.Handle).
    WithNodeID(nodeID).
    WithAgentID(agentID).
    WithDataDir(dataDir).
    WithTransportMode(openlinker.TransportAuto).
    Run(ctx)
```

例如以下方法可以直接用于 `Native(agent.Handle)`：

```go
func (a *Agent) Handle(
    ctx context.Context,
    run openlinker.NativeRun,
) (openlinker.NativeResult, error)
```

Runtime session、transport、lease、cancel、resume、Event/Result spool 和 drain 都由 SDK 管理。layout 只需要保留 Harness、trace、approval、artifact 和业务逻辑。

## Assignment 上下文

`NativeRun.Assignment` 保留 Runtime assignment 的业务可用字段：

- 完整 `AttemptIdentity`
- `AttemptDeadlineAt` 和 `RunDeadlineAt`
- `Input`
- `Metadata`

NodeEnvelope 和 Invocation Token 只保留在加密 `RuntimeStore` 与 Worker 内部，不通过 `NativeRun` 暴露。

常用读取入口：

```go
identity := run.Identity()
runID := run.RunID()
attemptID := run.AttemptID()
agentID := run.AgentID()
metadata := run.Metadata()
deadline, ok := run.Deadline()
```

传给 handler 的 `ctx` 已经绑定 Core deadline，并会在 cancel、lease revoke、drain deadline 或宿主退出时取消。Harness 应持续向下传递这个 context，而不是创建不受控的 `context.Background()`。

## Event helpers

```go
_ = run.MessageDelta(ctx, "正在处理")
_ = run.Progress(ctx, 50, "完成一半")
_ = run.Emit(ctx, "layout.trace.completed", struct {
    Steps int `json:"steps"`
}{Steps: 5})
```

helper 对应的事件类型：

| helper | event type |
|---|---|
| `Message` / `MessageDelta` | `run.message.delta` |
| `Progress` | `run.progress.changed` |
| `Emit` | 调用方提供的非 Core 保留事件类型 |

`Emit` 和兼容入口 `SendEvent` 都会在进入 durable spool 前完成以下检查：

- event type 必须使用小写点分段格式。
- 不能使用 `run.completed` 等 Core 保留类型。
- payload 必须能完整编码为 JSON object。
- map 内部包含 channel、function 等不可编码值时立即返回错误。

## Result helpers

```go
return openlinker.Success(output), nil
return openlinker.Failure("LAYOUT_FAILED", err), nil
return openlinker.RetryableFailure("UPSTREAM_BUSY", err), nil
```

SDK 会统一处理：

- `nil` 成功输出转换为空 JSON object。
- handler 返回的普通 error 转换为 `AGENT_RUNTIME_ERROR`。
- panic 转换为 `AGENT_RUNTIME_PANIC`。
- 非法 `NativeResult.Status` 转换为 `AGENT_RUNTIME_INVALID_RESULT`，不会直接破坏整个 worker 状态机。
- `RetryableFailure` 设置 Core 可识别的 retryable hint。

## Agent-to-Agent delegation

简单调用：

```go
child, err := run.CallAgent(ctx, targetAgentID, input, "需要子 Agent 处理")
```

框架需要 Metadata 或稳定幂等键时使用：

```go
child, err := run.CallAgentWithRequest(ctx, openlinker.RuntimeCallAgentRequest{
    TargetAgentID: targetAgentID,
    Input:         input,
    Metadata:      map[string]any{"trace_id": traceID},
    Reason:        "layout tool delegation",
}, actionID)
```

同一个逻辑调用重试时必须复用相同的 idempotency key。SDK 使用当前 assignment 的 NodeEnvelope 和 Invocation Token 生成委派证明，不使用长期 Agent Token 代替 assignment authority。

## 四种高层入口

不注册，直接运行：

```go
openlinker.WithAgent(agent).Run(ctx)
openlinker.Native(agent.Handle).Run(ctx)
```

显式注册并运行：

```go
openlinker.WithAgent(agent).RunOrRegister(ctx, spec, options...)
openlinker.Native(agent.Handle).RunOrRegister(ctx, spec, options...)
```

也可以先配置再运行：

```go
openlinker.Native(agent.Handle).
    WithRegistration(spec, options...).
    Run(ctx)
```

只有 `RunOrRegister` 或 `WithRegistration` 会创建、轮换或验证平台资源。普通 `Run` 即使检测到 `OPENLINKER_USER_TOKEN` 也不会静默注册。

## 注册状态与 policy

新代码使用精简的 `AgentSpec`：

```go
spec := openlinker.AgentSpec{
    Slug:        "layout-agent",
    Name:        "Layout Agent",
    Description: "Blades based Agent",
    Visibility:  "private",
}
```

注册控制通过 options 传入：

```go
openlinker.WithRegistrationPolicy(openlinker.RegisterPolicyReuseExisting)
openlinker.WithRegistrationStore(store)
openlinker.WithRegistrationUserToken(userToken)
openlinker.WithRegistrationAgentToken(agentToken)
openlinker.WithRegistrationAPIBase(apiBase)
openlinker.WithRegistrationToken(name, scopes, expiresInMinutes)
```

policy 语义：

| policy | 行为 |
|---|---|
| `reuse_existing` | 优先读取本地注册状态，不需要 User Token，也不会重复兑换 pending token。 |
| `rotate_token` | 复用 Agent，使用 User Token 创建新的 Agent Token。 |
| `force_new` | 使用 User Token 显式创建新的 Agent 和 Token。 |
| `validate_only` | 使用 User Token 验证已保存 Agent 与 Token，绝不创建资源。 |

首次成功后会保存 AgentID、Agent Token、Token ID/prefix 和 API Base。后续启动优先复用保存结果；只有创建、轮换或验证确实需要时才要求 User Token。

`EnsureAgentRequest` 仍保留一个兼容周期，但已经标记 deprecated；新代码应使用 `AgentSpec` 和 registration options。

## 本地状态安全

默认 `EnvRegistrationStore` 写入 `.env`，并且：

- 使用临时文件、fsync 和原子 rename。
- 强制最终文件权限为 `0600`，即使旧文件原来是 `0644`。
- 保留不属于 SDK 管理范围的其他环境变量。
- `AgentRegistration.String()` 不包含 Agent Token。

生产环境建议注入由 Secret Manager 或加密存储支持的 `RegistrationStore`。
