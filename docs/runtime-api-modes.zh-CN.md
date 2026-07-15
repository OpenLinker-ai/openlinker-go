# OpenLinker Go Runtime API 模式

本文固定 `v0.2.0` 的公开 API 分层。API 模式描述 SDK 为应用管理多少 Runtime 生命周期；它与连接使用 WebSocket 还是 HTTP 无关。

## 1. 极简 Agent

普通 Go Agent 使用 `WithAgent` 或 `WithFunc`。SDK 负责配置、mTLS、加密持久化、Session、assignment、lease、cancel、resume、重连和优雅关闭。

```go
err := openlinker.WithAgent(agent).Run(ctx)
err := openlinker.WithFunc(fn).Run(ctx)
```

示例：[runtime/agent-generic](../example/runtime/agent-generic)。

## 2. 显式注册并运行

注册是附加在极简或 Native 模式上的显式能力。仅设置 `OPENLINKER_USER_TOKEN` 不会创建平台资源。

```go
err := openlinker.WithAgent(agent).RunOrRegister(ctx, spec, options...)

err := openlinker.Native(handler).
    WithRegistration(spec, options...).
    Run(ctx)
```

示例：[runtime/agent-register](../example/runtime/agent-register)。

## 3. Native Runtime

框架需要 Assignment metadata、attempt/run identity、自定义事件、message delta、进度、Agent 委派、cancel/deadline 或自定义成功/失败结果时使用 `Native`。SDK 仍然管理同一个 `RuntimeWorker` 生命周期。

```go
err := openlinker.Native(agent.Handle).
    WithTransportMode(openlinker.TransportAuto).
    Run(ctx)
```

常用入口包括 `NativeRun.Emit`、`MessageDelta`、`Progress`、`CallAgent`、`Success`、`Failure` 和 `RetryableFailure`。

示例：[runtime/native-events](../example/runtime/native-events) 与 [runtime/native-delegation](../example/runtime/native-delegation)。

## 4. Managed Worker 与底层协议

`NewRuntimeWorker(RuntimeWorkerConfig)` 是高级可嵌入生命周期引擎，适合需要自定义 Store、capacity、logger 或 daemon 外壳的实现。

`NewRuntime` 及 Runtime HTTP/WebSocket 方法是最低协议层。绕过 `RuntimeWorker` 后，调用方必须自行负责 journal、lease、Event/Result spool、resume、cancel、reconnect 和 transport switching。

示例：[runtime/worker-managed](../example/runtime/worker-managed)、[runtime/protocol-http](../example/runtime/protocol-http) 和 [runtime/protocol-websocket](../example/runtime/protocol-websocket)。

## 传输模式

- `TransportAuto`：默认。优先 WebSocket，仅因传输不可用切换到 HTTP long-poll，并在安全点探测恢复。
- `TransportWebSocket`：严格只使用 WebSocket。
- `TransportHTTP`：严格只使用 HTTP long-poll。

认证、mTLS、权限、payload 和 contract 错误不会触发 fallback。`runtime_ws`、`runtime_pull` 和 `WithConnector` 只在 `v0.2.x` 迁移窗口保留兼容；新代码应使用 `TransportMode` 和 `WithTransportMode`。
