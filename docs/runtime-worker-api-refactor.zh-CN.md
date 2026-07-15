# RuntimeWorker API 收敛与内部模块

远端合并后，可靠 Worker 只保留一套状态机。普通 Agent 使用 `WithAgent` / `WithFunc`，框架使用 `Native`，高级调用方直接使用 `NewRuntimeWorker(RuntimeWorkerConfig)`，基础设施实现可以只依赖 `NewRuntime` 协议原语。

## 三个 Managed 入口

极简 Agent：

```go
err := openlinker.WithAgent(agent).Run(ctx)
```

Native facade：

```go
err := openlinker.Native(agent.Handle).
    WithRegistration(spec).
    Run(ctx)
```

直接构造 Worker：

```go
worker, err := openlinker.NewRuntimeWorker(openlinker.RuntimeWorkerConfig{
    PlatformURL: platformURL,
    NodeID: nodeID, AgentID: agentID, AgentToken: agentToken,
    DataDir: dataDir, MTLS: mtls, Handler: handler,
})
if err == nil {
    err = worker.Start(ctx)
}
```

`RuntimeWorker.Run(ctx)` 是 `Start(ctx)` 的 facade 兼容别名。Worker 本身单次使用；停止使用 `Stop(ctx)`。

## 关键模块

| 文件 | 职责 |
| --- | --- |
| `runtime_worker.go` | Worker 生命周期、启动、停止、shutdown 与公共配置字段。 |
| `runtime_worker_types.go` | `RuntimeWorkerConfig`、`RuntimeContext`、handler/result/store 接口。 |
| `runtime_worker_supervisor.go` | WebSocket/Pull attach、fallback、probe、promotion 与 generation fencing。 |
| `runtime_worker_session.go` | Session、heartbeat、claim、assignment journal 和 resume。 |
| `runtime_worker_attempt.go` | handler、deadline、lease、Event/Result 构造和 delegation。 |
| `runtime_worker_cancel.go` | cancel correlation、stopping/stopped ACK。 |
| `runtime_worker_spool.go` | durable Event/Result 上传和 ACK 重放。 |
| `runtime_store_*.go` | identity、加密、WAL、snapshot、锁、限制和 spool。 |
| `runtime_native.go` | 极简/Native facade、helper、环境配置映射和显式注册组合。 |

## 冲突合并原则

- 远端 `RuntimeWorker` 和 `RuntimeStore` 是唯一可靠状态机。
- 本地旧 JSON Store、`RuntimeJournal` adapter 和第二套 supervisor 没有重新引入。
- 公共协议名采用 `Runtime*`，不恢复 `RuntimeV2*` generation 名称。
- `WithAgent` / `Native` 只负责把易用配置和 handler 映射到 canonical Worker。
- `openlinker-agent-node` 是可选 adapter shell，不再拥有第二套 Runtime 状态机。

## 底层协议独立

`runtime_client.go`、`runtime_http.go`、`runtime_websocket.go`、`runtime_websocket_client.go` 和 `runtime_invocation.go` 不依赖 `RuntimeWorker`、`NativeRun` 或 `NativeResult`。协议实现者可以只使用底层 API，自行负责持久化和生命周期。
