# RuntimeWorker Session 与 Transport Supervisor

合并后的 supervisor 来自远端可靠 Worker，负责 Session、attachment generation、WebSocket/Pull 切换和 resume。

## Identity

- WorkerID 在同一 DataDir 内稳定。
- 每次 `OpenFileRuntimeStore` 代表新的进程启动：生成新的 RuntimeSessionID，并 durable 地递增 SessionEpoch。
- 同一进程内 transport 重连和切换复用当前 RuntimeSessionID / SessionEpoch。
- NodeID 和 AgentID 来自显式配置或环境，不由本地随机生成。

## 启动流程

```text
打开并锁定 RuntimeStore
        ↓
创建进程级 Session identity
        ↓
Auto 模式优先 attach WebSocket
        ↓
建立 Session / Ready
        ↓
根据 durable journal 执行 resume
        ↓
启动 claim、command、heartbeat、spool 和 transport supervisor
```

新的 transport 只有在 attach 和 resume 成功后才会发布给协议操作。

## Attachment generation fencing

HTTP/Pull 的 Session create 会得到 attachment ID。除 Session create 和 assignment-scoped delegation 外，后续请求携带 attachment header。

transport gate 使用 generation/epoch 防止以下竞态：

- 旧 attach 响应覆盖新 attachment。
- shutdown 后迟到的 WebSocket attach 被重新发布。
- transport 切换过程中旧请求继续使用已失效 generation。
- WebSocket close 导致已经路由到 waiter 的 reply 丢失。

切换前 gate 停止新操作并等待旧 generation 的 in-flight 调用退出。

## Heartbeat 与重试

Worker 默认每 5 秒 heartbeat，可通过 `RuntimeWorkerConfig.HeartbeatInterval` 调整。网络和临时服务错误按最小/最大退避重试；认证、contract、协议 mismatch 和 durable Store fatal error 直接终止。

Event/Result 上传循环与 claim/command loop 共用当前 transport gate。连接恢复后继续处理 durable spool，不生成新的业务 ID。
