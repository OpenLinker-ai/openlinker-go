# RuntimeStore、并发、安全与可观测性

合并后的 Managed Worker 使用远端 `RuntimeStore` 作为唯一 durable 边界。它比合并前的整份 JSON `RuntimeWorkerStore` / adapter 更细：assignment、payload、Event、Result 和 ACK 都有独立方法。

## RuntimeStore 边界

`RuntimeStore` 负责：

- 稳定 Worker identity 和进程级 SessionEpoch。
- assignment journal 的 received、ACK sent、confirmed、started、finished、revoked 状态。
- assignment payload 与 delegation authority 的安全保存。
- Event append/range replay/ACK。
- Result save/ACK 与 terminal Event 清理。
- 是否继续接受新任务以及关闭资源。

所有 mutation 必须在 durable 后返回成功。Worker 始终先落盘，再执行协议写入。

## 默认 FileRuntimeStore

`OpenFileRuntimeStore(dataDir)` 提供：

- 加密的 assignment payload、Event 和 Result spool。
- identity checksum、journal WAL 和 snapshot。
- 单进程文件锁，拒绝两个 Worker 同时写同一目录。
- 原子写、fsync、私有权限和跨平台实现。
- 文件大小、记录数量、整数边界和可用磁盘空间检查。
- 损坏、缺失 identity、密钥不匹配和状态非法时明确失败，不静默重建 durable 数据。

生产环境可以注入数据库型 Store，但不应再实现第二套 Worker journal 语义。

## 并发边界

- Worker 的 `stateMu` 只保护 draining、active Attempt、spool permission 和 ready 状态。
- 每个 active Attempt 独立维护 cancel、lease 和完成状态。
- transport gate 在切换 attachment 前阻止新的协议操作，并等待旧 generation 的 in-flight 调用退出。
- Store 自己负责 journal/spool 的锁、WAL 和 durable 顺序。
- handler 完成不等于 Attempt retired；Result ACK 或明确 revoke 前仍保留 lease 和 active Attempt。

## 日志安全

Worker 使用标准 `log.Logger`，所有协议错误在输出前经过 `scrubRuntimeError`。日志记录 transport、spool retry、assignment defer、cancel 和恢复阶段，但不应包含 Agent Token、NodeEnvelope、Invocation Token、私钥内容或明文加密 key。

Native/极简 facade 的 `WithLogger` 接受 `*log.Logger`。handler、工具和自定义 Store 仍需自行保证 payload 与凭证脱敏。
