# RuntimeWorker Attempt 生命周期

本文描述合并后 canonical `RuntimeWorker` 的 deadline、lease、cancel、drain 和 durable-write 行为。

## Assignment 与 handler

Worker 收到 assignment 后按顺序执行：

1. 校验本地 Node、Agent、Worker、Session 与 fencing identity。
2. 创建 assignment journal。
3. 加密保存 input、metadata、NodeEnvelope 和 Invocation Token。
4. 持久化 `ACKSent` 后向 Core ACK。
5. 收到 confirmation 并持久化 `Confirmed` 后才启动 handler。

重放已进入 `Started` / `Finished` 的 assignment 时只重新 ACK，不会再次执行 handler。

## Deadline

handler context 使用 `min(AttemptDeadlineAt, RunDeadlineAt)`。超时前未开始或执行中超时都会生成稳定失败：

```text
ATTEMPT_DEADLINE_EXCEEDED
```

失败 Result 先进入加密 spool，再由上传循环提交。handler 返回后 Attempt 仍保留 lease，直到 Result ACK 或明确 revoke。

## Lease 与 revoke

lease renewal 使用当前 fenced identity 和最后 ACK Event sequence。收到 stale lease、expired lease 或 revoke 后，Worker 停止 handler、禁止继续提交，并按照 durable journal 状态完成清理。

临时传输失败不会重新执行 handler；transport 恢复并完成 resume 后，原 EventID、ResultID 和 sequence 继续上传。

## Cancel

取消命令按 correlation 和 durable Attempt 状态处理：

```text
stopping -> stopped
```

Worker 先取消 handler context；只有 handler 真正退出后才发送 `stopped`。取消 ACK 失败会保留相关状态并重试，不会提前释放仍在运行的 handler。

## Drain 与 Stop

`Stop(ctx)` 将 Worker 设为 draining、停止接受新任务并触发 shutdown。shutdown 会等待 Runtime loops 和 active execution 按 deadline 收敛，关闭当前 attachment、HTTP idle connection 和 Store。

Worker 是单次使用对象；`Start` 返回后不能再次启动同一个实例。

## Durable-write 原则

| 阶段 | durable 边界 |
| --- | --- |
| assignment | journal/payload 成功后才 ACK 和执行。 |
| Event | `AppendEvent` 成功后才加入上传队列。 |
| Event ACK | Store ACK 成功后才推进 sequence。 |
| Result | `StoreResult` 成功后才提交。 |
| Result ACK | `AckResult` 成功后才 retire Attempt。 |
| resume | 依据 Core decision 恢复上传权限、继续执行或 revoke。 |

任何被分类为 fatal 的 durable error 都会停止 Worker，避免继续接收无法可靠恢复的新任务。
