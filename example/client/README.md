# Client 示例

本分类将提供六个单一概念示例：

- `agent-discovery`：列出 Agent、读取详情和 Agent Card。
- `run-sync`：使用显式 idempotency key 同步运行 Agent。
- `run-async`：提交任务并轮询 Run 状态。
- `run-stream`：通过 SSE 消费 Run Event。
- `run-callbacks`：使用 SDK 的 platform callback helper。
- `run-history`：读取 Run 的 Events、Messages、Artifacts 和 Children。

这些示例将在 Item 10 的下一阶段实现，并使用 `httptest` 做离线协议验证。
