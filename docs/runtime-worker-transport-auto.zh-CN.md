# RuntimeWorker TransportAuto

`TransportAuto` 是极简 Agent、Native 和 Managed Worker 的默认传输策略。它与 API 层级正交。

## 状态流转

```text
connecting_ws
      ↓ WebSocket 不可用
switching_to_pull → pull_active
      ↓ probe 成功
switching_to_ws → ws_active
      ↓ WebSocket 断开
switching_to_pull
```

`RuntimeTransportAuto` / `TransportAuto` 优先 WebSocket。初始 WebSocket 失败或运行中断开时切到 HTTPS long-poll；Pull 健康期间按指数退避 probe WebSocket，恢复后重新 attach、resume 并发布 WebSocket。

## 安全切换点

切换不依赖“当前恰好没有 Attempt”这种脆弱时序。`switchingRuntimeClient` 会：

1. 关闭旧 generation，阻止新的协议操作进入。
2. 等待旧 generation 的 in-flight 调用退出。
3. attach 新 transport。
4. 使用新 transport 完成 durable resume。
5. 原子发布新 generation，并唤醒 Event/Result spool。

因此正在运行的 handler 不会被重新执行，Event/Result 仍使用原 ID 和 sequence。

## 错误分类

网络、EOF、WebSocket 断开和可重试 attach/session 错误允许 fallback 或重试。认证、mTLS、contract、required feature、协议 mismatch 和其他永久错误直接返回，不会被伪装成 WebSocket 不可用。

WebSocket probe 成功但 attach/resume 遇到可重试 Session conflict 时，Worker 会恢复 Pull，后续继续 probe，不会停留在无 transport 状态。

## 显式模式

- `RuntimeTransportWebSocket` / `TransportWebSocket`：只使用 WebSocket，断开后继续重连 WebSocket。
- `RuntimeTransportPull` / `TransportHTTP`：只使用 HTTP long-poll，不启动 WebSocket supervisor。
- `RuntimeTransportAuto` / `TransportAuto`：WebSocket 优先并自动切换。

底层自研 Runtime 实现绕过 `RuntimeWorker` 时，需要自行实现同等的 generation fencing、resume 和 spool 恢复。
