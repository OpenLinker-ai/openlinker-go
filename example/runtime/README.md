# Runtime 示例

Runtime 示例按照 API 层级组织，而不是按照 HTTP/WebSocket 传输方式组织：

- `agent-generic`：极简 `WithAgent(...).Run()`，普通 Agent 的推荐入口。
- `agent-register`：显式 `RunOrRegister` / `WithRegistration`。
- `native-events`：Native handler、MessageDelta、Emit、Progress 和 Result helper。
- `native-delegation`：assignment-scoped Agent-to-Agent delegation。
- `worker-managed`：自定义 Store、capacity、logger 和 transport mode。
- `protocol-http`：底层 Runtime v2 HTTP 原语。
- `protocol-websocket`：底层 Runtime v2 WebSocket 原语。

普通 Agent 项目应从 `agent-generic` 开始；底层 protocol 示例不会替开发者管理 session、lease、spool、resume 和 reconnect。
