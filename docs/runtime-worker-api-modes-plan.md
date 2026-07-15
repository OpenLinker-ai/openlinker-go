# Plan

**Status:** Inprogress
**Target release:** `v0.2.0`（先发布 `v0.2.0-rc.1`）
**Review gate:** 本计划通过 review 前不开始实现

> 2026-07-15 远端合并说明：远端 `main` 已提供更完整的可靠 `RuntimeWorker`、加密 `RuntimeStore`、transport generation fencing 和无 `RuntimeV2*` 公共命名。Action Items 1–8 的目标保持不变，但最终实现以远端 Worker/Store 为唯一底座，本地四层 API 与注册能力作为 facade 迁移；旧文件名、JSON Store 和 `RuntimeJournal` 方案不再代表当前实现。详见 `runtime-remote-reconcile-plan.md`。

将 `openlinker-go` 演进为分层、可嵌入且默认易用的 Runtime v2 SDK：普通开发者只需使用 `WithAgent(...).Run()`，框架开发者使用 Native Runtime，Agent Node 和基础设施实现则复用 Managed RuntimeWorker 或底层协议原语。实现顺序先补齐 Runtime v2 正确性与恢复能力，再收敛公开 API、拆分内部状态机，最后通过兼容层、下游集成和候选版本完成发布。

## Scope

- In:
  - 在 `openlinker-go` 中正式定义并实现四个 API 层级：极简 Agent、显式自动注册、Native Runtime、底层 Runtime v2 协议。
  - 将 WebSocket/HTTP 从“开发模式”中解耦，提供默认 `TransportAuto`、显式 WebSocket、显式 HTTP 三种传输策略。
  - 补齐 Session heartbeat、自动重连、resume、transport fallback/promotion、deadline、cancel/drain、Store 错误传播等 Runtime v2 语义。
  - 简化环境配置、mTLS 加载、身份持久化、默认 Store、注册请求、事件与 Result API。
  - 在 `example/` 下建立类似 Blades examples 的单概念 demo 集合，完整覆盖 Client、Registration、Runtime、A2A 和 Webhook 的推荐使用路径。
  - 保留一个发布周期的兼容入口，提供测试、文档、示例以及 `openlinker-agent-layout` / `openlinker-agent-node` 迁移指引。
  - 以当前 `runtime_worker.go`、`runtime_native.go`、`runtime_worker_store.go`、`registration_*` 和 Runtime v2 transport 实现为基础演进，不恢复任何 Runtime v1 connector 或 fallback。
- Out:
  - 不修改 OpenLinker Core 的 Runtime v2 contract、服务端调度语义或认证模型；若实现中发现 contract 缺口，单独提出 Core 变更计划。
  - 本计划阶段不直接迁移 `openlinker-agent-layout` 或 `openlinker-agent-node` 仓库，只定义其接入契约和验收用例。
  - SDK 默认 `FileRuntimeStore` 已提供加密 spool、跨平台私有权限、文件锁和 durable WAL；调用方仍可注入其他 `RuntimeStore`。
  - 不支持因认证、mTLS、权限或 contract 错误而降级到其他传输，也不引入 Runtime v1 兼容模式。

## Action items

[x] 1. 冻结目标 API、兼容边界和当前行为基线。
  - 在设计基线中明确四种开发使用模式及其依赖方向：`WithAgent` / `WithFunc` → `Native` → Managed `RuntimeWorker` → Runtime v2 Protocol；自动注册仅作为极简和 Native 模式上的显式启动能力。
  - 明确传输是正交配置：新常量使用 `TransportAuto`、`TransportWebSocket`、`TransportHTTP`，默认 `TransportAuto`；现有 `runtime_ws`、`runtime_pull` 和 `WithConnector` 仅作为一个发布周期的 deprecated 兼容入口。
  - 盘点并记录当前公开 API，重点覆盖 `RuntimeWorker` 的公开字段、`NativeAgentRunner` 的二十余个 `WithXxx`、`EnsureAgentRequest`、`NativeResult`、`AgentEvent` 和 Store 接口，避免重构时无意破坏下游。
  - 为四种模式各增加一个只做编译验证的示例或 example test，先锁定期望调用形式，再开始内部改造。
  - 建立基线命令：`go test ./...`、`go test -race ./...`、`go vet ./...`；保存当前结果，后续每个阶段都必须维持通过。
  - 涉及文件：`runtime_native.go`、`runtime_worker.go`、`registration_types.go`、`README.md`、`README.zh-CN.md`、`example/runtime/`、新增 API compile tests。
  - 验收：四种模式、传输策略、兼容期限和预期 `v0.2.0` breaking changes 在代码测试与文档中有唯一一致的定义。

[x] 2. 建立统一配置加载与真正可用的极简 Agent 启动路径。
  - 新增内部 `RuntimeWorkerConfig` 和集中式校验，将配置优先级固定为“显式配置 > 环境变量 > 已持久化状态 > SDK 默认值”。
  - 让 `WithAgent(agent).Run(ctx)` 与 `WithFunc(fn).Run(ctx)` 自动解析 Runtime base、Agent ID、Agent Token、capacity、transport、state path 和 mTLS 文件配置，不要求普通用户手工创建 `Runtime`、`http.Client`、NodeID、WorkerID 或 Store。
  - 增加基于环境变量的 mTLS client 构造，支持 Node certificate、private key、Runtime CA 和可选 server name；一次性返回全部缺失或冲突配置，而不是逐次失败。
  - 保持显式注入优先：现有 `WithRuntime`、`WithHTTPClient`、`WithTransport`、`WithStore` 用于测试或高级定制时不得被环境配置覆盖。
  - 默认 Store 目录使用 `OPENLINKER_RUNTIME_DATA_DIR`，未设置时落到 `.openlinker/runtime-<agent-id>`；旧 `OPENLINKER_RUNTIME_STATE_PATH` 只作为 facade 兼容输入。
  - NodeID 使用已登记的显式身份；WorkerID 由 Store 稳定持久化，每次进程启动生成新的 RuntimeSessionID 并递增 SessionEpoch。
  - 涉及文件：新增 `runtime_worker_config.go`、`runtime_worker_tls.go`，调整 `runtime_native.go`、`runtime_worker_store.go`、`client.go` 或 Runtime client option 辅助函数。
  - 验收：在只设置必要环境变量和有效 mTLS 文件时，最小示例无需额外 builder 调用即可进入 Runtime ready；配置缺失时一次返回完整、无敏感信息的诊断。

[x] 3. 将一次性 Worker 循环改造成可恢复的 Session supervisor。
  - 从当前 `RuntimeWorker.Run` 中抽离外层 supervisor，形成“加载状态 → 建立连接 → resume → serve → 判断错误 → backoff 重连”的生命周期。
  - 对临时网络错误、WebSocket 非正常关闭、HTTP `502/503/504` 和可重试超时执行带随机抖动的指数退避；对 `401/403`、mTLS、contract digest、feature mismatch 和输入校验错误立即终止。
  - 重连时复用 RuntimeSessionID，并按照 Runtime v2 contract 递增 SessionEpoch；未 ACK 的 assignment journal、Event spool 和 Result spool 不得被清理或重新生成 ID。
  - 将 `HeartbeatRuntimeSession` 纳入 worker transport 能力；heartbeat 间隔有安全默认值和显式配置，WebSocket 断线反馈给 supervisor。
  - 统一 session close、父 context cancel 和 graceful shutdown 行为，避免正常退出被错误识别为需要重连；关闭失败记录日志但不得覆盖更重要的主错误。
  - 涉及文件：`runtime_worker_supervisor.go`、`runtime_worker_session.go`、`runtime_worker.go`、`runtime_http.go`、`runtime_websocket_client.go` 和 transport fake。
  - 验收：无需重启进程即可从 HTTP 临时错误和 WebSocket 断线中恢复；重连后 Core 收到同一 session identity、递增 epoch 以及稳定的 Event/Result ID。

[x] 4. 实现默认 `TransportAuto` 及安全的 fallback / promotion。
  - 初次连接优先 WebSocket；仅对代理不支持 upgrade、网络不可达、连接被中间设备终止等传输类错误切换到 HTTP long-poll。
  - 建立统一错误分类，禁止在 Agent Token、mTLS、权限、contract 或 payload 校验错误时 fallback，避免把真实配置问题伪装成传输问题。
  - HTTP fallback 期间定期使用现有 WebSocket probe 能力检测恢复；仅在没有进行中的协议写操作或其他定义明确的安全切换点 promotion 回 WebSocket。
  - 切换传输必须经过 supervisor 的 close/connect/resume 流程，不得创建第二套 assignment、lease、command 或 spool 状态机。
  - 显式 `TransportWebSocket` 和 `TransportHTTP` 必须严格遵循用户选择，不执行自动切换；直接 Managed Worker 使用 canonical transport client/dialer。
  - 涉及文件：`runtime_worker_supervisor.go`、`runtime_worker_transport.go`、`runtime_websocket_client.go`、`runtime_http.go`、公开 transport mode 定义。
  - 验收：覆盖 WebSocket→HTTP fallback、HTTP→WebSocket promotion、认证错误不 fallback、显式传输不切换四类测试，且每次切换后都先 resume 再 claim 新任务。

[x] 5. 补齐 Attempt deadline、lease、cancel、drain 和 durable-write 正确性。
  - handler context 自动绑定 `min(AttemptDeadlineAt, RunDeadlineAt)`；超时后取消 handler，并使用稳定 ResultID 生成 `ATTEMPT_DEADLINE_EXCEEDED` 失败结果，仍遵循“先落盘、后提交”。
  - 统一 lease renewal 错误处理：临时错误进入 attempt/session 恢复策略，明确 revoke 后停止 handler 且禁止提交过期结果；任何 lease 状态持久化失败都停止接收新 assignment 并进入 drain。
  - 完成 cancel 状态闭环：收到命令后依次处理 delivered/stopping，handler 退出后 ACK stopped；超出 cancel deadline 由 Core reaper 推进 unconfirmed，协议提交错误使用 failed，并使用独立 operation context 避免复用已取消 handler context。
  - 明确 drain 的语义：停止 claim 新任务、继续处理或恢复已有 attempt、等待 inflight 到零、最后关闭 session；父 context 强制结束时保留所有未 ACK spool。
  - 消除当前被忽略的 Store 保存错误，包括 lease 更新、revoke 清理和其他 `_ = saveStateLocked(...)` 路径；定义 assignment/Event/Result 各阶段落盘失败时的拒绝、停止或重试规则。
  - 检查 assignment ACK 成功但确认状态保存失败等不可逆窗口，确保 restart/resume 能够与 Core 决策收敛，不会重复执行 handler。
  - 涉及文件：新增 `runtime_worker_attempt.go`、`runtime_worker_commands.go`、`runtime_worker_assignment.go`、`runtime_worker_events.go`，调整 Store state schema 与 resume 逻辑。
  - 验收：deadline、cancel deadline、lease revoked、drain、每一个 Store failure injection 用例均不会丢失 spool、重复执行 handler 或提交 fenced Result。

[x] 6. 完成 Native Runtime 与注册能力的易用 API。
  - 为 `NativeRun` 增加通用 `Emit`，允许传入结构体或 map 并在 SDK 内验证为 JSON object；保留 `SendEvent` 兼容入口，并提供 `Message` / `MessageDelta`、`Progress` 等常用 helper。
  - 增加 `Success`、`Failure`、`RetryableFailure` helper，统一 Native handler 的 Result/Error 映射、空输出和 panic 行为，减少直接构造 `NativeResult` / `AgentError`。
  - 保留完整 Assignment、Metadata、Attempt/Run identity、delegated call、cancel/deadline context 能力，确保 `openlinker-agent-layout` 可只实现 Harness 与业务 handler。
  - 将庞大的 `EnsureAgentRequest` 分层为精简 `AgentSpec` 与注册 options；保留旧请求入口为 deprecated wrapper，内部统一走同一注册实现。
  - 增加 `WithRegistration(spec, options...)`，并让 `RunOrRegister` 接收精简 spec；自动创建或修改平台资源必须始终由调用方显式启用，不能仅因检测到 `OPENLINKER_USER_TOKEN` 而发生。
  - 首次注册保存 AgentID 和 Agent Token，后续启动优先复用注册状态；User Token 仅在创建、验证或轮换确实需要时使用，日志和错误中不得泄露明文 Token。
  - 涉及文件：`runtime_native.go`、`registration_types.go`、`registration_bootstrap.go`、`registration_store.go`、`registration_client.go`、相关 contract tests。
  - 验收：极简、极简+注册、Native、Native+注册四条入口均可用同一 RuntimeWorker 生命周期运行，且无静默资源创建。

[x] 7. 收敛公开 RuntimeWorker API，并拆分内部状态机。
  - 采用远端按职责拆分的 Worker、session、attempt、cancel、spool、transport 与 `runtime_store_*` 模块；拆分以状态机职责和独立测试为准。
  - 直接 Managed Worker 使用 `NewRuntimeWorker(RuntimeWorkerConfig)`；普通入口通过 facade 暴露 `WithStore`、`WithTransportMode`、`WithCapacity`、`WithLogger`、`WithRegistration` 等扩展点。
  - 旧的字段赋值方式和 `WithNodeID`、`WithWorkerID`、`WithPullWait`、`WithConnector` 等方法保留一个发布周期，添加 deprecated 注释并通过 adapter 映射到新配置。
  - 保证底层协议 API `NewRuntime`、HTTP Runtime v2 primitives 和 WebSocket primitives 不依赖 Managed Worker，使基础设施实现仍可只选择最底层。
  - 避免 `NativeAgentRunner` 和 `RuntimeWorker` 各自维护一套配置解析；所有 facade 必须最终生成同一个 validated worker config。
  - 涉及文件：拆分后的 `runtime_worker_*.go`、`runtime_native.go`、公开 option 定义、兼容测试。
  - 验收：核心状态机模块可以独立单测；新 API 没有公开协议身份细节；旧调用方式在兼容期内仍编译并产生等价配置。

[x] 8. 优化 Store、并发、安全与可观测性边界。
  - 缩小全局 mutex 范围：锁内只修改内存状态并生成不可变 snapshot，锁外执行 JSON 编码和磁盘 I/O；每个 attempt 保持独立 event 顺序锁，避免 capacity > 1 时互相阻塞。
  - 采用细粒度 `RuntimeStore` 接口，覆盖 identity、assignment/payload、Event append/range/ACK、Result save/ACK 和 terminal cleanup。
  - 默认 `FileRuntimeStore` 提供加密、WAL/snapshot、checksum、文件锁、私有权限、磁盘与记录限制；损坏或 identity/key 不一致时明确失败。
  - Worker 使用标准 `log.Logger`，协议错误在输出前统一 scrub，避免泄露 Agent Token、NodeEnvelope、InvocationToken 和密钥材料。
  - 涉及文件：`runtime_store_*.go`、`runtime_worker_spool.go`、`runtime_worker_attempt.go` 和相关并发/故障测试。
  - 验收：capacity 4 并发下通过 race test，无全局磁盘写锁导致的明显串行化；故障日志足以定位阶段且不包含密钥或 invocation credential。

[x] 9. 建立 Runtime v2 chaos、契约和回归测试矩阵。
  - 扩展 fake transport 为可编排的 chaos transport，可在 session create、claim、assignment ACK、lease、Event、Result、command、resume 和 close 的请求前后注入断线、ACK 丢失、重试错误或永久错误。
  - 覆盖：HTTP heartbeat、WebSocket 重连、fallback/promotion、assignment 落盘后 ACK 前崩溃、ACK 后本地保存失败、Event/Result ACK 丢失、lease 超时/revoke、deadline、cancel deadline、drain、Store 损坏、capacity 4、多 attempt resume 和 retryable Result ACK。
  - 对 stable ID 和 sequence 增加断言：重放不得生成新 EventID/ResultID，Event sequence 必须单调，Result final sequence 必须与 ACK 状态一致。
  - 增加 mTLS/env config 单测，使用临时证书验证缺文件、错误 CA、key mismatch、server name 和显式 client override。
  - 保持 Runtime contract digest、HTTP/WebSocket semantic tests 和 registration contract tests；如需跨 Core 验证，提供可选 integration test，而不让普通 `go test ./...` 依赖外部服务。
  - 在 CI 中固定执行 `go test ./...`、`go test -race ./...`、`go vet ./...`，并对关键 supervisor/chaos 用例执行重复测试以发现时序问题。
  - 中文测试矩阵、故障注入语义和本地/Core 验证命令见 `runtime-chaos-test-matrix.zh-CN.md`。
  - 涉及文件：`runtime_worker_test.go` 拆分后的测试文件、transport fakes、TLS tests、contract tests、CI 配置。
  - 验收：所有已知不可逆窗口都有确定测试；测试能证明重连、resume 和 spool replay 不依赖进程重启或偶然时序。

[ ] 10. 按 Blades 的单概念颗粒度建立 `example/` demo 体系，并优先补齐 Client 侧 API。
  - 阶段进度：
    - [x] 建立共享 `example` module、分类目录、中文索引和 `internal/exampleutil`，迁移 `runtime/agent-generic`。
    - [x] 实现 Client 侧六个单概念示例及离线协议测试。
    - [x] 实现 Registration 示例及显式资源变更保护。
    - [ ] 实现极简、注册、Native、Managed Worker 和底层 Protocol Runtime 示例。
    - [ ] 实现 A2A 与 Webhook 示例。
    - [ ] 补齐示例总索引、根 README/CI、全量测试与 smoke test 说明，完成 Item 10 验收。
  - 参考 [go-kratos/blades examples](https://github.com/go-kratos/blades/tree/main/examples) 的组织原则：整个示例集合使用一个共享 module，每个叶子目录只回答一个问题，普通示例以单个 `main.go` 为主，只有场景本身需要时才增加同目录 helper、配置文件或测试。
  - 保留仓库现有的 `example/` 单数目录，不为了对齐参考项目而做无收益的目录重命名；将当前 `example/runtime/agent-generic` 的独立 `go.mod` / `go.sum` 合并到 `example/go.mod` / `example/go.sum`，并使用 `replace github.com/OpenLinker-ai/openlinker-go => ../` 测试当前工作树。
  - 使用“API 大类目录 + 单概念叶子目录”的两级结构，避免 OpenLinker 的 Client、Runtime、A2A 与 Webhook 示例全部平铺；计划目录如下：

    ```text
    example/
      README.md
      go.mod
      go.sum
      internal/exampleutil/

      client/
        agent-discovery/
        run-sync/
        run-async/
        run-stream/
        run-callbacks/
        run-history/

      registration/
        ensure-agent/
        token-management/

      runtime/
        agent-generic/
        agent-register/
        native-events/
        native-delegation/
        worker-managed/
        protocol-http/
        protocol-websocket/

      a2a/
        jsonrpc/
        http-json-sse/
        grpc/

      webhook/
        verify-request/
    ```

  - 为 Client 侧建立六个最小示例，确保公开应用 API 不只存在于 README 片段中：
    - `client/agent-discovery`：只演示 `NewClient`、`ListAgents`、`GetAgent` 和 `GetAgentCard`，输出可调用 Agent 的基础信息。
    - `client/run-sync`：只演示带显式 idempotency key 的 `RunAgent`，输出最终状态与结果。
    - `client/run-async`：只演示 `StartAgentRun`、轮询 `GetRun` 和 context timeout，解释异步提交与同步等待的差异。
    - `client/run-stream`：只演示 `StartAgentRun` 后使用 `StreamRunEvents` 消费 SSE，正确处理终态、断流与 context cancel。
    - `client/run-callbacks`：只演示 platform callback helper，不引入外部 webhook server，展示 message delta 与 terminal callback。
    - `client/run-history`：在一个已存在的 Run 上读取 Events、Messages、Artifacts 和 Children，展示分页/retention 元数据而不重新发起任务。
  - 为 Registration 建立两个显式改变平台状态的示例：`ensure-agent` 演示创建或复用 Agent 与本地注册状态，`token-management` 演示列出、创建和撤销 Agent Token；后者必须要求明确 CLI flag 才执行创建/撤销，默认只读，避免复制运行时误删凭证。
  - 为四种 Runtime API 层级建立对应示例：
    - `runtime/agent-generic`：保留并更新现有最懒 `WithAgent(...).Run()`，只实现文本 handler。
    - `runtime/agent-register`：演示显式 `RunOrRegister` / `WithRegistration`，并说明首次与后续启动的凭证差异。
    - `runtime/native-events`：演示 Assignment/Metadata、MessageDelta、Emit、Progress 与 Success/Failure helper。
    - `runtime/native-delegation`：只演示 assignment-scoped Agent-to-Agent delegation 和 cancel/deadline context。
    - `runtime/worker-managed`：演示自定义 Store、capacity、logger 和 transport mode，但不手写协议状态机。
    - `runtime/protocol-http` 与 `runtime/protocol-websocket`：分别演示底层 Runtime v2 原语，并醒目标注开发者需自行负责 session、lease、spool、resume 和 reconnect，普通 Agent 不应从这里开始。
  - 为传输/集成 API 增加 `a2a/jsonrpc`、`a2a/http-json-sse`、`a2a/grpc` 和 `webhook/verify-request`；每个示例只覆盖一种绑定或签名验证流程，不把 A2A transport 与 Agent Runtime transport 混为一谈。
  - `example/internal/exampleutil` 仅允许提供重复且不属于 SDK 教学重点的能力，例如环境变量读取、context/signal、JSON pretty print、终态判断和测试 server；不得封装 `NewClient`、`RunAgent`、`WithAgent` 等核心调用，否则读者看不到真实 SDK 使用方式。
  - `example/README.md` 作为示例索引，按“从哪里开始”而非仅按字母排序：Client 调用方从 agent discovery/run sync 开始，普通 Agent 从 agent-generic 开始，框架从 native-events 开始，基础设施开发者最后查看 protocol 示例；每项列出用途、所需环境变量、是否创建外部资源和运行命令。
  - 每个 demo 使用一致的环境变量命名和错误输出，禁止硬编码 Token、Agent ID、证书路径或生产 endpoint；提供 `.env.example` 时只能包含占位符，并确保真实凭证、Runtime state 和临时证书被 `.gitignore` 排除。
  - 为示例建立可离线验证的测试：所有目录必须通过编译；Client/registration/webhook 示例使用 `httptest` 验证请求路径、认证头、idempotency key、SSE 和签名；Runtime 示例使用 fake transport 验证选用的 API 层级，不要求 CI 连接真实 Core。
  - CI 增加 `cd example && go test ./...`、`go vet ./...`，并在发布前用测试租户运行一组 opt-in smoke scripts；根 module 的 `go test ./...` 不会自动进入嵌套 example module，因此两套命令必须分别执行。
  - 涉及文件：新增 `example/go.mod`、`example/README.md`、上述 demo 目录与 tests，迁移 `example/runtime/agent-generic`，更新根 `README.md`、`README.zh-CN.md` 和 CI 配置。
  - 验收：每个公开 API 大类至少有一个可运行 demo；每个叶子目录只演示一个核心概念；新用户不阅读 SDK 内部实现即可从索引找到正确入口；示例 module 可独立完成 test/vet，且 smoke test 不泄露或提交任何真实凭证。

[ ] 11. 更新文档、示例索引并按 RC → 下游验证 → 正式版发布。
  - README 第一屏仅展示极简 `WithAgent(...).Run()`；随后依次说明显式自动注册、Native Runtime、Managed RuntimeWorker 和底层协议，避免普通 Agent 用户误入协议原语。
  - 从 README 链接到 `example/README.md`，并选择少量推荐 demo 作为主文档入口；完整示例留在 example 索引，避免 README 被所有场景代码淹没。
  - 增加四种模式的使用说明以及传输配置章节，明确“API 开发模式”和“WebSocket/HTTP 传输模式”互相独立；中英文文档保持同步。
  - 为 `openlinker-agent-layout` 提供迁移说明：使用 `Native(agent.Handle)`，Runtime 生命周期、transport、resume 和 Store 由 SDK 管理，layout 保留 Harness、trace、approval、artifact 与业务逻辑；依赖固定 RC/tag，不继续 vendor 或跟随 `@latest`。
  - 为 `openlinker-agent-node` 提供迁移说明：优先使用 SDK Managed RuntimeWorker，保留 CLI、adapter、TLS/证书装载、加密 Store、日志、signals 和 daemon 封装；仅协议测试工具或特殊集群实现使用底层原语。
  - 发布 `v0.2.0-rc.1`，用 `openlinker-agent-layout` 和 `openlinker-agent-node` 做真实集成验证；修复 RC 反馈后发布 `v0.2.0`，保留 deprecated wrappers 到约定的下一版本。
  - 在 CHANGELOG 中列出新默认值、breaking changes、兼容入口、迁移命令和 rollback 方式；下游必须固定明确 tag，并移除长期 `replace`、`vendor`、`@main` 或 `@latest` 依赖。
  - 发布门槛：SDK 全部测试通过；两个下游项目完成 smoke/integration test；文档示例可编译；无 Runtime v1 路由、类型或 fallback 回归。
  - 涉及文件：`README.md`、`README.zh-CN.md`、`CHANGELOG.md`、`example/runtime/`、新增 `docs/` 迁移文档及两个下游仓库的后续变更。
  - 验收：新用户可从 README 直接运行极简示例，高级用户能清楚找到 Native、Managed Worker 和 Protocol 层；RC 经两个下游验证后才转正式版本。

## Open questions

- 是否确认将本轮公开 API 收敛作为 `v0.2.0` breaking release，并仅保留一个发布周期的 deprecated wrappers？建议：确认，避免长期维护公开字段和两套 builder。
- `WithAgent(...).Run()` 的 mTLS 默认约定是否统一采用文件型环境变量，还是还需在首版同时支持 PEM 内容型变量？建议：首版只支持文件路径，显式 `WithHTTPClient` 覆盖其他证书来源。
- `TransportAuto` 从 HTTP promotion 回 WebSocket 的安全点是否限定为“当前没有 inflight attempt”，还是允许保留 attempt 并通过 resume 切换？建议：首版限定无 inflight，后续在 chaos 测试证明安全后再放宽。
