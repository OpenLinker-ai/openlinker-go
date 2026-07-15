# Runtime 远端冲突合并报告

## 基线

- 本地原始基线：`976a34b`。
- 本地 Action Items 1–8 快照：`codex/runtime-api-modes-pre-sync` / `f951172`。
- 远端合并基线：`origin/main` / `0fc8f13`。
- 集成分支：`codex/runtime-api-modes-reconcile`。

远端比本地原始基线新增 12 个 Runtime 提交。临时 merge 演练得到 17 个实际冲突：4 个内容冲突、8 个 modify/delete 冲突和 5 个 add/add Worker 冲突。

## 采用的远端实现

- generation-free `Runtime*` 协议类型和 `contracts/core-runtime.json`。
- attachment header 与 generation fencing。
- HTTP/Pull、WebSocket client 和 routed reply close 处理。
- 单一可靠 `RuntimeWorker`、Session supervisor、claim、heartbeat、resume、cancel 和 drain。
- assignment journal、加密 payload/Event/Result spool。
- `FileRuntimeStore` 的 identity、WAL、snapshot、checksum、锁、权限、空间和记录限制。
- Result ACK 前保留 active Attempt 和 lease 的语义。

## 保留并迁移的本地能力

- `WithAgent` / `WithFunc` 极简 Agent facade。
- `Native(handler)`、`NativeRun`、Message/Progress/Emit 和 Result helper。
- `RunOrRegister`、`WithRegistration`、`AgentSpec` 和 registration policy。
- creator Agent/Token API、pending Agent Token 注册和 `.env` registration store。
- 标准环境变量解析、聚合配置错误、mTLS helper 和显式配置覆盖。
- 四种 API mode 编译契约、layout handler contract、中文设计文档和最小 Agent 示例。

这些能力现在都映射到远端 canonical `RuntimeWorkerConfig`、`RuntimeHandler` 和 `RuntimeStore`，不再拥有独立 supervisor 或 spool。

## 未重新引入的本地实现

- `runtime_v2_*` 文件和 `RuntimeV2*` 公共命名。
- 本地第二套 `runtime_worker_assignment/events/commands/state` 状态机。
- 整份 JSON `RuntimeWorkerStore`、`RuntimeJournal` adapter 和 schema v1 文件。
- 本地自定义结构化 Logger 接口。
- 自定义 transport 注入型 Managed Worker 公共 API。

对应目标已经由远端更完整的 Worker、加密 Store、transport gate 和错误 scrub 覆盖。

## 公开 API 调整

- 底层协议统一使用 `Runtime*`，不提供 generation alias。
- Managed Worker 使用 `NewRuntimeWorker(RuntimeWorkerConfig) (*RuntimeWorker, error)`。
- Managed Worker 的原生生命周期是 `Start` / `Stop`；`Run` 是 facade 兼容别名。
- 高层 `WithAgent` / `Native` 继续使用 `Run` / `RunOrRegister`。
- 默认状态从单个 JSON 文件变成 `OPENLINKER_RUNTIME_DATA_DIR` 下的加密 Store；旧 `OPENLINKER_RUNTIME_STATE_PATH` 仅转换为兼容目录。
- NodeID 必须是已登记身份；WorkerID 由 Store 稳定保存，每次进程启动生成新的 RuntimeSessionID 并递增 SessionEpoch。

## 验证边界

合并完成需要同时通过：

- 根 module 普通测试、Race 和 Vet。
- 远端 `examples/runtime-echo`。
- 本地嵌套 `example/runtime/agent-generic` module。
- Runtime API generation boundary、Core Runtime contract 和 registration contract。
- `openlinker-core/pkg/runtime` 定向测试。

## 验证结果

- `go test ./... -count=1`：通过。
- `go test -race ./...`：通过。
- `go vet ./...`：通过。
- `examples/runtime-echo`：随根 module 普通测试和 Race 通过。
- `example/runtime/agent-generic` 的 test/vet：通过。
- Auto transport 与高层 facade 的关键 Race 用例连续 5 次：通过。
- `openlinker-core/pkg/runtime` 定向测试：通过。
