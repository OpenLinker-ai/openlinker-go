# Plan

**Status:** Completed
**Base:** 本地 `976a34b` + Action Items 1–8 未提交实现
**Target:** 远端 `main` `0fc8f13` 及其后续可验证提交

以远端可靠 `RuntimeWorker`、加密 `RuntimeStore` 和无 generation 公共命名为唯一底座，将本地已经完成的四层易用 API、显式注册、环境配置、中文文档和兼容测试迁移到新实现上。全过程先保留可回滚快照，再在独立集成分支处理冲突，Item 9 在本计划全部通过前保持暂停。

## Scope

- In:
  - 修复 Git pack sidecar 和 remote 凭证配置。
  - 保存当前工作区的完整可回滚快照。
  - 同步远端 12 个 Runtime 提交并解决实际冲突。
  - 采用远端 Worker、transport、加密 Store、spool、resume 和 attachment generation 实现。
  - 迁移本地 `WithAgent`、`WithFunc`、`Native`、registration、配置和中文文档。
  - 重新验证计划 Action Items 1–8。
- Out:
  - 不实现 Action Item 9 chaos 测试矩阵。
  - 不修改 `openlinker-core` 业务代码。
  - 不恢复远端明确删除的 `RuntimeV2*` 公共命名。
  - 不保留第二套并行 Worker 或持久化状态机。

## Action items

[x] 1. 修复 Git 环境并建立安全基线。
  - 隔离 `.git/objects/pack/._*` 无效 macOS sidecar，运行 `git fsck`。
  - 移除 origin URL 中嵌入的凭证，改用干净 HTTPS URL 和 credential helper。
  - 记录本地、远端提交和工作区文件清单。

[x] 2. 保存当前 Action Items 1–8 工作快照。
  - 创建 `codex/runtime-api-modes-pre-sync` 分支。
  - 提交当前 tracked/untracked 实现，确保任何迁移决定都可回滚或逐文件恢复。
  - 在快照上运行根 module 的普通测试。

[x] 3. 建立远端集成分支和基线。
  - 获取最新 `origin/main`，创建 `codex/runtime-api-modes-reconcile`。
  - 验证纯远端 `go test ./...`、`go test -race ./...` 和 `go vet ./...`。
  - 盘点远端新增的 Worker、Store、protocol、discovery 和 examples 能力。

[x] 4. 统一 Runtime 协议命名和 contract。
  - 接受 `runtime_v2_*` 到 `runtime_*` 的远端重命名和删除。
  - 将本地仍有价值的 HTTP/WebSocket semantic tests 迁移到新类型。
  - 采用 `contracts/core-runtime.json`、远端 digest、attachment header 与 session close/heartbeat contract。

[x] 5. 统一 Managed Worker 与持久化状态机。
  - 保留远端 `RuntimeWorker`、transport supervisor、assignment journal、加密 spool、resume、cancel 和 drain。
  - 不合并本地重复 Worker/Store 状态机；只迁移远端缺少的 API 或测试语义。
  - 对 deadline、TransportAuto、稳定 ID、日志安全和并发边界逐项建立保留/替代映射。

[x] 6. 迁移四层易用 API 与注册能力。
  - 在远端 `RuntimeWorkerConfig` / `RuntimeHandler` 上实现 `WithAgent`、`WithFunc` 和 `Native` facade。
  - 保留 `Run` / `RunOrRegister` 的推荐入口，并适配远端 `Start` 生命周期。
  - 迁移 `AgentSpec`、registration state、显式资源创建和环境配置，不引入第二套 Runtime 配置解析。

[x] 7. 合并文档、示例和兼容边界。
  - 合并中英文 README、CHANGELOG、API modes 文档和中文阶段总结。
  - 保留远端 `examples/runtime-echo`，不提前执行 Action Item 10 的完整 example 重组。
  - 更新 Action Items 1–8 的实现说明，使其引用合并后的真实文件和 API。

[x] 8. 执行完整验证和下游确认。
  - 运行 `go test ./...`、`go test -race ./...`、`go vet ./...`。
  - 运行本地嵌套 `example/` module 测试（如仍存在）。
  - 运行 `openlinker-core/pkg/runtime` 定向测试和公开 API/contract 编译测试。
  - 重复关键 transport/supervisor 测试，排除偶然时序通过。

[x] 9. 完成审核与交付。
  - 输出中文差异与冲突解决报告。
  - 标记本计划 Action Items，并重新审核主计划 Action Items 1–8。
  - 保持 Action Item 9 未开始，等待用户 review 后继续。

## Open questions

- 无。按已确认策略，以远端可靠 Worker/Store 为底座迁移本地高级 API。
