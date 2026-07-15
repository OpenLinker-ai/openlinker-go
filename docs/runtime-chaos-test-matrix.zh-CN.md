# Runtime v2 Chaos、契约与回归测试矩阵

**状态：** 已完成

**对应计划：** `runtime-worker-api-modes-plan.md` Action Item 9

**完成日期：** 2026-07-15

本文记录 Runtime v2 SDK 的可靠性测试边界、故障注入方式和 CI 验证命令。普通单元测试不依赖正在运行的 OpenLinker Core；与 Core 的跨仓库兼容性通过可选命令单独验证。

## 1. Chaos 故障模型

`runtime_worker_chaos_test.go` 提供测试专用的 `chaosRuntimeClient`，可以在下列协议操作的请求前或响应后注入错误：

- Session：create、heartbeat、close。
- Assignment：claim、ACK、reject。
- Attempt：lease renew。
- Spool：Event append、Result finalize。
- 恢复与命令：resume、commands poll、cancel ACK。
- 委派：Agent-to-Agent call。

两种注入时机代表不同故障窗口：

- `before`：Core 尚未处理请求，客户端可以安全重试。
- `after`：Core 已处理请求，但响应或业务 ACK 丢失；客户端必须使用原有 identity、EventID、ResultID 和 sequence 重放，不能重复执行 handler。

`TestChaosRuntimeClientSupportsEveryProtocolOperation` 会检查所有注入点，防止后续增加协议方法时出现未被 chaos 层覆盖的路径。

## 2. 回归测试矩阵

| 可靠性边界 | 主要测试 | 核心断言 |
|---|---|---|
| Session create/heartbeat/close | `TestChaosRuntimeClientSupportsEveryProtocolOperation`、`TestRuntimeHTTPHeartbeatRejectsAttachmentRotation`、`TestRuntimeHTTPHeartbeatSharesPullGenerationWhileLifecycleRotationWaits` | 生命周期操作可注入故障；heartbeat 不接受 attachment identity 漂移；连接代际切换不会与 heartbeat 并发破坏状态 |
| WebSocket 重连与 HTTP fallback/promotion | `TestRuntimeTransportAutoConfirmsBeforeExecuteAndSwitchesWSPullWS`、`TestRuntimeTransportAutoFallsBackWhenInitialWebSocketIsUnavailable`、`TestSwitchingRuntimeClientCancelsOldGenerationBeforePublishingNew` | fallback 后先 resume/confirm 再执行；恢复后安全 promotion；旧 transport generation 在新连接发布前被取消 |
| Assignment 落盘后、ACK 前崩溃 | `TestAssignmentJournalWALSyncIsRecoveryPoint`、`TestAssignmentJournalReplaysAfterRestart` | WAL durable point 之后可恢复并重放；journal 状态只能单向推进 |
| Assignment ACK 响应丢失 | `TestRuntimeResumeAfterAssignmentACKResponseLossExecutesOnce`、`TestRuntimeChaosACKLossPreservesStableIDsAndExecutesOnce` | resume 与 Core 决策收敛；handler 只执行一次；重放使用相同 assignment identity |
| ACK 成功后本地 confirmed 保存失败 | `TestRuntimeAssignmentACKThenConfirmedSaveFailureResumesWithoutDuplicateExecution` | 第一次进程不执行未完成本地确认的任务；重启后根据 durable ACK-sent journal resume；最终只执行一次 |
| Event/Result ACK 丢失 | `TestRuntimeChaosACKLossPreservesStableIDsAndExecutesOnce`、`TestRuntimeReliableFlowReplaysStableEventAndResult`、`TestRuntimeResultEventsMissingReplaysRetainedRangeBeforeRetry` | EventID、ResultID 保持稳定；Event sequence 单调；Result final sequence 与 Core 要求的 Event 范围一致 |
| Event/Result durable crash window | `TestEventRenameCrashRecoversDurableRecordAndCounter`、`TestEventACKCrashNeverLosesUnacknowledgedState`、`TestResultRenameCrashPromotesJournalOnRecovery`、`TestResultACKCrashCleansOnlyAfterDurableBusinessACK` | rename、WAL sync、目录 sync 和业务 ACK 各阶段都不会丢失未确认 spool，也不会提前清理 Result |
| Lease 超时与 revoke | `TestRuntimeStaleLeaseCancelsExactAttempt`、`TestRuntimeLeaseRevokeCancelsOnlyTargetAttempt`、`TestRuntimeFinishedAttemptRenewsLeaseUntilSpoolIsAcknowledged` | 只取消目标 attempt；已完成但 Result 未确认时继续维护 lease；过期 attempt 不提交 fenced Result |
| Attempt deadline | `TestRuntimeExpiredAttemptDeadlineDoesNotInvokeAdapter` | 已过期任务不进入业务 handler，并使用可靠 Result 流程报告终态 |
| Cancel 与 cancel deadline | `TestRuntimeCancelACKsStoppedOnlyAfterAdapterExited`、`TestRuntimeCancelDeadlineACKsFailedWithoutWaitingForever` | 正常 cancel 在 handler 退出后 ACK stopped；不响应取消的 handler 不会无限阻塞，deadline 后 ACK failed |
| Drain | `TestRuntimeDrainCommandAdvertisesZeroCapacity` | 收到 drain 后向 Core 宣告零可用容量，不再 claim 新任务 |
| capacity 4 与多 attempt resume | `TestRuntimeCapacityFourResumesFourAttemptsConcurrently`、`TestConcurrentEventAppendsAllocateUniqueSequence` | 四个 durable attempt 可并发恢复；实际并发达到 4；Event sequence 唯一且无数据竞争 |
| Store 损坏、截断与身份丢失 | `TestAssignmentJournalDetectsTruncation`、`TestAssignmentJournalDetectsSnapshotCorruption`、`TestSpoolDetectsTruncatedAndCorruptRecords`、`TestRuntimeStoreFailsClosedWhenKeyOrIdentityIsLost` | 损坏数据明确失败，不静默丢弃；identity 或加密 key 丢失时 fail closed |
| 敏感信息与磁盘限制 | `TestDurableStoreDoesNotPersistSensitivePlaintext`、`TestSpoolBackpressureRecordLimitAndUsageSurviveRestart`、`TestSpoolPreservesControlReserveAndUnackedResultAtDiskLimit` | durable 文件不保存敏感明文；限制跨重启保持；磁盘压力下保留控制空间和未确认 Result |
| WebSocket 多消息 resume | `TestRuntimeWebSocketResumeCollectsEveryCorrelatedDecision`、`TestRuntimeWebSocketResumeRejectsPartialDecisionsOnClose` | 收集全部 correlated resume decision；部分响应后断线不会被误判为完整成功 |
| mTLS/env 配置失败 | `TestRuntimeHTTPClientRejectsMissingFilesKeyMismatchAndInvalidCA`、`TestRuntimeHTTPClientRejectsWrongCAAndServerName` | 一次报告缺失文件；拒绝 key mismatch、非法 CA、错误信任链和错误 server name |
| 显式 HTTP client 优先级 | `TestNativeRunnerExplicitHTTPClientOverridesMTLSFiles` | 显式 `WithHTTPClient` 优先于环境变量中的 mTLS 文件配置 |
| Runtime contract 与公共边界 | `TestRuntimeContractMatchesExportedConstants`、`TestRegistrationContractMapsToImplementedMethods`、`TestRuntimeProtocolSourcesDoNotDependOnManagedWorker`、`TestRuntimePublicSurfaceHasNoGenerationName` | contract digest/feature 与 Core 定义一致；registration contract 有实现映射；底层协议不依赖 Managed Worker；公共 API 不暴露 transport generation 细节 |

## 3. Stable ID 与 sequence 验收

关键不可逆窗口统一验证以下性质：

1. Assignment ACK、Event ACK 或 Result ACK 丢失后，重放不能生成新的业务 identity。
2. 同一 attempt 的 Event sequence 必须严格单调，进程重启和并发 append 后仍然唯一。
3. ResultID 在首次 durable save 时确定，直至业务 ACK 前保持不变。
4. Result 的 final sequence 必须覆盖已 durable 的 Event；Core 返回缺失范围时先补发 Event，再重试原 Result。
5. resume 只能推进 journal 状态，不能让 confirmed/started attempt 回退或重复调用 handler。

## 4. CI 与本地验证命令

根模块固定执行：

```bash
go test ./...
go test -race ./...
go vet ./...
```

关键时序测试在 CI 中使用 race detector 重复执行 5 次：

```bash
go test -race . \
  -run '^(TestRuntimeChaosACKLossPreservesStableIDsAndExecutesOnce|TestRuntimeCapacityFourResumesFourAttemptsConcurrently|TestRuntimeTransportAutoConfirmsBeforeExecuteAndSwitchesWSPullWS|TestSwitchingRuntimeClientCancelsOldGenerationBeforePublishingNew)$' \
  -count=5
```

当前独立示例模块也执行：

```bash
cd example/runtime/agent-generic
go test ./...
go vet ./...
```

Action Item 10 将示例迁移到共享 `example` module 后，CI 命令会相应收敛为 `cd example && go test ./... && go vet ./...`。

## 5. 可选 Core 跨仓库验证

普通 `go test ./...` 不连接外部服务，也不依赖 Core 仓库。开发机存在相邻的最新 `openlinker-core` 工作树时，可以执行以下定向兼容验证：

```bash
cd ../openlinker-core
go test ./pkg/runtime \
  -run 'TestRuntime(Session|Resume|HTTP|WebSocket)|Test.*RuntimeSession|Test.*RuntimeResume|Test.*EventStore|Test.*Lease|Test.*Cancellation' \
  -count=1
```

该命令验证 Core 的 Session、resume、HTTP/WebSocket、Event Store、lease 与 cancel 语义；它是跨仓库回归补充，不替代 SDK 的离线 chaos 测试。
