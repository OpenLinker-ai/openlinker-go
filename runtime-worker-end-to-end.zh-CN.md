# 从零运行一个 RuntimeWorker：完整操作手册

本文是 `openlinker-go` 中运行 Managed `RuntimeWorker` 的端到端操作手册，覆盖平台资源、
Runtime Node、mTLS、Agent 身份、SDK 代码、持久化、真实调用、cancel、重启和 K8s 部署。

只看到 `Runtime Ready` 不能说明 Agent 已经可用。完整验收分为三层：

```text
连接验收：Runtime Ready
    ↓
执行验收：真实 Run success，Event/Result 被 Core 接收
    ↓
可靠性验收：运行中 cancel + 进程或 Pod 重启恢复
```

三层全部通过，才能认为 RuntimeWorker 可用。

## 1. 先理解三种身份

| 身份 | 典型配置 | 用途 | 谁创建 |
| --- | --- | --- | --- |
| 用户身份 | User Token 或登录 JWT | 创建 Run、查看结果、管理 Agent | 用户或管理员 |
| Agent 身份 | `AgentID` + `AgentToken` | Worker 代表哪个 Agent 接收任务 | Agent 注册或管理流程 |
| Runtime Node 身份 | `NodeID` + mTLS cert/key | 哪个受信任计算节点连接 Runtime | Core 运维人员 |

不要混用：

- User Token 不能作为 Runtime 长期凭据。
- Agent Token 不能代替 Runtime Node mTLS 证书。
- 自动注册只创建或复用 Agent 与 Agent Token，不创建 Runtime Node 和证书。
- `NodeID` 不是 Pod ID、主机名或 SDK 生成的 WorkerID。

## 2. 谁应该直接使用 RuntimeWorker

普通 Go Agent 推荐：

```go
openlinker.WithAgent(agent).Run(ctx)
```

需要完整 Assignment 上下文的框架推荐：

```go
openlinker.Native(handler).Run(ctx)
```

只有需要自定义 Store、capacity、logger、daemon 外壳或生命周期集成时，才直接使用：

```go
openlinker.NewRuntimeWorker(config)
```

三种入口最终使用同一套 RuntimeWorker 状态机。本文使用 `NewRuntimeWorker` 展示全部配置，
高层 facade 的平台准备步骤完全相同。

## 3. 完整流程

```text
1. 确认 Core ready
2. 创建或确认 Agent
3. 创建 active_runtime Agent Token
4. 运维签发 Runtime Node 与 mTLS 证书
5. 分发 Node ID、Agent ID、Token 和证书
6. 准备私有、持久化的 Runtime data 目录
7. 实现 RuntimeHandler
8. 创建并运行 RuntimeWorker
9. 验证 Runtime Ready
10. 使用用户身份创建真实 Run
11. 验证 Event、Result 和最终状态
12. 验证运行中 cancel
13. 重启进程或删除 Pod，验证 identity/spool 恢复
```

## 4. 确认 Core 可用

```bash
export OPENLINKER_API_BASE='https://openlinker.example'
curl -fsS "$OPENLINKER_API_BASE/readyz"
```

至少确认：

```json
{
  "ready": true,
  "mode": "normal"
}
```

如果返回 `hard_maintenance`、`member_not_ready` 或 HTTP 503，应先恢复 Core。Worker 不能绕过
Core 的 Runtime cluster 保护。Runtime listener 能建立 TLS，也不代表 Core 已允许创建 Session。

## 5. 准备 Agent 身份

### 5.1 生产环境或已有 Agent

```text
OPENLINKER_AGENT_ID=<已有 Agent UUID>
OPENLINKER_AGENT_TOKEN=<active_runtime Agent Token>
```

生产 Pod 应关闭自动注册，不携带 User Token。Pod 删除、扩缩容和升级不应该创建新 Agent。

如果旧环境变量名为 `OPENLINKER_RUNTIME_TOKEN`，迁移时应改成标准名称：

```text
OPENLINKER_AGENT_TOKEN
```

### 5.2 本地开发或首次 Demo

自动创建 Agent 时使用高层显式注册入口：

```go
err := openlinker.WithAgent(agent).
    RunOrRegister(ctx, openlinker.AgentSpec{
        Slug:       "my-agent",
        Name:       "My Agent",
        Visibility: "private",
    })
```

首次运行需要 User Token。注册完成后保存 `AgentID` 和 `AgentToken`，后续启动不再使用 User
Token。直接 `NewRuntimeWorker` 不会隐式创建 Agent。

## 6. 签发 Runtime Node 和 mTLS

这一步由 Core 运维人员执行，不由 Agent Pod 自己执行。

Core 运维命令示例：

```bash
/app/api runtime-node issue \
  --ca-cert /secure/runtime-client-ca.crt \
  --ca-key /secure/runtime-client-ca.key \
  --display-name my-runtime-node \
  --node-version openlinker-go/runtime-worker \
  --capacity 1 \
  --valid-for 8760h \
  --cert-out /secure/runtime-node.crt \
  --key-out /secure/runtime-node.key
```

该命令会登记 Runtime Node、生成 `NodeID`，并签发带严格 Runtime Node URI SAN 的客户端
证书。Worker 还需要 Runtime server CA：

```text
runtime-server-ca.crt
```

Runtime Client CA 私钥只能保存在 Core 运维环境，不能分发给 Agent Pod。

### 6.1 NodeVersion 必须完全一致

直接使用 `NewRuntimeWorker` 且没有显式设置 `RuntimeWorkerConfig.NodeVersion` 时，SDK 上报：

```text
openlinker-go/runtime-worker
```

Node 登记也必须使用同一个值。如果代码设置：

```go
config.NodeVersion = "my-runtime/1.0"
```

签发时必须使用：

```text
--node-version my-runtime/1.0
```

不一致时 Core 会拒绝连接：

```text
RUNTIME_CLIENT_UPGRADE_REQUIRED
```

`NodeVersion` 不是镜像 tag，除非程序确实使用同一个字符串上报。

### 6.2 Node capacity 与 Worker capacity

Node 登记 capacity 是整个 Node 的并发上限；`RuntimeWorkerConfig.Capacity` 是单个 Worker
Session 的并发 Attempt 上限。例如一个 Node 供三个单并发 Pod 使用：

```text
Node capacity = 3
每个 Worker capacity = 1
```

### 6.3 检查证书

```bash
openssl x509 -in runtime-node.crt -noout \
  -subject -issuer -dates -serial -fingerprint -sha256

openssl x509 -in runtime-node.crt -noout -text \
  | sed -n '/Subject Alternative Name/,+2p'
```

URI SAN 中的 Node ID 必须与 `OPENLINKER_NODE_ID` 一致。

检查 cert/key：

```bash
openssl x509 -in runtime-node.crt -pubkey -noout | openssl sha256
openssl pkey -in runtime-node.key -pubout | openssl sha256
```

两行摘要必须相同。

## 7. 准备运行配置

```bash
export OPENLINKER_API_BASE='https://openlinker.example'
export OPENLINKER_RUNTIME_BASE='https://runtime.openlinker.example'

export OPENLINKER_NODE_ID='11111111-1111-4111-8111-111111111111'
export OPENLINKER_AGENT_ID='22222222-2222-4222-8222-222222222222'
export OPENLINKER_AGENT_TOKEN='<从 Secret 或受保护文件读取>'

export OPENLINKER_NODE_CERT_FILE='/run/openlinker/runtime-node.crt'
export OPENLINKER_NODE_KEY_FILE='/run/openlinker/runtime-node.key'
export OPENLINKER_RUNTIME_CA_FILE='/run/openlinker/runtime-server-ca.crt'

export OPENLINKER_RUNTIME_TRANSPORT='auto'
export OPENLINKER_RUNTIME_CAPACITY='1'
export OPENLINKER_RUNTIME_DATA_DIR='/var/lib/my-agent/runtime'
```

`OPENLINKER_RUNTIME_BASE` 可以省略，由 Worker 通过 `OPENLINKER_API_BASE` 发现；生产环境显式
设置通常更易排查。只有 Runtime URL 主机名与证书 SAN 需要覆盖时才设置：

```bash
export OPENLINKER_RUNTIME_SERVER_NAME='runtime.openlinker.example'
```

不要把 Token 写入镜像、Git、普通 ConfigMap、命令历史或日志。

## 8. 准备持久化目录

默认 `FileRuntimeStore` 在 `DataDir` 保存：

- 稳定 WorkerID 和递增 SessionEpoch。
- assignment journal 和 payload。
- 加密 Event/Result spool。
- resume 状态和 spool 加密 key。

```bash
install -d -m 700 /var/lib/my-agent/runtime
chmod 600 /run/openlinker/runtime-node.key
```

要求：

```text
Runtime data 目录：0700
Runtime identity、journal、spool key：0600
同一个 DataDir 同时只能由一个 Worker 进程使用
```

生产 `DataDir` 不能放在容器临时层或 `emptyDir`。普通重启不能清空 `DataDir`。只有明确切换
Node 身份且确认没有待 resume Run 时，才允许按运维流程迁移或清理旧 identity。

## 9. 实现 RuntimeHandler

```go
package main

import (
    "context"
    "errors"
    "log"
    "os"
    "os/signal"
    "syscall"

    openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func main() {
    ctx, stop := signal.NotifyContext(
        context.Background(), os.Interrupt, syscall.SIGTERM,
    )
    defer stop()

    config, err := openlinker.LoadRuntimeWorkerConfig()
    if err != nil {
        log.Fatal(err)
    }
    config.Handler = openlinker.RuntimeHandlerFunc(handleRun)
    config.OnReady = func(ready openlinker.RuntimeReadyPayload) {
        log.Printf("runtime ready: core=%s attachment=%s features=%d",
            ready.CoreInstanceID, ready.AttachmentID, len(ready.Features))
    }

    worker, err := openlinker.NewRuntimeWorker(config)
    if err != nil {
        log.Fatal(err)
    }
    if err := worker.Run(ctx); err != nil && !errors.Is(err, context.Canceled) {
        log.Fatal(err)
    }
}

func handleRun(
    ctx context.Context,
    run openlinker.RuntimeContext,
) (openlinker.RuntimeResult, error) {
    if err := run.Emit("run.message.delta", map[string]any{
        "text": "开始处理",
    }); err != nil {
        return openlinker.RuntimeResult{}, err
    }

    select {
    case <-ctx.Done():
        return openlinker.RuntimeResult{}, ctx.Err()
    default:
    }

    return openlinker.RuntimeResult{
        Status: "success",
        Output: map[string]any{
            "input": run.Input,
            "text":  "处理完成",
        },
    }, nil
}
```

重要行为：

- Handler 只收到 Core 已确认的 Assignment。
- cancel 和 deadline 通过 handler `ctx` 传递。
- `run.Emit` 先 durable journal，再上传。
- Handler 返回后 Result 进入 spool，直到 Core ACK。
- `RuntimeWorker` 是单次使用对象；`Run` 返回后必须重新构造。

## 10. 启动 Worker

```bash
go run ./cmd/my-runtime-worker
```

或：

```bash
CGO_ENABLED=0 go build -trimpath -o bin/my-runtime-worker ./cmd/my-runtime-worker
./bin/my-runtime-worker
```

`OnReady` 只说明 mTLS、Agent Token、Node/Agent/contract 和 Runtime Session 成功。它不能证明
用户能创建 Run、业务模型和工具可用、Result 已 ACK、cancel 和重启恢复可用。

## 11. 创建真实 Run

使用用户侧 JWT 或 User Token，不使用 Agent Token：

```bash
export OPENLINKER_USER_TOKEN='<从安全位置读取>'
export IDEMPOTENCY_KEY="runtime-smoke-$(date +%s)"

curl -fsS -X POST \
  "$OPENLINKER_API_BASE/api/v1/runs" \
  -H "Authorization: Bearer $OPENLINKER_USER_TOKEN" \
  -H 'Content-Type: application/json' \
  -H "Idempotency-Key: $IDEMPOTENCY_KEY" \
  -H 'Prefer: wait=0' \
  --data "{
    \"agent_id\": \"$OPENLINKER_AGENT_ID\",
    \"input\": {\"text\": \"只回复 runtime worker smoke success\"},
    \"metadata\": {\"source\": \"runtime-worker-smoke\"}
  }"
```

保存 `run_id` 后查询：

```bash
export RUN_ID='<返回的 run_id>'

curl -fsS \
  -H "Authorization: Bearer $OPENLINKER_USER_TOKEN" \
  "$OPENLINKER_API_BASE/api/v1/runs/$RUN_ID"
```

执行验收必须确认：

- Run 最终 `success`。
- Handler 确实被调用。
- Event sequence 连续。
- Result 被 Core 接收。
- `DataDir` 没有无限积压的 Event/Result spool。
- 输出符合预期。

如果 Web 页面报错，而 Core 没有 `POST /api/v1/runs`，问题在浏览器或前端，不在 Worker。

## 12. 验证运行中 cancel

准备一个持续运行的 handler 分支：

```go
select {
case <-time.After(60 * time.Second):
    return openlinker.RuntimeResult{
        Status: "success",
        Output: map[string]any{"text": "finished"},
    }, nil
case <-ctx.Done():
    return openlinker.RuntimeResult{}, ctx.Err()
}
```

Run 运行期间调用：

```bash
curl -fsS -X POST \
  -H "Authorization: Bearer $OPENLINKER_USER_TOKEN" \
  "$OPENLINKER_API_BASE/api/v1/runs/$RUN_ID/cancel"
```

确认 handler 的 `ctx.Done()` 被触发、Run 进入 canceled 语义、Worker 还能处理下一条 Run。外部
进程应使用 `exec.CommandContext`，网络请求应绑定同一个 context。

## 13. 验证重启和 resume

基础重启测试：

1. 记录 WorkerID 和 SessionEpoch。
2. 发送 `SIGTERM` 或删除 Pod。
3. 使用相同 Agent ID、Node ID 和 `DataDir` 启动。
4. 确认 WorkerID 不变、SessionEpoch 递增。
5. 再创建真实 Run。

可靠性测试：

1. 在 Event 或 Result 尚未 ACK 时中断进程。
2. 使用相同 `DataDir` 重启。
3. 确认 SDK resume assignment，并重放未 ACK Event/Result。
4. 确认没有重复执行业务副作用。

SDK durable spool 不能替业务数据库自动去重；副作用应使用 RunID、AttemptID 或业务
idempotency key 保护。

## 14. K8s 生产部署

### 14.1 ConfigMap 与 Secret

ConfigMap 放非敏感配置：

```text
OPENLINKER_API_BASE
OPENLINKER_RUNTIME_BASE
OPENLINKER_NODE_ID
OPENLINKER_AGENT_ID
OPENLINKER_RUNTIME_TRANSPORT
OPENLINKER_RUNTIME_CAPACITY
OPENLINKER_RUNTIME_DATA_DIR
证书文件路径
```

Secret 放：

```text
OPENLINKER_AGENT_TOKEN
runtime-node.crt
runtime-node.key
runtime-server-ca.crt
```

User Token 不进入生产 Worker Pod。

### 14.2 副本和 PVC

```yaml
spec:
  replicas: 1
  strategy:
    type: Recreate
  revisionHistoryLimit: 10
```

默认不要让两个 Pod 同时使用同一 Agent identity 和同一 RWO `DataDir`。

```yaml
volumeMounts:
  - name: runtime-data
    mountPath: /var/lib/my-agent

env:
  - name: OPENLINKER_RUNTIME_DATA_DIR
    value: /var/lib/my-agent/runtime
```

### 14.3 非 root 与文件权限

```yaml
securityContext:
  runAsNonRoot: true
  runAsUser: 65532
  runAsGroup: 65532
  fsGroup: 65532
  fsGroupChangePolicy: OnRootMismatch
```

默认 `fsGroupChangePolicy: Always` 可能在 PVC 重挂载时增加 group 权限，导致 SDK 拒绝读取
不再私有的 identity、journal 或 spool key：

```text
runtime identity is corrupt
runtime spool key is invalid
```

initContainer 应恢复权限：

```sh
mkdir -p /var/lib/my-agent/runtime
find /var/lib/my-agent/runtime -type d -exec chmod 0700 {} +
find /var/lib/my-agent/runtime -type f -exec chmod 0600 {} +
```

这只恢复权限，不删除 identity 或 spool。

### 14.4 mTLS Secret

```yaml
volumeMounts:
  - name: runtime-mtls
    mountPath: /run/openlinker
    readOnly: true

volumes:
  - name: runtime-mtls
    secret:
      secretName: my-runtime-mtls
      defaultMode: 0440
```

### 14.5 优雅关闭

```yaml
terminationGracePeriodSeconds: 90
```

应用必须把 `SIGTERM` 传给 context。Worker 会停止接收新任务并 drain active Attempt。

## 15. 常见错误

| 错误或现象 | 主要原因 | 处理方式 |
| --- | --- | --- |
| `RUNTIME_CLIENT_UPGRADE_REQUIRED` | NodeVersion、protocol 或 contract 不一致 | 先核对登记 NodeVersion 与上报值 |
| `runtime identity is corrupt` | 内容损坏、权限不是 `0600`，或错误复用 Node identity | 先检查权限，不要直接删除 DataDir |
| `runtime spool key is invalid` | spool key 长度或权限被改变 | 检查 PVC、`fsGroup` 和文件模式 |
| mTLS handshake 失败 | cert/key、CA 或 SAN 不匹配 | 使用 OpenSSL 检查证书链和公钥 |
| HTTP 401/403 | Agent Token 无效、撤销或 scope 不足 | 检查 active_runtime Token |
| Runtime 503 | Core 维护或 member 未 ready | 检查 Core `/readyz` |
| WebSocket 不 fallback | 实际是认证、mTLS 或 contract 永久错误 | 修复配置，不要强制 fallback |
| Ready 但调用无结果 | Run 未创建、Handler 依赖失败或 Result 未 ACK | 分别检查 Core POST、Handler 和 spool |
| 页面网络错误且 Core 无 POST | 浏览器、前端 API、CORS 或 fetch 前 JS 错误 | 不要继续修改 Worker |
| 同一 DataDir 启动两个进程失败 | Store 文件锁生效 | 每个 Worker 使用独占 DataDir |

## 16. 最终验收清单

### 平台和身份

- [ ] Core `/readyz` 为 `ready=true`、`mode=normal`。
- [ ] Agent ID 正确，Agent Token 为 `active_runtime`。
- [ ] Node ID 已登记且未 revoked。
- [ ] NodeVersion 与 Worker 上报值完全一致。
- [ ] Node capacity 足够。

### mTLS 和持久化

- [ ] Node cert/key 匹配，server CA 正确。
- [ ] Runtime URL 主机名或 IP 在 server cert SAN 中。
- [ ] `DataDir` 持久化且独占。
- [ ] 目录 `0700`、私有文件 `0600`。
- [ ] K8s 设置 `fsGroupChangePolicy: OnRootMismatch`。

### 连接验收

- [ ] Worker Runtime Ready。
- [ ] 使用预期 transport。
- [ ] 没有认证、mTLS、contract 或 feature mismatch。

### 执行验收

- [ ] 用户侧成功创建真实 Run。
- [ ] Handler 收到 Assignment。
- [ ] Event sequence 连续，Result 被 Core ACK。
- [ ] Run 最终 `success` 且输出正确。

### 可靠性验收

- [ ] 运行中 cancel 能终止 handler。
- [ ] cancel 后 Worker 能处理下一条 Run。
- [ ] SIGTERM 能优雅关闭。
- [ ] 删除 Pod 后 WorkerID 稳定、SessionEpoch 递增。
- [ ] 未 ACK Event/Result 能在重启后恢复。
- [ ] 没有重复执行业务副作用。

只有以上检查全部完成，才应把 RuntimeWorker 标记为生产可用。

## 17. 相关文档和示例

- [RuntimeWorker 配置与默认行为](./runtime-worker-configuration.zh-CN.md)
- [RuntimeWorker 生命周期](./runtime-worker-lifecycle.zh-CN.md)
- [RuntimeWorker supervisor](./runtime-worker-supervisor.zh-CN.md)
- [TransportAuto](./runtime-worker-transport-auto.zh-CN.md)
- [Runtime API 分层](./runtime-api-modes.zh-CN.md)
- [Managed RuntimeWorker 示例](../example/runtime/worker-managed)
