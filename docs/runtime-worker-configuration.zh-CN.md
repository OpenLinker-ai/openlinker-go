# RuntimeWorker 配置与默认行为

合并远端可靠 Worker 后，极简 Agent、Native facade 和 `NewRuntimeWorker` 最终都使用同一个 `RuntimeWorkerConfig`。配置优先级为：显式 builder → 环境变量 → `.env` 注册状态 → SDK 默认值。

## 极简入口所需环境变量

| 环境变量 | 用途 |
| --- | --- |
| `OPENLINKER_NODE_ID` | 已登记 Node 的 UUID。SDK 不再把随机本地 ID 当成平台 Node 身份。 |
| `OPENLINKER_AGENT_ID` | Agent UUID；注册状态中已有时可以省略。 |
| `OPENLINKER_AGENT_TOKEN` | Agent Runtime Token；注册状态中已有时可以省略。 |
| `OPENLINKER_API_BASE` | 平台地址，用于发现专用 Runtime origin。 |
| `OPENLINKER_RUNTIME_BASE` | 可选的 Runtime origin 显式覆盖。 |
| `OPENLINKER_NODE_CERT_FILE` | Node mTLS 客户端证书。 |
| `OPENLINKER_NODE_KEY_FILE` | Node mTLS 私钥。 |
| `OPENLINKER_RUNTIME_CA_FILE` | Runtime 服务端 CA。 |

可选配置：

| 环境变量 | 默认值 | 用途 |
| --- | --- | --- |
| `OPENLINKER_RUNTIME_TRANSPORT` | `auto` | `auto`、`websocket`/`ws`、`http`/`pull`。 |
| `OPENLINKER_RUNTIME_CAPACITY` | `1` | 最大并发 Attempt 数，范围 1–1024。 |
| `OPENLINKER_RUNTIME_DATA_DIR` | `.openlinker/runtime-<agent-id>` | 加密 Store、identity、journal 和 spool 目录。 |
| `OPENLINKER_RUNTIME_STATE_PATH` | 无 | 旧 facade 的兼容变量；`.json` 路径会转换成同名目录。 |
| `OPENLINKER_RUNTIME_SERVER_NAME` | 空 | 覆盖 TLS server name。 |

配置错误通过 `RuntimeConfigError` 一次报告多个缺失项。

## 显式配置

```go
worker, err := openlinker.NewRuntimeWorker(openlinker.RuntimeWorkerConfig{
    PlatformURL: "https://openlinker.example",
    NodeID:      nodeID,
    AgentID:     agentID,
    AgentToken:  agentToken,
    Transport:   openlinker.RuntimeTransportAuto,
    DataDir:     "/var/lib/my-agent/runtime",
    MTLS: openlinker.RuntimeMTLSConfig{
        CertFile: "/run/openlinker/node.crt",
        KeyFile:  "/run/openlinker/node.key",
        CAFile:   "/run/openlinker/runtime-ca.crt",
    },
    Handler: handler,
})
```

高级 facade 可以覆盖同一配置：

```go
err := openlinker.Native(agent.Handle).
    WithNodeID(nodeID).
    WithAgentID(agentID).
    WithDataDir(dataDir).
    WithTransportMode(openlinker.TransportAuto).
    Run(ctx)
```

## mTLS

`RuntimeWorker` 会使用 `RuntimeMTLSConfig` 创建 TLS 1.3 client，并禁止 Runtime endpoint 重定向。HTTP long-poll 和 WebSocket 共用相同的证书、私钥和 CA。

底层调用方可以使用 `NewRuntimeHTTPClient` 和 `NewRuntimeFromEnv`。`NewRuntimeFromEnv` 要求显式 `OPENLINKER_RUNTIME_BASE`；平台 discovery 由 Managed Worker 负责。

## 默认 RuntimeStore

未注入 `RuntimeStore` 时，SDK 使用 `DataDir` 打开 `FileRuntimeStore`。它负责：

- 稳定 WorkerID、每次进程启动生成新的 RuntimeSessionID，以及持久化递增的 SessionEpoch。
- assignment journal 与 payload。
- 加密的 Event/Result spool。
- WAL、snapshot、checksum、文件锁、磁盘空间和记录数量限制。
- Unix/Windows 的私有权限与原子 durable write。

同一个 DataDir 只能由一个 Worker 进程持有。生产环境可以实现其他 `RuntimeStore`，但所有变更方法都必须在真正 durable 后才返回成功。
