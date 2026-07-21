# Managed RuntimeWorker 示例

[English](README.md)

本目录演示高级调用方如何直接使用 `NewRuntimeWorker`，自定义 Store、capacity、logger 和
handler。

第一次运行前必须先准备平台 Agent、Agent Token、稳定的 Node/Agent ID 和持久化目录；
Runtime 安全策略由平台发现决定。不要只复制 `main.go` 然后猜测身份来源。

完整操作顺序见：

[从零运行一个 RuntimeWorker：完整操作手册](../../../runtime-worker-end-to-end.zh-CN.md)

准备完成后，在 `example/` module 根目录运行：

```bash
export OPENLINKER_API_BASE='https://openlinker.example'
export OPENLINKER_NODE_ID='11111111-1111-4111-8111-111111111111'
export OPENLINKER_AGENT_ID='22222222-2222-4222-8222-222222222222'
export OPENLINKER_AGENT_TOKEN='<从安全位置读取>'
export OPENLINKER_RUNTIME_DATA_DIR='/var/lib/my-agent/runtime'
export OPENLINKER_RUNTIME_TRANSPORT='auto'

go run ./runtime/worker-managed
```

token-only Runtime 不要配置证书文件。发现明确要求 mTLS 时，使用 SDK 自动登记，或完整提供
外部 PKI 配置组。

示例把 capacity 覆盖为 4，因此 Runtime Node 登记 capacity 必须足够。普通单并发 Worker 应
改成 1，或者使用 SDK 默认值。

日志出现 Runtime Ready 后，还必须创建真实 Run、检查 Result、执行 cancel 并重启进程验证，
才能确认部署可用。
