# 测试租户 Smoke Test

[English](README.md)

Smoke scripts 会连接真实测试租户，永远不会被普通 `go test` 或 CI 自动执行。

运行前使用临时测试凭证配置对应环境变量，并显式确认：

```bash
export OPENLINKER_EXAMPLE_SMOKE=1
```

- `client.sh`：执行 Agent discovery 和一次带显式 idempotency key 的同步 Run。
- `registration-readonly.sh`：只列出指定 Agent 的 Token，不创建或撤销资源。

脚本不会输出环境变量或主动打印 Token。运行结束后应撤销临时 User Token，并清理测试 Run。
