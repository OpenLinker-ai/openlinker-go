# Registration 示例

[English](README.md)

Registration 示例会读取或修改当前用户拥有的 Agent 资源。所有命令都在 `example/` 目录执行。

| 目录 | 默认行为 | 是否修改平台资源 |
|---|---|---:|
| `ensure-agent` | 创建或复用指定 Agent，并确保存在 Agent Token | 可能 |
| `token-management` | 只列出指定 Agent 的 Token | 否 |

## 创建或复用 Agent

```bash
export OPENLINKER_API_BASE=https://api.openlinker.ai
export OPENLINKER_USER_TOKEN=ol_user_xxx
export OPENLINKER_AGENT_SLUG=my-runtime-agent
export OPENLINKER_AGENT_NAME='My Runtime Agent'
export OPENLINKER_AGENT_DESCRIPTION='Created by the Go SDK example' # 可选
export OPENLINKER_AGENT_TAGS='agent,runtime,demo'                   # 可选

go run ./registration/ensure-agent
```

第一次运行时，`EnsureAgent` 会使用 User Token 创建短期 `pending_registration` Agent Token，再通过公开 Agent 注册端点原子创建 Agent，并把同一枚 Token 激活为可供 Runtime 使用的 Agent Token。它不会使用 User Token 调用只接受网页登录 JWT 的 Agent 管理路由。注册结果默认保存在：

```text
.openlinker/registration.env
```

可以通过 `OPENLINKER_REGISTRATION_STATE_PATH` 修改路径。文件权限为 `0600`，保存 Agent ID 和 Agent Token，已被示例目录的 `.gitignore` 排除。后续使用同一状态文件运行时会直接复用，不重复创建平台资源，也不再要求 `OPENLINKER_USER_TOKEN`。

示例输出不会打印明文 Agent Token，只显示 Token ID 和 prefix。

## Token 管理

默认操作是只读列表：

```bash
export OPENLINKER_API_BASE=https://api.openlinker.ai
export OPENLINKER_USER_TOKEN=ol_user_xxx
export OPENLINKER_AGENT_ID=22222222-2222-4222-8222-222222222222

go run ./registration/token-management
```

创建 Token 必须同时显式指定 action 和写入确认：

```bash
go run ./registration/token-management \
  --action=create \
  --confirm-write \
  --name='local runtime token' \
  --scopes='agent:pull,agent:call'
```

创建响应中的 `plaintext_token` 通常只显示一次，应立即保存到安全位置，不要写入源码、日志或提交到 Git。

撤销 Token 同样需要双重显式参数：

```bash
go run ./registration/token-management \
  --action=revoke \
  --confirm-write \
  --token-id=33333333-3333-4333-8333-333333333333
```

缺少 `--confirm-write` 时，create/revoke 会在发送任何 HTTP 请求前失败。

## 离线测试

`ensure-agent` 测试验证创建后本地复用、状态文件权限和敏感输出；`token-management` 测试验证默认只读、写操作保护、请求 body 和 revoke 路径。测试使用 `httptest`，不连接真实平台。
