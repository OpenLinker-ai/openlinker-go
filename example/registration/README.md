# Registration examples

[简体中文](README.zh-CN.md)

These examples read or change Agents owned by the current user. Run commands
from `example/`.

| Directory | Default behavior | May change platform state |
| --- | --- | ---: |
| `ensure-agent` | Create or reuse an Agent and ensure it has an Agent Token | Yes |
| `token-management` | List Tokens for one Agent | No |

## Create or reuse an Agent

```bash
export OPENLINKER_API_BASE=https://api.openlinker.ai
export OPENLINKER_USER_TOKEN=ol_user_xxx
export OPENLINKER_AGENT_SLUG=my-runtime-agent
export OPENLINKER_AGENT_NAME='My Runtime Agent'
export OPENLINKER_AGENT_DESCRIPTION='Created by the Go SDK example'
export OPENLINKER_AGENT_TAGS='agent,runtime,demo'

go run ./registration/ensure-agent
```

On the first run, `EnsureAgent` creates a short-lived
`pending_registration` Agent Token with the User Token. It then uses the
public Agent registration endpoint to create the Agent atomically and activate
that same credential as an Agent Token that Runtime can use.

Registration is saved by default to:

```text
.openlinker/registration.env
```

Change the path with `OPENLINKER_REGISTRATION_STATE_PATH`. The file is mode
`0600`, is ignored by the example module, and contains the Agent ID and Agent
Token. A later run reuses it and no longer needs the User Token.

Example output never includes the plaintext Agent Token.

## Manage Tokens

Listing is the safe default:

```bash
export OPENLINKER_API_BASE=https://api.openlinker.ai
export OPENLINKER_USER_TOKEN=ol_user_xxx
export OPENLINKER_AGENT_ID=22222222-2222-4222-8222-222222222222
go run ./registration/token-management
```

Creating or revoking a Token requires both the action and `--confirm-write`:

```bash
go run ./registration/token-management \
  --action=create \
  --confirm-write \
  --name='local runtime token' \
  --scopes='agent:pull,agent:call'

go run ./registration/token-management \
  --action=revoke \
  --confirm-write \
  --token-id=33333333-3333-4333-8333-333333333333
```

A plaintext Token is normally shown only once. Save it immediately in a
secret store; never put it in source, logs, or Git.

Offline tests check local reuse, private file permissions, redacted output,
read-only defaults, write confirmation, request bodies, and revoke paths.
