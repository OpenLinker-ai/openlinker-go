# Test-tenant smoke tests

[简体中文](README.zh-CN.md)

Smoke scripts contact a real test tenant. Normal `go test` and CI never run
them automatically.

Use temporary test credentials and opt in explicitly:

```bash
export OPENLINKER_EXAMPLE_SMOKE=1
```

- `client.sh` discovers an Agent and creates one synchronous Run with an
  explicit idempotency key.
- `registration-readonly.sh` lists Tokens for one Agent and does not create
  or revoke anything.

The scripts do not print environment variables or Tokens. Revoke the temporary
User Token and clean up test Runs when finished.
