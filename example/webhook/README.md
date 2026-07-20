# Webhook example

[简体中文](README.zh-CN.md)

`verify-request` starts a small HTTP server that verifies the
`X-OpenLinker-Signature` HMAC-SHA256 signature against the raw request body.

```bash
export OPENLINKER_CALLBACK_SECRET=replace-with-the-secret-used-when-creating-the-run
export OPENLINKER_WEBHOOK_ADDR=:8080
go run ./webhook/verify-request
```

Signature verification must happen before JSON decoding because the signature
binds the original bytes. After verification, the SDK restores
`request.Body` so the application handler can decode it normally.

The example limits the body to 1 MiB. A missing or invalid signature receives
`401`, and the payload is not processed.
