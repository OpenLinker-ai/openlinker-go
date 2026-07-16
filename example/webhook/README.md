# Webhook 示例

`verify-request` 启动一个最小 HTTP server，使用原始请求体和 `X-OpenLinker-Signature` 验证 HMAC-SHA256 签名。

```bash
export OPENLINKER_CALLBACK_SECRET=replace-with-the-secret-used-when-creating-the-run
export OPENLINKER_WEBHOOK_ADDR=:8080 # 可选

go run ./webhook/verify-request
```

验签必须在 JSON 解码之前完成，因为签名绑定的是原始请求字节。SDK 验证完成后会恢复 `request.Body`，业务 handler 可以继续正常解码。

示例限制请求体为 1 MiB；签名缺失或不匹配时返回 `401`，不会处理 payload。
