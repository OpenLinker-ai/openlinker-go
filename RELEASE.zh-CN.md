# 发布流程

English documentation: [RELEASE.md](./RELEASE.md)

`openlinker-go` 从 `main` 发布，前提是 CI 和本地发布检查都通过。公共 SDK 版本应使用
语义化版本 tag。

`v0.2.0-rc.1` 的专项门槛、Core/layout 验证和回滚步骤见
[docs/v0.2.0-rc.1-release-checklist.zh-CN.md](./docs/v0.2.0-rc.1-release-checklist.zh-CN.md)。

## 发布前检查

1. 确认 `README.md` 与 `README.zh-CN.md` 对 Core/Hosted、Client/Runtime 和 Agent Node
   Adapter 边界的描述一致，并确认 `CONTRIBUTING`、`SECURITY`、`SUPPORT`、contracts、
   protobuf 文件和示例是最新的。
2. 确认 `CHANGELOG.md` 描述了公共 API 变化、兼容性说明和 Core 版本假设。
3. 运行 `gofmt -w .`。
4. 运行 `go test ./...`。
5. 在干净 checkout 上运行源码 secret scan，例如 `gitleaks dir --redact .`。
6. 确认生成产物是有意提交且与源文件一致。
7. 确认 `.env`、覆盖率输出、本地二进制和私有日志没有被跟踪。

## 打 tag

公共 Go module release 使用语义化版本 tag：

```bash
git tag v0.x.y
git push origin v0.x.y
```

pre-1.0 版本可以包含 breaking change，但必须在 `CHANGELOG.md` 中说明。

发布前必须确认没有真实 user token、agent token、callback secret 或私有服务地址进入示例、
测试或文档。
