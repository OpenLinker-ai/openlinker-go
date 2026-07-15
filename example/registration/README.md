# Registration 示例

本分类将提供：

- `ensure-agent`：显式创建或复用 Agent，并保存本地注册状态。
- `token-management`：列出、创建和撤销 Agent Token。

注册和 Token 变更会修改平台资源，因此示例必须要求显式 CLI flag；默认行为保持只读。
