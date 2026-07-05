# Release Process

Chinese documentation: [RELEASE.zh-CN.md](./RELEASE.zh-CN.md)

`openlinker-go` releases are cut from `main` after CI and local release gates
pass. Public SDK releases should use semantic version tags.

## Pre-Release Checklist

1. Confirm `README.md`, `CONTRIBUTING.md`, `SECURITY.md`, `SUPPORT.md`,
   contracts, protobuf files, and examples are current.
2. Confirm `CHANGELOG.md` describes public API changes, compatibility notes,
   and any Core version assumptions.
3. Run `gofmt -w .`.
4. Run `go test ./...`.
5. Run a current-source secret scan on a clean checkout, for example
   `gitleaks dir --redact .`.
6. Confirm generated artifacts are intentional and aligned with their sources.
7. Confirm `.env` files, coverage output, local binaries, and private logs are
   not tracked.

## Tagging

Use semantic version tags for public Go module releases:

```bash
git tag v0.x.y
git push origin v0.x.y
```

Pre-1.0 releases may include breaking changes, but they must be called out in
`CHANGELOG.md`.
