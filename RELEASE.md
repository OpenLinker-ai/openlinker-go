# Release Process

This repository is released from `main` after CI and local release gates pass.

Before tagging a release:

1. Confirm `README.md`, `CHANGELOG.md`, `SECURITY.md`, contracts, and examples
   are current.
2. Run `go test ./...`.
3. Run a current-source secret scan with `gitleaks dir --redact .`.
4. Confirm generated artifacts, `.env` files, coverage, and local binaries are
   not tracked.

Use semantic version tags for public SDK releases. Keep contract changes and
compatibility notes in `CHANGELOG.md`.
