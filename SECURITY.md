# Security Policy

Chinese documentation: [SECURITY.zh-CN.md](./SECURITY.zh-CN.md)

Do not open public issues for vulnerabilities.

Use GitHub private vulnerability reporting when available. If it is not
available, contact the maintainers through the published OpenLinker
security/support channel. Include the affected repository, commit or release,
reproduction steps, impact, and whether any live token, public endpoint, or
customer data is involved.

## Supported Versions

`openlinker-go` is pre-1.0. Security fixes target the current `main` branch and
the latest tagged release when tags are available. Older commits may not receive
backports unless maintainers explicitly announce support for a release line.

## Security-Sensitive Areas

- authorization header handling
- callback signature creation and verification
- webhook raw-body handling
- runtime WebSocket and pull connectors
- A2A push notification credentials
- gRPC metadata and tenant routing
- accidental token exposure in examples or errors

## Reporting Guidance

Please include:

- the affected SDK version or commit
- a minimal reproduction
- whether the issue is client-side, runtime connector, callback, A2A, or gRPC
- expected vs. actual behavior
- whether any live secret was exposed

Never include real third-party secrets in public reports, tests, screenshots, or
logs. If a token was exposed, rotate it before sharing details.

## Disclosure

Maintainers will triage reports as quickly as practical. Please avoid public
disclosure until a fix, mitigation, or coordinated disclosure timeline is
available.
