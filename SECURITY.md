# Security Policy

Do not open public issues for vulnerabilities.

Use GitHub private vulnerability reporting when available. Otherwise contact
the maintainers through the published OpenLinker security/support channel with
the affected commit, reproducible steps, impact, and whether a live token or
service is involved.

Security-sensitive areas include:

- authorization header handling
- callback signature verification
- runtime WebSocket and pull connectors
- A2A push notification credentials

Never include real third-party secrets in public reports or test cases.

