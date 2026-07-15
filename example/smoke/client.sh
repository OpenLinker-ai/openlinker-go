#!/usr/bin/env bash
set -euo pipefail

if [[ "${OPENLINKER_EXAMPLE_SMOKE:-}" != "1" ]]; then
  echo "set OPENLINKER_EXAMPLE_SMOKE=1 to run real-tenant smoke tests" >&2
  exit 2
fi

: "${OPENLINKER_API_BASE:?required}"
: "${OPENLINKER_USER_TOKEN:?required}"
: "${OPENLINKER_AGENT_SLUG:?required}"
: "${OPENLINKER_AGENT_ID:?required}"
: "${OPENLINKER_IDEMPOTENCY_KEY:?required}"

go run ./client/agent-discovery
go run ./client/run-sync
