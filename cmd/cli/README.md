# OpenLinker CLI

Small JSON-first CLI for OpenLinker agents, skills, and automation.

The CLI is intentionally thin over the Go SDK. It is designed for agent-facing
skills:

- stdout is always JSON
- diagnostics and errors go to stderr
- tokens are read from flags or environment variables but are never printed
- `delegate` defaults to the current OpenLinker run context from environment

## Configuration

```bash
export OPENLINKER_API_BASE=http://localhost:8080
export OPENLINKER_TOKEN=ol_user_xxx
export OPENLINKER_RUNTIME_TOKEN=ol_runtime_xxx
```

Runtime context, usually injected by the OpenLinker runtime:

```bash
export OPENLINKER_RUN_ID=run_xxx
export OPENLINKER_AGENT_ID=agent_xxx
export OPENLINKER_TRACE_ID=trace_xxx
```

## Commands

Inspect runtime context without exposing credentials:

```bash
openlinker context
```

Discover agents:

```bash
openlinker agents search --query "summarization" --callable
openlinker agents get --slug writer-agent
openlinker agents card --slug writer-agent --extended
```

Run an agent from a user/API context:

```bash
openlinker run \
  --agent agent_writer \
  --input '{"task":"write a short summary"}'
```

Delegate from the current OpenLinker run:

```bash
openlinker delegate \
  --agent agent_reviewer \
  --reason "review the generated summary" \
  --input '{"task":"review this draft"}'
```

The command above uses `OPENLINKER_RUN_ID` as the parent run. Override it with
`--parent-run` when needed.

Inspect run state and A2A delegation traces:

```bash
openlinker runs get --id run_xxx
openlinker runs children --id run_xxx
openlinker runs events --id run_xxx
openlinker runs messages --id run_xxx
openlinker runs artifacts --id run_xxx
```

## Skill Guidance

Skills should call this CLI instead of directly handling OpenLinker tokens. A
skill can decide when to delegate, then run:

```bash
openlinker delegate --agent agent_xxx --reason "..." --input '{"task":"..."}'
```

Do not put tokens in skill files or command examples. Use environment variables,
workload identity, or a credential broker outside of the agent prompt.
