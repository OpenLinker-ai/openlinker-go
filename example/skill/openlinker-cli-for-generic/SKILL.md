---
name: openlinker-cli-for-generic
description: "Use this skill when an agent should interact with OpenLinker through the openlinker CLI: discover callable agents, start user-context runs, delegate to child agents from the current runtime run, or inspect run status/events/messages/artifacts. Prefer this skill for OpenLinker A2A delegation, multi-agent handoff, and run trace inspection."
---

# OpenLinker CLI Skill

Use the `openlinker` CLI as the boundary between the agent and OpenLinker Core. Do not handle or print tokens directly. Assume credentials and runtime context are provided by the environment unless the user explicitly supplies flags.

## Environment

Required for most commands:

```bash
OPENLINKER_API_BASE=http://localhost:8080
```

User-context commands use one of:

```bash
OPENLINKER_TOKEN
OPENLINKER_USER_TOKEN
OPENLINKER_DEMO_JWT
```

Runtime delegation uses one of:

```bash
OPENLINKER_RUNTIME_TOKEN
OPENLINKER_AGENT_TOKEN
```

Runtime context may be injected by OpenLinker:

```bash
OPENLINKER_RUN_ID
OPENLINKER_AGENT_ID
OPENLINKER_TRACE_ID
```

Never include real token values in prompts, skill files, logs, or final answers.

## First Checks

Check runtime context without exposing credentials:

```bash
openlinker context
```

If a command needs user auth and fails with auth errors, ask the operator to provide `OPENLINKER_TOKEN` or `OPENLINKER_USER_TOKEN`.

If `delegate` fails because no parent run is available, ask the operator to run inside an OpenLinker runtime assignment or set `OPENLINKER_RUN_ID`.

## Discover Agents

Search callable agents:

```bash
openlinker agents search --query "summary" --callable
```

Fetch an agent:

```bash
openlinker agents get --slug writer-agent
```

Fetch an agent card:

```bash
openlinker agents card --slug writer-agent --extended
```

Use discovery when the user gives a capability but not a concrete target agent id.

## Start A User Run

Use `run` when acting from a user/API context and starting a top-level OpenLinker run:

```bash
openlinker run \
  --agent agent_writer \
  --input '{"task":"write a short summary"}'
```

Plain text input is allowed:

```bash
openlinker run --agent agent_writer --text "write a short summary"
```

## Delegate From Current Run

Use `delegate` when the current agent should call another OpenLinker agent as a child run:

```bash
openlinker delegate \
  --agent agent_reviewer \
  --reason "review the draft" \
  --input '{"task":"review this draft"}'
```

`delegate` defaults to `OPENLINKER_RUN_ID` as `parent_run_id` and `OPENLINKER_TRACE_ID` as `trace_id`.

Override parent run only when the user or runtime explicitly provides it:

```bash
openlinker delegate \
  --agent agent_reviewer \
  --parent-run run_parent \
  --input '{"task":"review this draft"}'
```

Prefer `delegate` over `run` for A2A handoff, multi-agent workflows, or any action that should appear as a child in OpenLinker traces.

## Inspect Runs

Get a run:

```bash
openlinker runs get --id run_xxx
```

Inspect A2A children:

```bash
openlinker runs children --id run_xxx
```

Inspect events, messages, and artifacts:

```bash
openlinker runs events --id run_xxx --limit 50
openlinker runs messages --id run_xxx
openlinker runs artifacts --id run_xxx
```

Use these after delegation to report the child run id, status, and parent-child relationship.

## Output Handling

The CLI writes JSON to stdout. Parse stdout as JSON when making decisions. Treat stderr as diagnostics only.

In final answers, summarize run ids, statuses, and next actions. Do not include tokens.
