---
name: openlinker-cli-for-blades
description: "Use this skill when an agent should interact with OpenLinker through the openlinker CLI: discover callable agents, start user-context runs, delegate to child agents from the current runtime run, or inspect run status/events/messages/artifacts. Prefer this skill for OpenLinker A2A delegation, multi-agent handoff, and run trace inspection."
---

# OpenLinker CLI Skill

Use this skill through the Blades `run_skill_script` tool. Do not invent shell commands.

The executable script is:

```json
{
  "skill_name": "openlinker-cli",
  "script_path": "scripts/openlinker"
}
```

Tokens and runtime context must come from environment variables. Never print or reveal token values.

## Environment

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

Common variables:

```bash
OPENLINKER_API_BASE
OPENLINKER_API_URL
OPENLINKER_RUN_ID
OPENLINKER_AGENT_ID
OPENLINKER_TRACE_ID
```

## Commands

Check current context:

```json
{
  "skill_name": "openlinker-cli",
  "script_path": "scripts/openlinker",
  "args": ["context"]
}
```

Search callable agents by text:

```json
{
  "skill_name": "openlinker-cli",
  "script_path": "scripts/openlinker",
  "args": ["agents", "search", "--query", "summary", "--callable"]
}
```

Search callable agents by tag:

```json
{
  "skill_name": "openlinker-cli",
  "script_path": "scripts/openlinker",
  "args": ["agents", "search", "--tag", "a2a", "--callable"]
}
```

Get an agent by slug:

```json
{
  "skill_name": "openlinker-cli",
  "script_path": "scripts/openlinker",
  "args": ["agents", "get", "--slug", "writer-agent"]
}
```

Start a top-level run:

```json
{
  "skill_name": "openlinker-cli",
  "script_path": "scripts/openlinker",
  "args": ["run", "--agent", "agent_writer", "--input", "{\"task\":\"write a short summary\"}"]
}
```

Delegate from the current run:

```json
{
  "skill_name": "openlinker-cli",
  "script_path": "scripts/openlinker",
  "args": ["delegate", "--agent", "agent_reviewer", "--reason", "review the draft", "--input", "{\"task\":\"review this draft\"}"]
}
```

Inspect run children:

```json
{
  "skill_name": "openlinker-cli",
  "script_path": "scripts/openlinker",
  "args": ["runs", "children", "--id", "run_xxx"]
}
```

## Stop Rule

After a successful CLI JSON response, summarize the result and stop. Do not call the same OpenLinker command repeatedly unless the user asks for another query.
