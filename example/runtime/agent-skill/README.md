# OpenLinker Skill Agent Example

This example shows how a Blades Agent can use the OpenLinker CLI through a
Skill. The repository does not commit the compiled `openlinker` binary, so build
it locally before running this example.

## Build the CLI

From the repository root:

```bash
mkdir -p ./example/runtime/agent-skill/skills/openlinker-cli/scripts
go build -o ./example/runtime/agent-skill/skills/openlinker-cli/scripts/openlinker ./cmd/cli
chmod +x ./example/runtime/agent-skill/skills/openlinker-cli/scripts/openlinker
```

The generated file is a local build artifact. Do not commit it.

## Run

```bash
cd example/runtime/agent-skill

OPENAI_API_KEY=sk-xxx \
OPENLINKER_API_BASE=https://api.openlinker.ai \
OPENLINKER_RUNTIME_TOKEN=ol_runtime_xxx \
go run .
```

Optional runtime context:

```bash
OPENLINKER_AGENT_ID=agent_xxx
OPENLINKER_RUN_ID=run_xxx
OPENLINKER_TRACE_ID=trace_xxx
```

The Skill asks Blades to invoke `scripts/openlinker` through
`run_skill_script`. The CLI reads credentials and run context from the
environment, so real tokens should stay outside `SKILL.md`, prompts, logs, and
commits.

## Use the Skill Elsewhere

Copy `skills/openlinker-cli` into your Agent project, build `cmd/cli` for the
target machine, and place the resulting executable at:

```text
skills/openlinker-cli/scripts/openlinker
```

Then load the Skill in your Agent framework and provide the same OpenLinker
environment variables at runtime.
