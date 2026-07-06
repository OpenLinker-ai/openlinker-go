# OpenLinker Native LLM Worker Example

This example runs a Blades Agent as an OpenLinker native runtime worker. The
OpenLinker runtime lifecycle and Blades result mapping are handled by
`openlinkerblades.WithAgent(agent).Run(ctx)`, so the worker only needs to create
the Agent.

## Run

Use pull mode when the Agent was registered with `connection_mode` set to
`runtime_pull`:

```bash
OPENLINKER_API_BASE=https://api.openlinker.ai \
OPENLINKER_RUNTIME_TOKEN=ol_live_runtime_xxx \
OPENLINKER_WORKER_CONNECTOR=runtime_pull \
OPENAI_MODEL=gpt-4.1-mini \
OPENAI_API_KEY=sk-xxxx \
go run .
```

Use WebSocket mode when the Agent was registered with `connection_mode` set to
`runtime_ws`:

```bash
OPENLINKER_API_BASE=https://api.openlinker.ai \
OPENLINKER_RUNTIME_TOKEN=ol_live_runtime_xxx \
OPENLINKER_WORKER_CONNECTOR=runtime_ws \
OPENAI_MODEL=gpt-4.1-mini \
OPENAI_API_KEY=sk-xxxx \
go run .
```

For OpenAI-compatible providers, set `OPENAI_BASE_URL`:

```bash
OPENAI_BASE_URL=https://api.example.com/v1 go run .
```

To stop after a fixed number of completed assignments:

```bash
OPENLINKER_WORKER_MAX_RUNS=1 go run .
```

## Configuration

| Variable | Default | Description |
| --- | --- | --- |
| `OPENLINKER_RUNTIME_TOKEN` | required | Runtime token created for the Agent. |
| `OPENLINKER_API_BASE` | `https://api.openlinker.ai` | OpenLinker API base URL. |
| `OPENLINKER_WORKER_CONNECTOR` | `runtime_pull` | `runtime_pull` or `runtime_ws`. |
| `OPENLINKER_WORKER_PULL_WAIT` | `25s` | Long-poll wait duration for pull mode. |
| `OPENLINKER_WORKER_MAX_RUNS` | `0` | Stop after this many completed runs. `0` means run forever. |
| `OPENAI_MODEL` | required | Model name used by Blades. |
| `OPENAI_API_KEY` | required | API key for OpenAI or an OpenAI-compatible provider. |
| `OPENAI_BASE_URL` | provider default | Optional OpenAI-compatible API base URL. |

## Input

The adapter reads the first non-empty value from `text`, `query`, `task`, or
`prompt`. It also accepts a plain string input.

```json
{
  "text": "write a one-line greeting"
}
```

## Output

```json
{
  "text": "Hello from the native LLM worker.",
  "llm": {
    "text": "Hello from the native LLM worker.",
    "run_id": "...",
    "agent_id": "...",
    "model": "gpt-4.1-mini"
  },
  "input": {
    "text": "write a one-line greeting",
    "raw": {
      "text": "write a one-line greeting"
    }
  }
}
```
