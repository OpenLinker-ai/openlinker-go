# OpenLinker A2A Agent Example

This example keeps the generic Agent entrypoint:

```go
openlinker.WithAgent(agent).Run(ctx)
```

Inside `Run`, it calls another Agent once:

```go
func (a A2AAgent) Run(ctx context.Context, input string) (string, error) {
	run, _ := openlinker.NativeRunFromContext(ctx)

	child, err := a.Caller.CallAgent(ctx, openlinker.CallAgentRequest{
		ParentRunID:   run.Assignment.RunID,
		CurrentRunID:  run.Assignment.RunID,
		TargetAgentID: a.TargetAgentID,
		Input:         openlinker.JSON{"task": input},
	})
	if err != nil {
		return "", err
	}

	return "child run: " + child.RunID, nil
}
```

## Run

```bash
OPENLINKER_RUNTIME_TOKEN=ol_live_runtime_xxx \
A2A_TARGET_AGENT_ID=agent_child_xxx \
go run .
```

Optional settings:

```bash
OPENLINKER_API_BASE=https://api.openlinker.ai
OPENLINKER_WORKER_CONNECTOR=runtime_pull
OPENLINKER_WORKER_MAX_RUNS=1
```
