# Run a RuntimeWorker from start to finish

[简体中文](runtime-worker-end-to-end.zh-CN.md)

This guide covers the complete path for a managed `openlinker-go`
`RuntimeWorker`: platform resources, Runtime Node, mTLS, Agent identity,
durable local state, a real Run, cancellation, restart, and Kubernetes
deployment.

A `Runtime Ready` log proves only that the connection is ready. A deployment
is ready only after all three checks pass:

```text
Connection: Runtime Ready
    ↓
Execution: a real Run succeeds and Core accepts its Events and Result
    ↓
Reliability: cancellation works and a process or Pod restart keeps durable state
```

## Three separate identities

| Identity | Typical value | Used for | Created by |
| --- | --- | --- | --- |
| User | User Token or login JWT | Create Runs, read results, manage Agents | User or administrator |
| Agent | Agent ID and Agent Token | Select the Agent that receives work | Agent registration |
| Runtime Node | Node ID and mTLS certificate/key | Trust the machine connecting to Runtime | Core operator |

Do not mix them:

- A User Token is not a worker credential.
- An Agent Token does not replace Runtime Node mTLS.
- Agent auto-registration does not create a Runtime Node or certificate.
- A Node ID is not a Pod ID, host name, or SDK Worker ID.

## Choose the right SDK entry

Most Go Agents should use:

```go
openlinker.WithAgent(agent).Run(ctx)
```

Frameworks that need task metadata, Events, progress, child Agent calls, and
custom Results should use:

```go
openlinker.Native(handler).Run(ctx)
```

Use `NewRuntimeWorker` directly only when infrastructure code must select the
store, capacity, logger, or process lifecycle. All three entries use the same
reliable Runtime worker underneath.

## 1. Check Core

```bash
export OPENLINKER_API_BASE='https://openlinker.example'
curl -fsS "$OPENLINKER_API_BASE/readyz"
```

Confirm that `ready` is true and `mode` is `normal`. Restore Core first if
it reports hard maintenance, a member that is not ready, or HTTP 503. A TLS
listener by itself does not mean Core allows new Runtime Sessions.

## 2. Prepare the Agent

For an existing production Agent, obtain:

```text
OPENLINKER_AGENT_ID=<Agent UUID>
OPENLINKER_AGENT_TOKEN=<active Agent Token>
```

Do not put a User Token in a long-running production Pod.

For a local first run, the high-level API can create or reuse an Agent:

```go
err := openlinker.WithAgent(agent).RunOrRegister(
    ctx,
    openlinker.AgentSpec{
        Slug:       "my-agent",
        Name:       "My Agent",
        Visibility: "private",
    },
)
```

The first registration needs a User Token. Save the returned Agent registration
and remove the User Token from later worker starts. `NewRuntimeWorker` never
creates an Agent implicitly.

## 3. Issue the Runtime Node and mTLS identity

A Core operator runs the node-issuance command in a trusted administration
environment:

```bash
/app/api runtime-node issue \
  --ca-cert /secure/runtime-client-ca.crt \
  --ca-key /secure/runtime-client-ca.key \
  --display-name my-runtime-node \
  --node-version openlinker-go/runtime-worker \
  --capacity 1 \
  --valid-for 8760h \
  --cert-out /secure/runtime-node.crt \
  --key-out /secure/runtime-node.key
```

The worker needs:

- the Node ID returned by Core;
- `runtime-node.crt`;
- `runtime-node.key`;
- the Runtime server CA certificate.

Never distribute the Runtime Client CA private key to an Agent machine.

The registered Node version must exactly match the worker's `NodeVersion`.
The default managed worker reports:

```text
openlinker-go/runtime-worker
```

If your code sets `NodeVersion` to another value, issue the Node with that
exact value. The Node capacity must be at least the total capacity of workers
using that Node identity.

Check that the certificate is current, has client-auth usage, and contains the
expected Node ID in its URI SAN.

## 4. Configure the worker

```bash
export OPENLINKER_API_BASE='https://openlinker.example'
export OPENLINKER_RUNTIME_BASE='https://runtime.openlinker.example'

export OPENLINKER_NODE_ID='11111111-1111-4111-8111-111111111111'
export OPENLINKER_AGENT_ID='22222222-2222-4222-8222-222222222222'
export OPENLINKER_AGENT_TOKEN='<read from a secret store>'

export OPENLINKER_NODE_CERT_FILE='/run/openlinker/runtime-node.crt'
export OPENLINKER_NODE_KEY_FILE='/run/openlinker/runtime-node.key'
export OPENLINKER_RUNTIME_CA_FILE='/run/openlinker/runtime-server-ca.crt'

export OPENLINKER_RUNTIME_TRANSPORT='auto'
export OPENLINKER_RUNTIME_CAPACITY='1'
export OPENLINKER_RUNTIME_DATA_DIR='/var/lib/my-agent/runtime'
```

`OPENLINKER_RUNTIME_BASE` is optional when the worker can discover Runtime
from `OPENLINKER_API_BASE`. Set
`OPENLINKER_RUNTIME_SERVER_NAME` only when certificate verification needs an
explicit server-name override.

Do not put Tokens in an image, Git, an ordinary ConfigMap, command history, or
logs.

## 5. Prepare durable local state

```bash
install -d -m 700 /var/lib/my-agent/runtime
chmod 600 /run/openlinker/runtime-node.key
```

The default file store keeps:

- stable Worker identity and increasing Session epoch;
- task journal and input;
- encrypted pending Event and Result records;
- resume state and the encryption key.

Use a private persistent volume, not a container layer or `emptyDir`. One data
directory may be opened by only one worker process. Do not delete its identity,
journal, key, or pending records to fix an error. Migrate or replace the
directory only through an operator procedure after confirming no work needs to
resume.

## 6. Implement and run the handler

```go
package main

import (
    "context"
    "errors"
    "log"

    openlinker "github.com/OpenLinker-ai/openlinker-go"
)

func main() {
    config, err := openlinker.LoadRuntimeWorkerConfig()
    if err != nil {
        log.Fatal(err)
    }
    config.Handler = openlinker.RuntimeHandlerFunc(handle)
    config.OnReady = func(ready openlinker.RuntimeReadyPayload) {
        log.Printf("runtime ready: core=%s attachment=%s",
            ready.CoreInstanceID, ready.AttachmentID)
    }

    worker, err := openlinker.NewRuntimeWorker(config)
    if err != nil {
        log.Fatal(err)
    }
    if err := worker.Run(context.Background()); err != nil &&
        !errors.Is(err, context.Canceled) {
        log.Fatal(err)
    }
}

func handle(
    ctx context.Context,
    run openlinker.RuntimeContext,
) (openlinker.RuntimeResult, error) {
    if err := run.Emit("run.message.delta", map[string]any{
        "text": "started",
    }); err != nil {
        return openlinker.RuntimeResult{}, err
    }
    select {
    case <-ctx.Done():
        return openlinker.RuntimeResult{}, ctx.Err()
    default:
    }
    return openlinker.RuntimeResult{
        Status: "success",
        Output: map[string]any{"input": run.Input, "text": "done"},
    }, nil
}
```

The handler starts only after Core confirms the task. Cancellation and deadline
reach the handler through `ctx`. Events are made durable before upload. The
Result remains in durable pending delivery until Core acknowledges it.

A `RuntimeWorker` is single-use. Build a new instance after `Run` returns.

## 7. Verify a real Run

Use a User Token or login session, never the Agent Token, to create a Run:

```bash
export OPENLINKER_USER_TOKEN='<read from a secret store>'
export OPENLINKER_IDEMPOTENCY_KEY="runtime-check-$(date +%s)"

curl -fsS -X POST \
  "$OPENLINKER_API_BASE/api/v1/runs" \
  -H "Authorization: Bearer $OPENLINKER_USER_TOKEN" \
  -H "Idempotency-Key: $OPENLINKER_IDEMPOTENCY_KEY" \
  -H 'Content-Type: application/json' \
  --data "{\"agent_id\":\"$OPENLINKER_AGENT_ID\",\"input\":{\"text\":\"runtime check\"}}"
```

Confirm that:

- the task reaches the handler;
- Events appear in order;
- the Run ends successfully with the expected output;
- the worker receives the Result ACK and clears its pending record.

## 8. Verify cancellation

Start work that stays active long enough to cancel, then call the Run cancel
endpoint with the User Token. Confirm that the handler's context is canceled,
the Run reaches canceled semantics, and the worker can accept a later task.

An external process started by the handler must also receive cancellation and
be reaped by the application.

## 9. Verify restart and resume

Keep the same data directory and test:

1. a normal stop and restart;
2. a process kill while an Event or Result is waiting for ACK;
3. a restart while Core still remembers the previous Session.

The worker must reuse its Worker identity, advance its Session epoch, replay
durable records with the same IDs, and never run a handler twice after it has
crossed the durable started boundary.

## Kubernetes checklist

- Put public addresses, Node ID, transport, capacity, and file paths in a
  ConfigMap.
- Put the Agent Token and mTLS private key in Secrets.
- Use a persistent volume for the Runtime data directory.
- Do not share one data directory between replicas.
- Run as non-root and preserve directory mode `0700` and private file mode
  `0600`.
- Use `fsGroupChangePolicy: OnRootMismatch` when a group is required; avoid
  recursive permission changes that make private files group-readable.
- Mount mTLS files read-only.
- On shutdown, stop accepting new work and allow enough time for active work
  and pending delivery to finish.

## Final acceptance

- Core is ready and not in maintenance.
- Agent ID, Agent Token, Node ID, Node version, and certificate SAN agree.
- Tokens and private keys are absent from images, Git, logs, and ConfigMaps.
- The data directory is private, durable, and locked by one worker.
- Runtime Ready succeeds.
- A real Run succeeds and its output is visible.
- Cancellation reaches the handler and any child process.
- Restart preserves identity and reliable pending delivery.

See [the Runtime examples](example/runtime/README.md) for minimal, Native,
managed, and low-level protocol code.
