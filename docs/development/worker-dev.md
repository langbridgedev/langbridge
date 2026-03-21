# Worker Development

The queued worker is a thin runtime-owned assembly under `apps/runtime_worker`.
It exists for queued, hosted, or edge-style execution, but it should not be used
as the default explanation for how the runtime product works.

## What The Worker Does

The worker consumes queued messages and dispatches them into runtime-owned
handlers and services.

It is relevant when you need:

- queued execution
- broker-driven orchestration
- customer-runtime or edge-style task handling
- parity with hosted or hybrid execution flows

## Run The Worker

```bash
python -m langbridge.apps.runtime_worker.main
```

Reload mode:

```bash
python -m langbridge.apps.runtime_worker.main --reload
```

## Important Environment Variables

- `WORKER_CONCURRENCY`
- `WORKER_BATCH_SIZE`
- `WORKER_POLL_INTERVAL`
- `WORKER_BROKER`
- `WORKER_EXECUTION_MODE`
- `FEDERATION_ARTIFACT_DIR`
- `FEDERATION_BROADCAST_THRESHOLD_BYTES`
- `FEDERATION_PARTITION_COUNT`
- `FEDERATION_STAGE_MAX_RETRIES`
- `FEDERATION_STAGE_PARALLELISM`

## Main Code Paths

- runtime loop: `apps/runtime_worker/main.py`
- broker integrations: `apps/runtime_worker/broker/`
- message contracts: `apps/runtime_worker/messaging/contracts/`
- dispatch and handlers: `apps/runtime_worker/handlers/`
- dependency assembly: `apps/runtime_worker/ioc/`
- shared runtime execution primitives: `langbridge/runtime/*`

## Relationship To The Runtime Host

The worker and the runtime host should tell the same architectural story:

- runtime services and ports are owned by `langbridge.runtime`
- federation is owned by `langbridge.federation`
- connectors are owned by `langbridge.connectors` and `langbridge.plugins`
- control-plane orchestration stays outside this repo boundary
