# Worker Development

## Worker Role

Runtime worker is the execution plane runtime for:
- SQL jobs
- Semantic query jobs
- Dataset preview/profile/ingest jobs
- Connector sync jobs
- Federated planning/execution integration

## Run Worker

```bash
python -m langbridge.apps.runtime_worker.main
```

Reload mode:

```bash
python -m langbridge.apps.runtime_worker.main --reload
```

## Key Environment Variables

- `WORKER_CONCURRENCY`
- `WORKER_BATCH_SIZE`
- `WORKER_POLL_INTERVAL`
- `WORKER_EXECUTION_MODE` (`hosted` or `customer_runtime`)
- `WORKER_BROKER`
- `FEDERATION_ARTIFACT_DIR`
- `FEDERATION_BROADCAST_THRESHOLD_BYTES`
- `FEDERATION_PARTITION_COUNT`
- `FEDERATION_STAGE_MAX_RETRIES`
- `FEDERATION_STAGE_PARALLELISM`

## Customer Runtime Mode

Set:
- `WORKER_EXECUTION_MODE=customer_runtime`
- `EDGE_API_BASE_URL=<control-plane>/api/v1`
- `EDGE_REGISTRATION_TOKEN=<registration token>`

Worker will register and use edge task pull/ack/result/fail transport.

## Main Code Paths

- Runtime loop: `langbridge/apps/runtime_worker/main.py`
- Dispatcher: `langbridge/apps/runtime_worker/handlers/dispatcher.py`
- SQL job handler: `langbridge/apps/runtime_worker/handlers/query/sql_job_request_handler.py`
- Semantic handler: `langbridge/apps/runtime_worker/handlers/query/semantic_query_request_handler.py`
- Federated tool: `langbridge/packages/runtime/execution/federated_query_tool.py`
