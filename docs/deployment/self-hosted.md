# Self-Hosted Deployment

Self-hosted mode runs both control and execution planes in customer-managed infrastructure.
The control-plane web app now lives in `langbridge-cloud/`; this repo documents
the runtime side of the deployment.

## Minimum Services

- API
- Worker
- Postgres
- Redis

Optional:
- Qdrant

## Recommended Start

Use the core compose services:

```bash
docker compose up --build migrate api worker db redis
```

## Operational Guidance

- Keep API and Worker versions aligned.
- Configure secrets via env/secret manager, never commit `.env` secrets.
- Tune worker concurrency with:
  - `WORKER_CONCURRENCY`
  - `WORKER_BATCH_SIZE`
- Configure federation planner knobs:
  - `FEDERATION_BROADCAST_THRESHOLD_BYTES`
  - `FEDERATION_PARTITION_COUNT`
  - `FEDERATION_STAGE_MAX_RETRIES`
  - `FEDERATION_STAGE_PARALLELISM`

## Execution Model

Self-hosted production rollouts use the same API + Worker + Federated Query Engine
path as hosted deployments, with cloud-owned control-plane surfaces consumed
separately from `langbridge-cloud/`.

Control-plane observability manifests now live in `../langbridge-cloud/monitoring`.
