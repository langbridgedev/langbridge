# Hosted Deployment

Hosted mode runs Langbridge control and execution planes in one managed environment.
The web application for that managed product now lives in `langbridge-cloud/`.

## Components

- Control Plane API (`langbridge-cloud/apps/api`)
- Worker runtime (`langbridge/apps/runtime_worker`)
- Web UI (`langbridge-cloud/apps/web`)
- Shared infra (Postgres, Redis)

## Local Hosted-Like Compose

Run core services only:

```bash
docker compose up --build migrate api worker db redis
```

Endpoints:
- Web app: start from `langbridge-cloud/apps/web`
- API docs: `http://localhost:8000/docs`

## Notes

- SQL and semantic execution use the Worker + Federated Query Engine path.
- Hosted deployments need API, Worker, Postgres, Redis, and the cloud-owned web app.
- Control-plane observability manifests now live in `../langbridge-cloud/monitoring`.
