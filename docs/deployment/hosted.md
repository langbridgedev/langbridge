# Hosted Deployment

Hosted mode runs Langbridge control and execution planes in one managed environment.

## Components

- Control Plane API (`langbridge/apps/api`)
- Worker runtime (`langbridge/apps/worker`)
- UI (`client`)
- Shared infra (Postgres, Redis, optional observability stack)

## Local Hosted-Like Compose

Run core services only:

```bash
docker compose up --build migrate api worker client db redis
```

Endpoints:
- UI: `http://localhost:3000`
- API docs: `http://localhost:8000/docs`

## Notes

- SQL and semantic execution use the Worker + Federated Query Engine path.
- Hosted deployments only need the API, Worker, UI, Postgres, and Redis core services.
