# Runtime Package

`langbridge.runtime` is the core runtime module for the Langbridge monolith.

It owns:

- runtime context and workspace-scoped identity
- configured runtime construction
- self-hosted runtime hosting
- runtime auth for self-hosted deployment
- runtime services, providers, persistence, and execution helpers

## Core Concepts

Runtime execution identity is:

- `workspace_id`
- `actor_id`
- `roles`
- `request_id`

Self-hosted auth modes are:

- `none`
- `static_token`
- `jwt`

When runtime auth is enabled, the self-hosted UI can also use a runtime-owned local operator session
for first-admin bootstrap and browser login without introducing cloud tenancy or signup flows.

## Main Modules

- `context.py`: runtime context
- `config/`: YAML schema, loading, validation, path resolution, normalization
- `bootstrap/`: configured runtime bootstrap and lower-level runtime assembly
- `application/`: runtime-facing orchestration split by concern
- `hosting/`: FastAPI host, auth, API models, server
- `services/`: dataset, SQL, semantic, sync, and agent execution services
- `execution/`: runtime execution helpers including the federated query bridge
- `providers/`: metadata, credential, cache, and repository adapters
- `persistence/`: in-memory stores, SQL runtime persistence, UoW, repositories, database mappings

## Important Notes

- runtime-core execution is workspace-scoped, not driven by external product identity
- the self-hosted runtime host currently serves configured local runtimes
- the runtime package is self-contained and does not require a separate control layer
- configured persisted runtimes bootstrap SQL resources once, then use a fresh Unit of Work and
  request-scoped session per operation rather than a shared long-lived `AsyncSession`
- `application/` owns runtime-facing orchestration for threads, agents, semantic, SQL, datasets,
  and connectors; bootstrap assembles the host but should not become the request workflow layer
- runtime hosts now have an explicit close path; local clients and FastAPI lifespan shutdown both
  dispose persisted runtime resources through the configured host
- persisted runtime metadata schema changes are Alembic-managed from the repo root; use
  `langbridge migrate --config ...` for explicit upgrades, or leave
  `runtime.migrations.auto_apply` enabled for local/self-hosted startup convenience
