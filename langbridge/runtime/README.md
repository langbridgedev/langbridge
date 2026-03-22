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

## Main Modules

- `context.py`: runtime context
- `local_config.py`: configured local runtime builder
- `hosting/`: FastAPI host, auth, API models, server
- `services/`: dataset, SQL, semantic, sync, and agent execution services
- `execution/`: runtime execution helpers including the federated query bridge
- `providers/`: metadata, credential, cache, and repository adapters
- `persistence/`: repositories, stores, and database mappings

## Important Notes

- runtime-core execution is workspace-scoped, not driven by external product identity
- the self-hosted runtime host currently serves configured local runtimes
- the runtime package is self-contained and does not require a separate control layer
