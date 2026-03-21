# Runtime Boundary

This repository exists to build and ship the Langbridge runtime product.

## Langbridge Versus Langbridge Cloud

`langbridge/` owns:

- embedded runtime execution
- self-hosted runtime hosting
- workspace-scoped runtime identity
- runtime-owned auth, connectors, datasets, semantic query, federation, and orchestration
- runtime-owned ports, providers, services, contracts, and persistence
- thin runtime assemblies such as the queued worker

`langbridge-cloud/` owns:

- hosted control-plane APIs
- cloud web application surfaces
- hosted worker orchestration and cloud-only operations
- product-account and control-plane administration
- cloud migrations and control-plane persistence

## What Belongs In This Repo

Code belongs here when it must exist for the runtime to execute workloads in:

- embedded Python use
- local development
- self-hosted deployment
- hybrid customer-managed deployment

That includes:

- `langbridge.runtime`
- `langbridge.client`
- `langbridge.connectors`
- `langbridge.plugins`
- `langbridge.semantic`
- `langbridge.federation`
- `langbridge.orchestrator`
- `langbridge.contracts`
- `apps/runtime_worker`

## What Should Stay Out

Do not treat this repo as the home for:

- control-plane product APIs
- cloud UI flows
- external product identity models as runtime-core identity
- cloud-only orchestration glue
- cloud-only operational tooling

## Current Shape

```text
langbridge/
  client/
  connectors/
  contracts/
  federation/
  orchestrator/
  plugins/
  runtime/
  semantic/
apps/
  runtime_worker/
packages/
  sdk/
```

`packages/sdk` is packaging for a separate SDK distribution. It is not the old
architecture model for the repo.

## Runtime-Owned Identity And Auth

Runtime-core identity is limited to:

- `workspace_id`
- `actor_id`
- `roles`
- `request_id`

Self-hosted runtime auth is intentionally thin:

- no auth
- static bearer token
- JWT bearer token

If a richer cloud product identity exists, it should be translated into this
runtime context at the boundary rather than pushed into runtime-core execution.

## Runtime-Owned Ports

Ports and adapters that define runtime behavior should stay runtime-owned even
when a cloud product provides one implementation.

Examples:

- dataset and connector metadata providers
- semantic model providers
- sync state providers
- credential providers
- runtime host and execution services

The runtime may integrate with a control plane through adapters, but the port
definitions and execution behavior stay here.
