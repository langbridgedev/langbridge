# Runtime Boundary

This repository exists to build and ship the Langbridge runtime.

## What Belongs In This Repo

Code belongs here when it is required for the runtime to execute workloads in:

- embedded Python use
- local development
- self-hosted deployment
- customer-managed runtime deployments

That includes:

- `langbridge.runtime`
- `langbridge.client`
- `langbridge.connectors`
- `langbridge.plugins`
- `langbridge.semantic`
- `langbridge.federation`
- `langbridge.orchestrator`
- `langbridge.mcp`
- `langbridge.ui`
- `apps/runtime_ui`
- `packages/sdk`

## What Should Stay Out

Do not treat this repo as the home for:

- unrelated product-web concerns
- external account or tenant administration models as runtime-core identity
- external orchestration logic embedded directly into runtime services
- tooling that requires the runtime to depend on a separate control plane to function

## Current Shape

```text
langbridge/
  client/
  connectors/
  federation/
  mcp/
  orchestrator/
  plugins/
  runtime/
  semantic/
  ui/
apps/
  runtime_ui/
packages/
  sdk/
```

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

## Runtime-Owned Ports

Ports and adapters that define runtime behavior should stay runtime-owned.

Examples:

- dataset and connector metadata providers
- semantic model providers
- sync state providers
- credential providers
- runtime host and execution services
- MCP and UI surfaces mounted by the runtime host
