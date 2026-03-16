# Runtime Boundary

## Purpose

`langbridge/` is the runtime product repository.

Its job is to produce portable execution capabilities that work in:

- self-hosted deployments
- embedded/local developer workflows
- hybrid enterprise deployments
- cloud-managed runtime deployments

This repository must not remain a mixed runtime plus cloud control-plane monolith.

## Ownership Rules

Code belongs in `langbridge/` when it is required to execute workloads even if Langbridge Cloud does not exist.

This includes:

- runtime host and execution APIs
- semantic execution
- federated planning and execution
- connectors
- dataset execution primitives
- runtime-safe agent execution primitives
- local runtime configuration
- SDK surfaces that target runtime or published APIs
- versioned contracts shared with cloud consumers

Code does not belong in `langbridge/` when it primarily exists to deliver or manage the hosted product.

This must move to `langbridge-cloud/`:

- cloud API application surfaces
- product web UI
- auth, orgs, workspaces, tenancy, invitations
- hosted job dispatch and orchestration glue
- runtime registry control-plane endpoints
- edge task gateway server surfaces
- database models and repositories for cloud persistence
- alembic migrations
- hosted observability and operations assets

## Current Mixed Areas

The current repo still contains cloud-owned code:

The product web app has already been moved to `langbridge-cloud/apps/web`.

The current repo also contains mixed packages that must be split before large moves:

- `langbridge/packages/common`
- `langbridge/packages/messaging`
- `langbridge/packages/orchestrator`
- `langbridge/packages/runtime`

Critical inversion violations already exist and must be removed early:

- runtime packages importing `langbridge.apps.*`
- runtime packages depending directly on cloud DB models and repositories
- contracts coupled to Redis transport details

## Target Runtime Shape

The runtime repo should converge on a package-oriented structure:

```text
langbridge/
  packages/
    contracts/
    runtime-config/
    runtime-core/
    runtime-control-plane-client/
    semantic-engine/
    federation-engine/
    connectors/
    messaging/
    agent-runtime/
    sdk/
    cli/
  apps/
    runtime/
  docker/
    runtime/
  docs/
```

### Package Intent

- `contracts/`: runtime-cloud schemas, OpenAPI-derived clients, transport-neutral job and result contracts
- `runtime-config/`: local runtime bootstrap and self-hosted configuration
- `runtime-core/`: runtime host, execution context, execution services, provider interfaces
- `runtime-control-plane-client/`: optional client adapters for hosted and hybrid runtime integration
- `semantic-engine/`: semantic model and semantic execution logic
- `federation-engine/`: planner, optimizer, stage execution, artifact handling
- `connectors/`: connector implementations and schemas
- `messaging/`: transport-neutral envelopes, job contracts, and message handler abstractions
- `agent-runtime/`: runtime-safe agent execution and tool interfaces
- `sdk/`: user-facing SDK built only on published runtime artifacts and published API contracts
- `cli/`: thin operational entrypoints for local and self-hosted runtime workflows

### Thin Apps Only

`apps/` must contain composition surfaces only.

Allowed examples:

- `apps/runtime`: thin runtime assembly for packaged execution

Not allowed:

- full cloud API surfaces
- product UI
- tenancy/auth/product orchestration logic

## Explicit Artifact Boundary

`langbridge/` is a product-producing repository.

It publishes:

- versioned Python packages to a private package registry
- versioned OCI images for runtime deployment
- versioned contracts and generated clients

Current checked-in contract artifact:

- runtime-owned contracts should ship from `langbridge.packages.contracts`
- the cloud-owned control-plane OpenAPI snapshot now lives in
  `../langbridge-cloud/contracts/openapi/control-plane.openapi.json`

Legacy app retirement order:

1. Keep `langbridge/apps/runtime_worker` thin and runtime-only.
2. Keep `apps/` only for thin runtime assembly, such as `apps/runtime`, or remove it entirely if no runtime app remains.

Recommended first-class runtime images:

- `ghcr.io/langbridgedev/runtime-worker`
- `ghcr.io/langbridgedev/runtime-edge-worker`
- optional `ghcr.io/langbridgedev/runtime-local`

Recommended registry split:

- Python runtime packages: AWS CodeArtifact
- OCI images: GHCR
- npm packages if needed later: GitHub Packages or the same CodeArtifact domain

GitHub Packages is not the Python package registry recommendation here because
the runtime/cloud package boundary is primarily Python and needs a real private
Python index.

`langbridge-cloud/` consumes:

- released runtime packages
- released runtime OCI images
- released contracts and generated clients

Direct sibling source imports across repositories are forbidden.

## Import Rules

The following patterns are forbidden in `langbridge/`:

- `packages/*` importing `apps/*`
- runtime packages importing cloud repository or DB layers
- permanent `file:` dependencies as a cross-repo strategy
- source copy-sync, subtree sync, or submodules as the integration model

## Migration Order

1. Freeze the repo boundary in documentation and CI policy.
2. Extract runtime-facing contracts from `packages/common`.
3. Remove `packages/* -> apps/*` imports.
4. Split runtime-safe code from cloud persistence and orchestration glue.
5. Move cloud apps and control-plane ownership into `langbridge-cloud/`.
6. Switch cloud to released runtime artifacts only.

## Definition Of Done

The runtime boundary is established when:

- `langbridge/` contains no cloud-owned app surfaces
- runtime packages do not import cloud apps
- runtime packages do not depend on cloud DB/repository code
- cloud consumes only published runtime artifacts
- release compatibility between cloud and runtime is versioned and documented
