# Deprecations and Migration

## Migration Note

**Trino removed in favor of built-in Federated Query Engine.**

Langbridge's target architecture no longer depends on:
- Trino as external federated query runtime.
- SQL gateway as an external SQL data plane.

The built-in federated planner and execution engine (Worker + `packages/federation`) is now the only supported structured engine.

## Removed Runtime Surface

The release-hardening cleanup removed the remaining tracked runtime residue:

- `langbridge/packages/connectors/langbridge_connectors/api/_trino/**`
- `langbridge/Dockerfile.gateway`
- `langbridge/requirements-gateway.txt`
- `langbridge/main.py`
- CI docker matrix entry for `langbridge/Dockerfile.gateway`
- Legacy env variables:
  - `TRINO_*`
  - `UNIFIED_TRINO_*`

## Historical Note

New implementation work should target control-plane dispatch + worker execution + federation package only.
