# Langbridge Contracts

Runtime-owned contract surface for versioned schemas shared across the runtime,
cloud control plane, and generated clients.

This package is being extracted away from `langbridge_common.contracts`.
Foundational modules such as `base`, `llm_connections`, semantic query
contracts, and the core SDK-facing dataset / SQL / thread / job contracts now
live here directly. Other modules still re-export their legacy implementations
while ownership is migrated in slices.

For the modules already migrated here, `langbridge_common.contracts` now acts
as a backward-compatible import shim back to this package.

Canonical public imports should use `langbridge.contracts.*`.
`langbridge.packages.contracts.*` remains as a temporary compatibility surface.
