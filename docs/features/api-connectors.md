# Connectors, Plugins, And Sync

Langbridge uses a plugin-style connector model.

## Structure

- connector interfaces and registration live under `langbridge.plugins`
- built-in connectors live under `langbridge.connectors`
- external connector packages can register through entry points

This keeps connector extension runtime-owned while allowing separate
distribution of connector packages where needed.

## Runtime Direction

Connectors should be understood in two groups:

- query-time connectors for direct SQL, NoSQL, or vector access
- sync-oriented SaaS or API connectors that materialize runtime-managed datasets

For SaaS and API sources, the intended direction is sync first:

- list available resources
- sync selected resources
- track per-resource sync state
- materialize datasets owned by the runtime workspace

That is a better fit for semantic, SQL, and agent workloads than querying third
party APIs directly during federation.

## Runtime-Owned Sync

Connector sync is owned by the runtime and exposed through runtime host
endpoints. The current self-hosted host supports:

- connector listing
- syncable resource discovery
- sync state inspection
- sync execution

The resulting datasets stay inside the runtime execution model.

## Declarative SaaS Connector Ownership

The declarative SaaS connector contract belongs in core `langbridge` under
`langbridge.connectors.saas.declarative`.

Core owns:

- manifest models and schema validation
- manifest loading helpers
- shared auth/config-schema derivation helpers
- manifest-driven HTTP execution for narrow sync-oriented SaaS connectors

The current declarative runtime slice is intentionally narrow and runtime-first:

- package manifests define auth, pagination, incremental cursor rules, and resource inventory
- core `langbridge` turns that manifest into an executable `ApiConnector`
- the existing runtime sync flow materializes those resources into runtime-managed datasets

This is enough for real manifest-defined SaaS sync without forcing every connector into the declarative model.

Connector packages under `langbridge-connectors` should stay thin and primarily
provide manifest files, package-specific config/schema adapters, and a package-owned
connector class that points at the core declarative runtime.
