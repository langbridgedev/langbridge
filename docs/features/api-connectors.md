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
