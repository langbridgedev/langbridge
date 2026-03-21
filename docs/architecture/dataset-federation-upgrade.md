# Dataset Federation Upgrade

This note tracks the runtime direction toward dataset-first structured execution.

## Current Direction

Legacy dataset types such as `TABLE`, `SQL`, `FILE`, and `FEDERATED` still exist
for compatibility, but the runtime is increasingly driven by richer execution
descriptors.

## What The Runtime Is Standardizing Around

- `source_kind`
- `connector_kind`
- `storage_kind`
- `relation_identity`
- `execution_capabilities`

## Why This Matters

- sync-materialized datasets and file-backed datasets can participate in structured execution more naturally
- runtime services and federation can reason from capabilities instead of bespoke source branches
- SQL, semantic, and agent workloads can share the same dataset-first execution substrate

## Status

This is directionally true in the current codebase, but not every legacy path has
been removed yet. New runtime docs and new execution work should describe the
dataset-first direction, not the older coarse-type model.
