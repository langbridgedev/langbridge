# Langbridge Declarative Stripe Connector Example

This package is the first example of the declarative SaaS connector direction for `langbridge`.

Stripe was chosen because it gives a clean first slice:

- bearer-token auth
- stable REST endpoints under `/v1`
- simple cursor pagination with `starting_after`
- practical incremental sync using Stripe `created` timestamps
- common business resources such as `customers`, `charges`, and `invoices`

## What This Package Demonstrates

Core `langbridge` now owns the shared declarative SaaS connector contract under
`langbridge.connectors.saas.declarative`.

This package stays thin and owns only Stripe-specific declarative content:

- connector identity
- manifest data
- plugin registration
- the package-owned executable connector class that binds the manifest to the core declarative runtime
- optional examples and future hooks

The dataset layer is expected to own materialization decisions:

- choose a built-in resource by key
- provide params and filters
- choose sync behavior
- optionally override the built-in resource with a custom resource block

This package now demonstrates a real but narrow runtime contract:

- core `langbridge` owns the manifest schema, auth/config derivation helpers, and declarative HTTP execution runtime
- this package supplies the Stripe manifest, Stripe config schema, and the package-owned connector class that points at the core runtime
- manifest-declared resources execute through the normal `ApiConnector` sync flow and materialize runtime-managed datasets
- custom resource overrides and richer hook points are still later work, not part of the current slice

## Files

- `src/langbridge_connector_stripe/manifests/stripe.yaml`: declarative connector manifest
- `src/langbridge_connector_stripe/config.py`: thin runtime-facing config/schema adapters that call core declarative helpers
- `src/langbridge_connector_stripe/connector.py`: Stripe connector class backed by the core declarative HTTP runtime
- `src/langbridge_connector_stripe/plugin.py`: minimal plugin surface for Langbridge
- `examples/dataset_selection_examples.yaml`: dataset-layer examples for manifest-declared resources

Shared infrastructure now lives in core `langbridge`:

- `langbridge/connectors/saas/declarative/manifest.py`: declarative manifest schema and loader
- `langbridge/connectors/saas/declarative/config.py`: shared auth/config-schema derivation helpers
- `langbridge/connectors/saas/declarative/runtime.py`: manifest-driven HTTP connector runtime

## Dataset Selection Examples

Manifest resource by key:

```yaml
dataset:
  name: stripe_customers
  connector: stripe
  connector_sync:
    resource_key: customers
    sync_mode: INCREMENTAL
    params:
      limit: 100
```

Another manifest resource:

```yaml
dataset:
  name: stripe_invoices
  connector: stripe
  connector_sync:
    resource_key: invoices
    sync_mode: INCREMENTAL
    params:
      limit: 100
```

## Current Runtime Scope

This slice intentionally covers:

- manifest-defined auth headers
- manifest-defined resource discovery
- manifest-defined cursor pagination
- manifest-defined incremental checkpoints
- runtime sync materialization through the existing API connector flow

Still later:

- dataset-level custom resource overrides
- resource-specific Python hooks for non-declarative edge cases
- broader per-resource response-shape overrides beyond the shared connector contract
