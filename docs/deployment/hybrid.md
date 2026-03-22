# Hybrid Deployment

Hybrid deployment means Langbridge runtime execution stays close to customer-managed data while integrating with external systems.

In practice:

- connectors, secrets, and source access stay on the runtime side
- the runtime executes with the same workspace-scoped model as self-hosted mode
- integration with external systems happens through explicit runtime-owned APIs and adapters

## Typical Shapes

- a runtime host inside customer infrastructure exposed to an internal platform
- an embedded runtime inside a customer-managed application
- a runtime deployment that is remotely observed or automated by external tooling

## Design Rules

- runtime ports stay runtime-owned
- connector credentials stay on the runtime side by default
- external identity should be translated into workspace-scoped runtime identity at the boundary
- hybrid integrations should use explicit clients, schemas, or message envelopes instead of leaking external control logic into runtime modules
