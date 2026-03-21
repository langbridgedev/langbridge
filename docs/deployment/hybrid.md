# Hybrid Deployment

Hybrid deployment means Langbridge runtime execution stays in customer-managed
infrastructure while a separate control layer may coordinate or observe it.

In practice:

- connectors, secrets, and source access stay on the runtime side
- the runtime executes with the same workspace-scoped model as self-hosted mode
- integration with cloud or external systems happens through explicit runtime-owned ports and contracts

## Runtime / Cloud Split

In a hybrid setup:

- `langbridge/` owns runtime execution, hosting, connectors, federation, and runtime identity
- `langbridge-cloud/` may own control-plane coordination, registration, or hosted UX

The runtime should remain portable even when one deployment path uses a control
plane.

## Typical Shapes

- runtime host in customer infrastructure with a separate cloud control plane
- queued worker in customer infrastructure pulling or receiving externally assigned work
- embedded runtime inside a customer-managed application with cloud-managed metadata outside the runtime boundary

## Design Rules

- runtime ports stay runtime-owned
- connector credentials never move into the cloud layer by default
- external product identity should be translated into workspace-scoped runtime identity at the boundary
- hybrid integrations should use explicit clients, contracts, or message envelopes instead of leaking cloud logic into runtime modules
