# Langbridge Contracts

Runtime-owned contract surface for versioned schemas shared across the runtime,
cloud control plane, and generated clients.

This package is the first extraction step away from
`langbridge_common.contracts`. During migration it re-exports the existing
contract implementations so runtime-owned packages can move to a stable public
surface before the underlying implementations are fully relocated.

Public imports should use `langbridge.packages.contracts.*`.
