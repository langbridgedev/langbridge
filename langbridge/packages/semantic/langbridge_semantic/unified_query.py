from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Mapping, Sequence
from uuid import UUID

from langbridge.packages.semantic.langbridge_semantic.loader import load_semantic_model
from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel


@dataclass(frozen=True)
class UnifiedSourceModel:
    model: SemanticModel
    connector_id: UUID


@dataclass(frozen=True)
class TenantAwareQueryContext:
    organization_id: UUID
    execution_connector_id: UUID


def build_unified_semantic_model(
    *,
    source_models: Sequence[UnifiedSourceModel],
    joins: Sequence[Mapping[str, Any]] | None = None,
    metrics: Mapping[str, Any] | None = None,
    name: str | None = None,
    description: str | None = None,
    dialect: str | None = None,
    version: str = "1.0",
) -> tuple[SemanticModel, dict[str, UUID]]:
    """
    Merge multiple semantic models into a single SemanticModel payload.

    Returns the unified model and a mapping of table_key -> source connector_id.
    """
    if not source_models:
        raise ValueError("source_models must include at least one semantic model.")

    semantic_models_payload: list[dict[str, Any]] = []
    table_connector_map: dict[str, UUID] = {}
    for source in source_models:
        semantic_models_payload.append(source.model.model_dump(by_alias=True, exclude_none=True))
        for table_key in source.model.tables.keys():
            if table_key in table_connector_map:
                raise ValueError(f"Duplicate table key '{table_key}' detected while building unified model.")
            table_connector_map[table_key] = source.connector_id

    unified_payload: dict[str, Any] = {
        "version": version,
        "name": name,
        "description": description,
        "dialect": dialect,
        "semantic_models": semantic_models_payload,
    }
    if joins:
        unified_payload["relationships"] = list(joins)
    if metrics:
        unified_payload["metrics"] = dict(metrics)

    return load_semantic_model(unified_payload), table_connector_map


def apply_tenant_aware_context(
    semantic_model: SemanticModel,
    *,
    context: TenantAwareQueryContext,
    table_connector_map: Mapping[str, UUID] | None = None,
) -> SemanticModel:
    """
    Produce a tenant-aware semantic model by assigning per-table catalogs.

    Catalog naming strategy is deterministic: `org_<org12>__src_<connector12>`.
    Existing explicit table catalogs are preserved.
    """
    model_copy = semantic_model.model_copy(deep=True)

    for table_key, table in model_copy.tables.items():
        if table.catalog:
            continue

        schema = (table.schema or "").strip()
        if "." in schema:
            # Normalize existing catalog.schema syntax into dedicated fields.
            catalog, normalized_schema = schema.split(".", 1)
            table.catalog = catalog
            table.schema = normalized_schema
            continue

        connector_id = context.execution_connector_id
        if table_connector_map and table_key in table_connector_map:
            connector_id = table_connector_map[table_key]

        table.catalog = _build_catalog_token(
            organization_id=context.organization_id,
            connector_id=connector_id,
        )

    return model_copy


def _build_catalog_token(*, organization_id: UUID, connector_id: UUID) -> str:
    org_token = organization_id.hex[:12]
    connector_token = connector_id.hex[:12]
    return f"org_{org_token}__src_{connector_token}"

