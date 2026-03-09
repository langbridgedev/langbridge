from __future__ import annotations

import re
import uuid
from collections.abc import Mapping
from typing import Any

from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.utils.storage_uri import resolve_local_storage_path
from langbridge.packages.common.langbridge_common.db.dataset import DatasetRecord
from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import DatasetRepository
from langbridge.packages.common.langbridge_common.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
    derive_legacy_dataset_type,
    resolve_dataset_connector_kind,
    resolve_dataset_source_kind,
    resolve_dataset_storage_kind,
)
from langbridge.packages.common.langbridge_common.utils.sql import enforce_read_only_sql
from langbridge.packages.federation.models import (
    DatasetExecutionDescriptor,
    FederationWorkflow,
    VirtualDataset,
    VirtualRelationship,
    VirtualTableBinding,
)
from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel, Table


class DatasetExecutionResolver:
    def __init__(self, *, dataset_repository: DatasetRepository | None = None) -> None:
        self._dataset_repository = dataset_repository

    async def build_workflow_for_dataset(
        self,
        *,
        dataset: DatasetRecord,
    ) -> tuple[FederationWorkflow, str, str]:
        dataset_type = str(dataset.dataset_type or "").upper()
        if dataset_type in {"TABLE", "SQL", "FILE"}:
            binding, dialect = self._build_binding_from_dataset_record(dataset=dataset)
            workflow = self._build_workflow_from_bindings(
                workflow_id=f"workflow_dataset_{dataset.id.hex[:12]}",
                workspace_id=str(dataset.workspace_id),
                dataset_id=f"dataset_{dataset.id.hex[:12]}",
                dataset_name=dataset.name,
                table_bindings={binding.table_key: binding},
                relationships=[],
            )
            return workflow, binding.table_key, dialect

        if dataset_type == "FEDERATED":
            workflow, default_table_key, dialect = await self._build_federated_dataset_workflow(dataset=dataset)
            return workflow, default_table_key, dialect

        raise BusinessValidationError(
            f"Dataset type '{dataset.dataset_type}' is not executable in this runtime yet."
        )

    async def build_semantic_workflow(
        self,
        *,
        organization_id: uuid.UUID,
        workflow_id: str,
        dataset_name: str,
        semantic_model: SemanticModel,
        connector_fallbacks: Mapping[str, uuid.UUID],
        raw_tables_payload: Mapping[str, Any] | None = None,
    ) -> tuple[FederationWorkflow, str]:
        table_bindings: dict[str, VirtualTableBinding] = {}
        dialects: list[str] = []

        for table_key, table in semantic_model.tables.items():
            raw_table = raw_tables_payload.get(table_key) if isinstance(raw_tables_payload, Mapping) else None
            dataset_ref = self._extract_dataset_ref(table=table, raw_table=raw_table)
            if dataset_ref is not None:
                dataset = await self._load_dataset(
                    workspace_id=organization_id,
                    dataset_id=dataset_ref,
                    table_key=table_key,
                )
                binding, dialect = self._build_binding_from_dataset_record(
                    dataset=dataset,
                    table_key=table_key,
                    logical_schema=(table.schema or None),
                    logical_table_name=table.name,
                    catalog_name=table.catalog,
                )
            else:
                connector_id = connector_fallbacks.get(table_key)
                if connector_id is None:
                    raise BusinessValidationError(
                        f"Missing connector fallback for semantic table '{table_key}'."
                    )
                binding, dialect = self._build_binding_from_legacy_table(
                    table_key=table_key,
                    table=table,
                    connector_id=connector_id,
                )
            table_bindings[table_key] = binding
            dialects.append(dialect)

        relationships = [
            VirtualRelationship(
                name=relationship.name,
                left_table=relationship.from_,
                right_table=relationship.to,
                join_type=relationship.type,
                condition=relationship.join_on,
            )
            for relationship in (semantic_model.relationships or [])
        ]
        workflow = self._build_workflow_from_bindings(
            workflow_id=workflow_id,
            workspace_id=str(organization_id),
            dataset_id=f"semantic_dataset_{organization_id.hex[:12]}",
            dataset_name=dataset_name,
            table_bindings=table_bindings,
            relationships=relationships,
        )
        return workflow, self._choose_workflow_dialect(dialects)

    def _build_binding_from_dataset_record(
        self,
        *,
        dataset: DatasetRecord,
        table_key: str | None = None,
        logical_schema: str | None = None,
        logical_table_name: str | None = None,
        catalog_name: str | None = None,
    ) -> tuple[VirtualTableBinding, str]:
        dataset_type = str(dataset.dataset_type or "").upper()
        logical_table = self._logical_table_name(
            dataset=dataset,
            logical_table_name=logical_table_name,
        )
        logical_schema_name = logical_schema if logical_schema is not None else dataset.schema_name
        resolved_table_key = table_key or self._table_key(
            schema_name=logical_schema_name,
            table_name=logical_table,
        )
        dialect = (dataset.dialect or "tsql").strip().lower() or "tsql"
        dataset_descriptor = self._build_dataset_execution_descriptor(dataset)

        if dataset_type == "TABLE":
            if dataset.connection_id is None:
                raise BusinessValidationError("Executable TABLE datasets require a connection_id.")
            physical_table = dataset.table_name or logical_table
            binding = VirtualTableBinding(
                table_key=resolved_table_key,
                source_id=f"source_{dataset.connection_id.hex[:12]}",
                connector_id=dataset.connection_id,
                schema=logical_schema_name,
                table=logical_table,
                catalog=catalog_name or dataset.catalog_name,
                metadata={
                    "dataset_id": str(dataset.id),
                    "source_kind": "connector",
                    "physical_catalog": dataset.catalog_name,
                    "physical_schema": dataset.schema_name,
                    "physical_table": physical_table,
                },
                dataset_descriptor=dataset_descriptor,
            )
            return binding, dialect

        if dataset_type == "SQL":
            if dataset.connection_id is None:
                raise BusinessValidationError("Executable SQL datasets require a connection_id.")
            sql_text = (dataset.sql_text or "").strip()
            if not sql_text:
                raise BusinessValidationError("SQL dataset is missing sql_text.")
            enforce_read_only_sql(sql_text, allow_dml=False, dialect=dialect)
            binding = VirtualTableBinding(
                table_key=resolved_table_key,
                source_id=f"source_{dataset.connection_id.hex[:12]}",
                connector_id=dataset.connection_id,
                schema=logical_schema_name,
                table=logical_table,
                catalog=catalog_name,
                metadata={
                    "dataset_id": str(dataset.id),
                    "source_kind": "connector",
                    "physical_sql": sql_text,
                    "sql_dialect": dialect,
                },
                dataset_descriptor=dataset_descriptor,
            )
            return binding, dialect

        if dataset_type == "FILE":
            storage_uri = self._resolve_file_storage_uri(dataset)
            file_format = self._resolve_file_format(dataset, storage_uri=storage_uri)
            file_config = dict(dataset.file_config_json or {})
            metadata: dict[str, Any] = {
                "dataset_id": str(dataset.id),
                "source_kind": "file",
                "storage_uri": storage_uri,
                "file_format": file_format,
            }
            for key in ("header", "delimiter", "quote"):
                if key in file_config:
                    metadata[key] = file_config[key]
            binding = VirtualTableBinding(
                table_key=resolved_table_key,
                source_id=f"file_{dataset.id.hex[:12]}",
                connector_id=None,
                schema=logical_schema_name,
                table=logical_table,
                catalog=None,
                metadata=metadata,
                dataset_descriptor=dataset_descriptor,
            )
            return binding, "duckdb"

        raise BusinessValidationError(
            f"Dataset type '{dataset.dataset_type}' is not supported for semantic or dataset execution."
        )

    @staticmethod
    def _build_binding_from_legacy_table(
        *,
        table_key: str,
        table: Table,
        connector_id: uuid.UUID,
    ) -> tuple[VirtualTableBinding, str]:
        return (
            VirtualTableBinding(
                table_key=table_key,
                source_id=f"source_{connector_id.hex[:12]}",
                connector_id=connector_id,
                schema=table.schema or None,
                table=table.name,
                catalog=table.catalog,
                metadata={
                    "source_kind": "connector",
                    "physical_catalog": table.catalog,
                    "physical_schema": table.schema,
                    "physical_table": table.name,
                },
            ),
            "tsql",
        )

    async def _build_federated_dataset_workflow(
        self,
        *,
        dataset: DatasetRecord,
    ) -> tuple[FederationWorkflow, str, str]:
        if self._dataset_repository is None:
            raise BusinessValidationError("Dataset repository is required for federated dataset execution.")

        plan = dataset.federated_plan_json if isinstance(dataset.federated_plan_json, Mapping) else {}
        plan_tables = self._normalize_plan_tables(plan.get("tables"))
        referenced_ids = [
            self._parse_uuid(value, context="referenced dataset id")
            for value in (dataset.referenced_dataset_ids_json or [])
            if value
        ]

        table_bindings: dict[str, VirtualTableBinding] = {}
        dialects: list[str] = []
        consumed_ids: set[uuid.UUID] = set()

        for table_key, table_entry in plan_tables.items():
            dataset_ref = self._parse_uuid(
                table_entry.get("dataset_id") or table_entry.get("datasetId"),
                context=f"table '{table_key}' dataset_id",
            )
            child_dataset = await self._load_dataset(
                workspace_id=dataset.workspace_id,
                dataset_id=dataset_ref,
                table_key=table_key,
            )
            binding, dialect = self._build_binding_from_dataset_record(
                dataset=child_dataset,
                table_key=table_key,
                logical_schema=self._string_or_none(table_entry.get("schema")),
                logical_table_name=self._string_or_none(table_entry.get("table") or table_entry.get("name")),
                catalog_name=self._string_or_none(table_entry.get("catalog")),
            )
            table_bindings[table_key] = binding
            dialects.append(dialect)
            consumed_ids.add(dataset_ref)

        for child_dataset_id in referenced_ids:
            if child_dataset_id in consumed_ids:
                continue
            child_dataset = await self._load_dataset(
                workspace_id=dataset.workspace_id,
                dataset_id=child_dataset_id,
                table_key=str(child_dataset_id),
            )
            binding, dialect = self._build_binding_from_dataset_record(dataset=child_dataset)
            table_bindings[binding.table_key] = binding
            dialects.append(dialect)

        if not table_bindings:
            raise BusinessValidationError(
                f"Federated dataset '{dataset.id}' has no executable child dataset bindings."
            )

        relationships = self._parse_relationships(plan.get("relationships"))
        workflow = self._build_workflow_from_bindings(
            workflow_id=f"workflow_dataset_{dataset.id.hex[:12]}",
            workspace_id=str(dataset.workspace_id),
            dataset_id=f"dataset_{dataset.id.hex[:12]}",
            dataset_name=dataset.name,
            table_bindings=table_bindings,
            relationships=relationships,
        )
        default_table_key = str(
            plan.get("default_table_key")
            or plan.get("defaultTableKey")
            or next(iter(table_bindings.keys()))
        )
        if default_table_key not in table_bindings:
            default_table_key = next(iter(table_bindings.keys()))
        return workflow, default_table_key, self._choose_workflow_dialect(dialects)

    async def _load_dataset(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_id: uuid.UUID,
        table_key: str,
    ) -> DatasetRecord:
        if self._dataset_repository is None:
            raise BusinessValidationError("Dataset repository is required for dataset-backed execution.")
        dataset = await self._dataset_repository.get_for_workspace(
            dataset_id=dataset_id,
            workspace_id=workspace_id,
        )
        if dataset is None:
            raise BusinessValidationError(
                f"Dataset '{dataset_id}' referenced by table '{table_key}' was not found."
            )
        return dataset

    @staticmethod
    def _build_workflow_from_bindings(
        *,
        workflow_id: str,
        workspace_id: str,
        dataset_id: str,
        dataset_name: str,
        table_bindings: Mapping[str, VirtualTableBinding],
        relationships: list[VirtualRelationship],
    ) -> FederationWorkflow:
        return FederationWorkflow(
            id=workflow_id,
            workspace_id=workspace_id,
            dataset=VirtualDataset(
                id=dataset_id,
                name=dataset_name,
                workspace_id=workspace_id,
                tables=dict(table_bindings),
                relationships=relationships,
            ),
            broadcast_threshold_bytes=settings.FEDERATION_BROADCAST_THRESHOLD_BYTES,
            partition_count=settings.FEDERATION_PARTITION_COUNT,
            max_stage_retries=settings.FEDERATION_STAGE_MAX_RETRIES,
            stage_parallelism=settings.FEDERATION_STAGE_PARALLELISM,
        )

    @staticmethod
    def _extract_dataset_ref(
        *,
        table: Table,
        raw_table: Any,
    ) -> uuid.UUID | None:
        raw_value = table.dataset_id
        if raw_value is None and isinstance(raw_table, Mapping):
            raw_value = raw_table.get("dataset_id") or raw_table.get("datasetId")
        if raw_value in {None, ""}:
            return None
        return DatasetExecutionResolver._parse_uuid(raw_value, context="semantic table dataset_id")

    @staticmethod
    def _resolve_file_storage_uri(dataset: DatasetRecord) -> str:
        file_config = dict(dataset.file_config_json or {})
        storage_uri = (
            str(dataset.storage_uri or "").strip()
            or str(file_config.get("storage_uri") or file_config.get("uri") or file_config.get("path") or "").strip()
        )
        if not storage_uri:
            raise BusinessValidationError(f"FILE dataset '{dataset.id}' is missing storage_uri.")
        return storage_uri

    @staticmethod
    def _resolve_file_format(dataset: DatasetRecord, *, storage_uri: str) -> str:
        file_config = dict(dataset.file_config_json or {})
        configured = str(file_config.get("format") or file_config.get("file_format") or "").strip().lower()
        if configured in {"csv", "parquet"}:
            return configured
        lowered_uri = storage_uri.lower()
        if lowered_uri.endswith(".parquet"):
            return "parquet"
        if lowered_uri.endswith(".csv"):
            return "csv"
        raise BusinessValidationError(
            f"FILE dataset '{dataset.id}' must declare a supported file format (csv or parquet)."
        )

    @staticmethod
    def _logical_table_name(
        *,
        dataset: DatasetRecord,
        logical_table_name: str | None,
    ) -> str:
        candidate = (logical_table_name or dataset.table_name or "").strip()
        if candidate:
            return candidate
        base_name = re.sub(r"[^a-zA-Z0-9_]+", "_", (dataset.name or "dataset").strip()).strip("_")
        return base_name.lower() or f"dataset_{dataset.id.hex[:8]}"

    @staticmethod
    def _table_key(*, schema_name: str | None, table_name: str) -> str:
        schema_value = (schema_name or "").strip()
        table_value = (table_name or "").strip()
        if schema_value:
            return f"{schema_value}.{table_value}"
        return table_value

    @staticmethod
    def _choose_workflow_dialect(dialects: list[str]) -> str:
        normalized = [str(value or "").strip().lower() for value in dialects if str(value or "").strip()]
        if not normalized:
            return "tsql"
        if any(value == "duckdb" for value in normalized):
            return "duckdb"
        return normalized[0]

    @staticmethod
    def _normalize_plan_tables(raw_tables: Any) -> dict[str, dict[str, Any]]:
        if isinstance(raw_tables, Mapping):
            return {
                str(table_key): dict(table_value)
                for table_key, table_value in raw_tables.items()
                if isinstance(table_value, Mapping)
            }
        if isinstance(raw_tables, list):
            normalized: dict[str, dict[str, Any]] = {}
            for index, table_value in enumerate(raw_tables):
                if not isinstance(table_value, Mapping):
                    continue
                table_key = str(
                    table_value.get("table_key")
                    or table_value.get("tableKey")
                    or table_value.get("name")
                    or f"dataset_{index + 1}"
                ).strip()
                if table_key:
                    normalized[table_key] = dict(table_value)
            return normalized
        return {}

    @staticmethod
    def _parse_relationships(raw_relationships: Any) -> list[VirtualRelationship]:
        relationships: list[VirtualRelationship] = []
        if not isinstance(raw_relationships, list):
            return relationships
        for item in raw_relationships:
            if not isinstance(item, Mapping):
                continue
            left_table = item.get("left_table") or item.get("leftTable")
            right_table = item.get("right_table") or item.get("rightTable")
            condition = item.get("condition") or item.get("on")
            if not left_table or not right_table or not condition:
                continue
            relationships.append(
                VirtualRelationship(
                    name=str(item.get("name") or f"{left_table}_to_{right_table}"),
                    left_table=str(left_table),
                    right_table=str(right_table),
                    join_type=str(item.get("join_type") or item.get("joinType") or item.get("type") or "inner"),
                    condition=str(condition),
                )
            )
        return relationships

    @staticmethod
    def _parse_uuid(value: Any, *, context: str) -> uuid.UUID:
        try:
            return uuid.UUID(str(value))
        except (TypeError, ValueError) as exc:
            raise BusinessValidationError(f"Invalid UUID for {context}.") from exc

    @staticmethod
    def _string_or_none(value: Any) -> str | None:
        normalized = str(value or "").strip()
        return normalized or None

    @staticmethod
    def _build_dataset_execution_descriptor(dataset: DatasetRecord) -> DatasetExecutionDescriptor:
        connector_kind = resolve_dataset_connector_kind(
            explicit_connector_kind=getattr(dataset, "connector_kind", None),
            connection_connector_type=(dataset.dialect if dataset.connection_id else None),
            file_config=dict(dataset.file_config_json or {}),
            storage_uri=dataset.storage_uri,
            legacy_dataset_type=dataset.dataset_type,
        )
        source_kind = resolve_dataset_source_kind(
            explicit_source_kind=getattr(dataset, "source_kind", None),
            legacy_dataset_type=dataset.dataset_type,
            connector_kind=connector_kind,
            file_config=dict(dataset.file_config_json or {}),
        )
        storage_kind = resolve_dataset_storage_kind(
            explicit_storage_kind=getattr(dataset, "storage_kind", None),
            legacy_dataset_type=dataset.dataset_type,
            file_config=dict(dataset.file_config_json or {}),
            storage_uri=dataset.storage_uri,
        )
        relation_identity = build_dataset_relation_identity(
            dataset_id=dataset.id,
            connector_id=dataset.connection_id,
            dataset_name=dataset.name,
            catalog_name=dataset.catalog_name,
            schema_name=dataset.schema_name,
            table_name=dataset.table_name,
            storage_uri=dataset.storage_uri,
            source_kind=source_kind,
            storage_kind=storage_kind,
            existing_payload=dict(getattr(dataset, "relation_identity_json", None) or {}),
        )
        capabilities = build_dataset_execution_capabilities(
            source_kind=source_kind,
            storage_kind=storage_kind,
            existing_payload=dict(getattr(dataset, "execution_capabilities_json", None) or {}),
        )
        return DatasetExecutionDescriptor(
            dataset_id=dataset.id,
            connector_id=dataset.connection_id,
            name=dataset.name,
            source_kind=source_kind.value,
            connector_kind=connector_kind,
            storage_kind=storage_kind.value,
            relation_identity=relation_identity.model_dump(mode="json"),
            execution_capabilities=capabilities.model_dump(mode="json"),
            legacy_dataset_type=derive_legacy_dataset_type(
                source_kind=source_kind,
                storage_kind=storage_kind,
            ),
            metadata={
                "description": dataset.description,
                "tags": list(dataset.tags_json or []),
            },
        )


def build_file_scan_sql(*, storage_uri: str, file_config: dict[str, Any] | None = None) -> str:
    config_payload = dict(file_config or {})
    normalized_uri = resolve_local_storage_path(storage_uri).as_posix().replace("'", "''")
    configured = str(
        config_payload.get("format")
        or config_payload.get("file_format")
        or ""
    ).strip().lower()
    if configured == "parquet" or normalized_uri.lower().endswith(".parquet"):
        return f"read_parquet('{normalized_uri}')"
    header = "true" if bool(config_payload.get("header", True)) else "false"
    delimiter = str(config_payload.get("delimiter") or ",").replace("'", "''")
    quote = str(config_payload.get("quote") or '\"').replace("'", "''")
    return (
        "read_csv_auto("
        f"'{normalized_uri}', "
        f"header={header}, "
        f"delim='{delimiter}', "
        f"quote='{quote}'"
        ")"
    )


def build_binding_for_dataset(
    dataset: DatasetRecord,
    *,
    table_key: str | None = None,
    logical_schema: str | None = None,
    logical_table: str | None = None,
    logical_catalog: str | None = None,
) -> tuple[VirtualTableBinding, str]:
    resolver = DatasetExecutionResolver()
    return resolver._build_binding_from_dataset_record(
        dataset=dataset,
        table_key=table_key,
        logical_schema=logical_schema,
        logical_table_name=logical_table,
        catalog_name=logical_catalog,
    )


def synthetic_file_connector_id(dataset_id: uuid.UUID) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_DNS, f"langbridge-file-dataset:{dataset_id}")
