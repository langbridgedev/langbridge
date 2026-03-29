
import re
import uuid
from collections.abc import Mapping
from typing import Any

from .errors import ExecutionRuntimeError, ExecutionValidationError
from langbridge.runtime.models.metadata import DatasetMetadata
from langbridge.runtime.ports import DatasetCatalogStore
from langbridge.runtime.providers import DatasetMetadataProvider
from langbridge.runtime.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
    derive_legacy_dataset_type,
    resolve_dataset_materialization_mode,
    resolve_dataset_connector_kind,
    resolve_dataset_source_kind,
    resolve_dataset_storage_kind,
)
from langbridge.runtime.utils.sql import enforce_read_only_sql
from langbridge.runtime.utils.storage_uri import resolve_local_storage_path
from langbridge.federation.models import (
    DatasetExecutionDescriptor,
    FederationWorkflow,
    VirtualDataset,
    VirtualRelationship,
    VirtualTableBinding,
)
from langbridge.semantic.model import Dataset as SemanticDataset, SemanticModel
from langbridge.runtime.settings import runtime_settings as settings


class DatasetExecutionResolver:
    def __init__(
        self,
        *,
        dataset_repository: DatasetCatalogStore | None = None,
        dataset_provider: DatasetMetadataProvider | None = None,
    ) -> None:
        self._dataset_repository = dataset_repository
        self._dataset_provider = dataset_provider

    async def build_workflow_for_dataset(
        self,
        *,
        dataset: DatasetMetadata,
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

        raise ExecutionValidationError(
            f"Dataset type '{dataset.dataset_type}' is not executable in this runtime yet."
        )

    async def build_semantic_workflow(
        self,
        *,
        workspace_id: uuid.UUID,
        workflow_id: str,
        dataset_name: str,
        semantic_model: SemanticModel,
        raw_datasets_payload: Mapping[str, Any] | None = None,
    ) -> tuple[FederationWorkflow, str]:
        table_bindings: dict[str, VirtualTableBinding] = {}
        dialects: list[str] = []

        for dataset_key, semantic_dataset in semantic_model.datasets.items():
            raw_dataset = (
                raw_datasets_payload.get(dataset_key)
                if isinstance(raw_datasets_payload, Mapping)
                else None
            )
            dataset_ref = self._extract_dataset_ref(
                semantic_dataset=semantic_dataset,
                raw_dataset=raw_dataset,
            )
            if dataset_ref is None:
                raise ExecutionValidationError(
                    f"Semantic dataset '{dataset_key}' must declare dataset_id for federated execution."
                )
            dataset = await self._load_dataset(
                workspace_id=workspace_id,
                dataset_id=dataset_ref,
                table_key=dataset_key,
            )
            binding, dialect = self._build_binding_from_dataset_record(
                dataset=dataset,
                table_key=dataset_key,
                logical_schema=(semantic_dataset.schema_name or None),
                logical_table_name=semantic_dataset.relation_name,
                catalog_name=semantic_dataset.catalog_name,
            )
            table_bindings[dataset_key] = binding
            dialects.append(dialect)

        relationships = [
            VirtualRelationship(
                name=relationship.name,
                left_table=relationship.source_dataset,
                right_table=relationship.target_dataset,
                join_type=relationship.type,
                condition=relationship.join_condition,
            )
            for relationship in (semantic_model.relationships or [])
        ]
        workflow = self._build_workflow_from_bindings(
            workflow_id=workflow_id,
            workspace_id=str(workspace_id),
            dataset_id=f"semantic_dataset_{workspace_id.hex[:12]}",
            dataset_name=dataset_name,
            table_bindings=table_bindings,
            relationships=relationships,
        )
        return workflow, self._choose_workflow_dialect(dialects)

    def _build_binding_from_dataset_record(
        self,
        *,
        dataset: Any,
        table_key: str | None = None,
        logical_schema: str | None = None,
        logical_table_name: str | None = None,
        catalog_name: str | None = None,
    ) -> tuple[VirtualTableBinding, str]:
        dataset_type = str(dataset.dataset_type or "").upper()
        materialization_mode = resolve_dataset_materialization_mode(
            explicit_materialization_mode=getattr(dataset, "materialization_mode", None),
            file_config=dict(dataset.file_config_json or {}),
        ).value
        logical_table = self._logical_table_name(
            dataset=dataset,
            logical_table_name=logical_table_name,
        )
        logical_schema_name = self._logical_schema_name(
            dataset=dataset,
            logical_schema=logical_schema,
        )
        resolved_table_key = table_key or self._table_key(
            schema_name=logical_schema_name,
            table_name=logical_table,
        )
        dialect = (dataset.dialect or "tsql").strip().lower() or "tsql"
        dataset_descriptor = self._build_dataset_execution_descriptor(dataset)

        if dataset_type == "TABLE":
            if dataset.connection_id is None:
                raise ExecutionValidationError("Executable TABLE datasets require a connection_id.")
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
                    "materialization_mode": materialization_mode,
                    "physical_catalog": dataset.catalog_name,
                    "physical_schema": dataset.schema_name,
                    "physical_table": physical_table,
                },
                dataset_descriptor=dataset_descriptor,
            )
            return binding, dialect

        if dataset_type == "SQL":
            if dataset.connection_id is None:
                raise ExecutionValidationError("Executable SQL datasets require a connection_id.")
            sql_text = (dataset.sql_text or "").strip()
            if not sql_text:
                raise ExecutionValidationError("SQL dataset is missing sql_text.")
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
                    "materialization_mode": materialization_mode,
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
                "materialization_mode": materialization_mode,
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

        raise ExecutionValidationError(
            f"Dataset type '{dataset.dataset_type}' is not supported for semantic or dataset execution."
        )

    async def _build_federated_dataset_workflow(
        self,
        *,
        dataset: DatasetMetadata,
    ) -> tuple[FederationWorkflow, str, str]:
        if self._dataset_provider is None and self._dataset_repository is None:
            raise ExecutionValidationError(
                "Dataset metadata provider is required for federated dataset execution."
            )

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
            raise ExecutionValidationError(
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
    ) -> Any:
        if self._dataset_provider is not None:
            dataset = await self._dataset_provider.get_dataset(
                workspace_id=workspace_id,
                dataset_id=dataset_id,
            )
        elif self._dataset_repository is not None:
            dataset = await self._dataset_repository.get_for_workspace(
                dataset_id=dataset_id,
                workspace_id=workspace_id,
            )
        else:
            raise ExecutionValidationError(
                "Dataset metadata provider is required for dataset-backed execution."
            )
        if dataset is None:
            raise ExecutionValidationError(
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
        semantic_dataset: SemanticDataset,
        raw_dataset: Any,
    ) -> uuid.UUID | None:
        raw_value = semantic_dataset.dataset_id
        if raw_value is None and isinstance(raw_dataset, Mapping):
            raw_value = raw_dataset.get("dataset_id") or raw_dataset.get("datasetId")
        if raw_value in {None, ""}:
            return None
        return DatasetExecutionResolver._parse_uuid(raw_value, context="semantic dataset dataset_id")

    @staticmethod
    def _resolve_file_storage_uri(dataset: DatasetMetadata) -> str:
        file_config = dict(dataset.file_config_json or {})
        storage_uri = (
            str(dataset.storage_uri or "").strip()
            or str(file_config.get("storage_uri") or file_config.get("uri") or file_config.get("path") or "").strip()
        )
        if not storage_uri:
            materialization_mode = resolve_dataset_materialization_mode(
                explicit_materialization_mode=getattr(dataset, "materialization_mode", None),
                file_config=file_config,
            )
            sync_meta = file_config.get("connector_sync") if isinstance(file_config.get("connector_sync"), Mapping) else {}
            if materialization_mode.value == "synced":
                resource_name = str(sync_meta.get("resource_name") or "").strip()
                connector_name = str(sync_meta.get("connector_type") or "").strip().lower() or "connector"
                resource_detail = f" resource '{resource_name}'" if resource_name else ""
                raise ExecutionValidationError(
                    f"Synced dataset '{dataset.name}' has not been populated yet. "
                    f"Run connector sync for {connector_name}{resource_detail} before querying it."
                )
            raise ExecutionValidationError(f"FILE dataset '{dataset.id}' is missing storage_uri.")
        return storage_uri

    @staticmethod
    def _resolve_file_format(dataset: DatasetMetadata, *, storage_uri: str) -> str:
        file_config = dict(dataset.file_config_json or {})
        configured = str(file_config.get("format") or file_config.get("file_format") or "").strip().lower()
        if configured in {"csv", "parquet"}:
            return configured
        lowered_uri = storage_uri.lower()
        if lowered_uri.endswith(".parquet"):
            return "parquet"
        if lowered_uri.endswith(".csv"):
            return "csv"
        raise ExecutionValidationError(
            f"FILE dataset '{dataset.id}' must declare a supported file format (csv or parquet)."
        )

    @staticmethod
    def _logical_table_name(
        *,
        dataset: DatasetMetadata,
        logical_table_name: str | None,
    ) -> str:
        candidate = (logical_table_name or dataset.table_name or "").strip()
        if candidate:
            return candidate
        base_name = re.sub(r"[^a-zA-Z0-9_]+", "_", (dataset.name or "dataset").strip()).strip("_")
        return base_name.lower() or f"dataset_{dataset.id.hex[:8]}"

    @staticmethod
    def _logical_schema_name(
        *,
        dataset: DatasetMetadata,
        logical_schema: str | None,
    ) -> str | None:
        if logical_schema is not None:
            return logical_schema
        dataset_schema = (dataset.schema_name or "").strip() or None
        if (
            str(dataset.dataset_type or "").upper() == "FILE"
            and dataset_schema == "api_connector"
        ):
            return None
        return dataset_schema

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
            left_table = (
                item.get("left_table")
                or item.get("leftTable")
                or item.get("source_dataset")
                or item.get("sourceDataset")
            )
            right_table = (
                item.get("right_table")
                or item.get("rightTable")
                or item.get("target_dataset")
                or item.get("targetDataset")
            )
            condition = item.get("condition") or item.get("on")
            if not condition:
                source_field = item.get("source_field") or item.get("sourceField")
                target_field = item.get("target_field") or item.get("targetField")
                operator = item.get("operator") or "="
                if left_table and right_table and source_field and target_field:
                    condition = f"{left_table}.{source_field} {operator} {right_table}.{target_field}"
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
            raise ExecutionValidationError(f"Invalid UUID for {context}.") from exc

    @staticmethod
    def _string_or_none(value: Any) -> str | None:
        normalized = str(value or "").strip()
        return normalized or None

    @staticmethod
    def _build_dataset_execution_descriptor(dataset: Any) -> DatasetExecutionDescriptor:
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
        materialization_mode = resolve_dataset_materialization_mode(
            explicit_materialization_mode=getattr(dataset, "materialization_mode", None),
            file_config=dict(getattr(dataset, "file_config_json", None) or {}),
        )
        return DatasetExecutionDescriptor(
            dataset_id=dataset.id,
            connector_id=dataset.connection_id,
            name=dataset.name,
            materialization_mode=materialization_mode.value,
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
    dataset: DatasetMetadata,
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
