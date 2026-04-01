
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

from langbridge.runtime.application.errors import BusinessValidationError
from langbridge.runtime.config.models import (
    LocalRuntimeDatasetConfig,
    LocalRuntimeDatasetPolicyConfig,
)
from langbridge.runtime.models import ConnectorMetadata, DatasetMetadata, DatasetPolicyMetadata
from langbridge.runtime.models.metadata import (
    DatasetMaterializationMode,
    DatasetStatus,
    DatasetSourceKind,
    DatasetStorageKind,
    DatasetType,
    LifecycleState,
    ManagementMode,
)
from langbridge.runtime.settings import runtime_settings as settings
from langbridge.runtime.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
    infer_file_storage_kind,
    resolve_dataset_materialization_mode,
)

if TYPE_CHECKING:
    from langbridge.runtime.bootstrap.configured_runtime import ConfiguredLocalRuntimeHost


def _dataset_sql_alias(name: str) -> str:
    alias = re.sub(r"[^a-z0-9_]+", "_", str(name or "").strip().lower())
    alias = re.sub(r"_+", "_", alias).strip("_")
    if not alias:
        return "dataset"
    if alias[0].isdigit():
        return f"dataset_{alias}"
    return alias


def _relation_parts(relation_name: str) -> tuple[str | None, str | None, str]:
    parts = [part.strip() for part in str(relation_name or "").split(".") if part.strip()]
    if not parts:
        raise BusinessValidationError("Dataset table source must not be empty.")
    if len(parts) == 1:
        return None, None, parts[0]
    if len(parts) == 2:
        return None, parts[0], parts[1]
    return parts[0], parts[1], parts[2]


def _merge_dataset_tags(*, existing: list[str], required: list[str]) -> list[str]:
    merged: list[str] = []
    seen: set[str] = set()
    for raw_tag in [*existing, *required]:
        tag = str(raw_tag or "").strip()
        if not tag:
            continue
        normalized = tag.lower()
        if normalized in seen:
            continue
        seen.add(normalized)
        merged.append(tag)
    return merged


@dataclass(frozen=True, slots=True)
class _DatasetSourceInput:
    table_name: str
    resource_name: str
    sql_text: str
    storage_uri: str | None
    requested_file_format: str
    header: bool | None
    delimiter: str | None
    quote: str | None

    @classmethod
    def from_config(cls, request: LocalRuntimeDatasetConfig) -> "_DatasetSourceInput":
        source_config = request.source
        storage_uri = str(source_config.storage_uri or "").strip() or None
        if storage_uri is None and source_config.path:
            storage_uri = Path(str(source_config.path)).resolve().as_uri()
        return cls(
            table_name=str(source_config.table or "").strip(),
            resource_name=str(source_config.resource or "").strip(),
            sql_text=str(source_config.sql or "").strip(),
            storage_uri=storage_uri,
            requested_file_format=str(
                source_config.format or source_config.file_format or ""
            ).strip().lower(),
            header=source_config.header,
            delimiter=source_config.delimiter,
            quote=source_config.quote,
        )

    @property
    def is_table(self) -> bool:
        return bool(self.table_name)

    @property
    def is_resource(self) -> bool:
        return bool(self.resource_name)

    @property
    def is_sql(self) -> bool:
        return bool(self.sql_text)

    @property
    def is_file(self) -> bool:
        return self.storage_uri is not None

    @property
    def requires_connector(self) -> bool:
        return self.is_table or self.is_sql or self.is_resource


@dataclass(frozen=True, slots=True)
class _DatasetDefinition:
    catalog_name: str | None
    schema_name: str | None
    table_name: str
    relation_name: str
    dataset_type: DatasetType
    sql_text: str | None
    storage_kind: DatasetStorageKind
    source_kind: DatasetSourceKind
    dialect: str
    storage_uri: str | None
    file_config: dict[str, Any] | None


class DatasetApplication:
    def __init__(self, host: "ConfiguredLocalRuntimeHost") -> None:
        self._host = host

    @staticmethod
    def _management_mode_value(value: ManagementMode | str) -> str:
        return str(getattr(value, "value", value))

    @staticmethod
    def _dataset_label(*, dataset, configured_record) -> str:
        if configured_record is not None:
            return configured_record.label
        return dataset.name

    @staticmethod
    def _dataset_semantic_model(*, configured_record) -> str | None:
        if configured_record is None:
            return None
        return configured_record.semantic_model_name

    @staticmethod
    def _sync_resource_name(dataset) -> str | None:
        file_config = dict(dataset.file_config_json or {})
        payload = file_config.get("connector_sync")
        if not isinstance(payload, Mapping):
            return None
        resource_name = str(payload.get("resource_name") or "").strip()
        return resource_name or None

    async def _sync_state_snapshot(self, *, dataset) -> dict[str, Any] | None:
        resource_name = self._sync_resource_name(dataset)
        if not resource_name or dataset.connection_id is None:
            return None
        state = await self._host._connector_sync_state_repository.get_for_resource(
            workspace_id=self._host.context.workspace_id,
            connection_id=dataset.connection_id,
            resource_name=resource_name,
        )
        if state is None:
            return {
                "status": "never_synced",
                "resource_name": resource_name,
                "last_cursor": None,
                "last_sync_at": None,
                "records_synced": 0,
                "bytes_synced": None,
            }
        return {
            "status": state.status_value,
            "resource_name": resource_name,
            "last_cursor": state.last_cursor,
            "last_sync_at": state.last_sync_at,
            "records_synced": int(state.records_synced or 0),
            "bytes_synced": state.bytes_synced,
        }

    async def list_datasets(self) -> list[dict[str, Any]]:
        async with self._host._runtime_operation_scope():
            records = await self._host._dataset_repository.list_for_workspace(
                workspace_id=self._host.context.workspace_id,
                limit=1000,
                offset=0,
            )
        items: list[dict[str, Any]] = []
        for dataset in records:
            configured_record = self._host._datasets_by_id.get(dataset.id)
            connector_name = None
            if dataset.connection_id is not None:
                connector = next(
                    (candidate for candidate in self._host._connectors.values() if candidate.id == dataset.connection_id),
                    None,
                )
                connector_name = connector.name if connector is not None else None
            sync_resource = self._sync_resource_name(dataset)
            sync_state = await self._sync_state_snapshot(dataset=dataset)
            management_mode = self._management_mode_value(dataset.management_mode)
            items.append(
                {
                    "id": dataset.id,
                    "name": dataset.name,
                    "label": self._dataset_label(dataset=dataset, configured_record=configured_record),
                    "description": dataset.description,
                    "connector": connector_name,
                    "semantic_model": self._dataset_semantic_model(configured_record=configured_record),
                    "materialization_mode": resolve_dataset_materialization_mode(
                        explicit_materialization_mode=dataset.materialization_mode_value,
                    ).value,
                    "status": dataset.status_value,
                    "sync_resource": sync_resource,
                    "sync_status": None if sync_state is None else sync_state["status"],
                    "last_sync_at": None if sync_state is None else sync_state["last_sync_at"],
                    "management_mode": management_mode,
                    "managed": management_mode == ManagementMode.CONFIG_MANAGED.value,
                }
            )
        return items

    async def get_dataset(
        self,
        *,
        dataset_ref: str,
    ) -> dict[str, Any]:
        async with self._host._runtime_operation_scope():
            dataset = await self._host._resolve_dataset_record(dataset_ref)
            configured_record = self._host._datasets_by_id.get(dataset.id)
            connector = self._host._connector_for_id(dataset.connection_id)
            columns = await self._host._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
            policy = await self._host._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
            sync_state = await self._sync_state_snapshot(dataset=dataset)
        management_mode = self._management_mode_value(dataset.management_mode)
        return {
            "id": dataset.id,
            "name": dataset.name,
            "label": self._dataset_label(dataset=dataset, configured_record=configured_record),
            "description": dataset.description,
            "sql_alias": dataset.sql_alias,
            "connector": connector.name if connector is not None else None,
            "connector_id": connector.id if connector is not None else None,
            "semantic_model": self._dataset_semantic_model(configured_record=configured_record),
            "dataset_type": dataset.dataset_type_value,
            "materialization_mode": resolve_dataset_materialization_mode(
                explicit_materialization_mode=dataset.materialization_mode_value,
            ).value,
            "source_kind": dataset.source_kind_value,
            "storage_kind": dataset.storage_kind_value,
            "table_name": dataset.table_name,
            "storage_uri": dataset.storage_uri,
            "sql_text": dataset.sql_text,
            "file_config": dataset.file_config_json,
            "dialect": dataset.dialect,
            "status": dataset.status_value,
            "tags": list(dataset.tags_json or []),
            "management_mode": management_mode,
            "managed": management_mode == ManagementMode.CONFIG_MANAGED.value,
            "sync_resource": self._sync_resource_name(dataset),
            "sync_state": sync_state,
            "relation_identity": dataset.relation_identity_json,
            "execution_capabilities": dataset.execution_capabilities_json,
            "columns": [
                {
                    "id": column.id,
                    "name": column.name,
                    "data_type": column.data_type,
                    "nullable": bool(column.nullable),
                    "description": column.description,
                    "is_computed": bool(column.is_computed),
                    "expression": column.expression,
                    "ordinal_position": column.ordinal_position,
                }
                for column in columns
            ],
            "policy": (
                {
                    "max_rows_preview": policy.max_rows_preview,
                    "max_export_rows": policy.max_export_rows,
                    "redaction_rules": dict(policy.redaction_rules_json),
                    "row_filters": list(policy.row_filters_json),
                    "allow_dml": bool(policy.allow_dml),
                }
                if policy is not None
                else None
            ),
            "created_at": dataset.created_at,
            "updated_at": dataset.updated_at,
        }

    async def _create_dataset_revision_and_lineage(
        self,
        *,
        dataset: DatasetMetadata,
        policy: DatasetPolicyMetadata,
        actor_id: uuid.UUID,
        change_summary: str | None = None,
    ) -> None:
        summary = change_summary or f"Runtime dataset '{dataset.name}' created."
        if (
            dataset.materialization_mode_value == DatasetMaterializationMode.SYNCED.value
        ):
            await self._host.services.dataset_sync._create_dataset_revision(
                dataset=dataset,
                policy=policy,
                created_by=actor_id,
                change_summary=summary,
            )
            await self._host.services.dataset_sync._replace_dataset_lineage(dataset=dataset)
            return

        await self._host.services.dataset_query._create_dataset_revision(
            dataset=dataset,
            policy=policy,
            created_by=actor_id,
            change_summary=summary,
        )
        await self._host.services.dataset_query._replace_dataset_lineage(dataset)
    @staticmethod
    def _require_runtime_managed_dataset(dataset: DatasetMetadata) -> None:
        management_mode = str(getattr(dataset.management_mode, "value", dataset.management_mode)).lower()
        if management_mode != ManagementMode.RUNTIME_MANAGED.value:
            raise BusinessValidationError(
                f"Dataset '{dataset.name}' is config_managed and read-only in the runtime UI."
            )

    def _source_request_from_dataset(self, *, dataset: DatasetMetadata) -> dict[str, Any]:
        sync_resource = self._sync_resource_name(dataset)
        if sync_resource:
            return {"resource": sync_resource}
        if dataset.sql_text:
            return {"sql": dataset.sql_text}
        if dataset.storage_uri:
            file_config = dict(dataset.file_config_json or {})
            payload = {
                "storage_uri": dataset.storage_uri,
                "format": file_config.get("format") or file_config.get("file_format"),
            }
            for key in ("header", "delimiter", "quote"):
                if key in file_config:
                    payload[key] = file_config.get(key)
            return payload
        relation_parts = [part for part in [dataset.catalog_name, dataset.schema_name, dataset.table_name] if part]
        relation_name = ".".join(str(part) for part in relation_parts)
        if not relation_name:
            relation_name = str(dataset.table_name or "").strip()
        return {"table": relation_name}

    @staticmethod
    def _policy_request_from_dataset(policy: DatasetPolicyMetadata | None) -> dict[str, Any]:
        if policy is None:
            return {}
        return {
            "max_rows_preview": policy.max_rows_preview,
            "max_export_rows": policy.max_export_rows,
            "redaction_rules": dict(policy.redaction_rules_json),
            "row_filters": list(policy.row_filters_json),
            "allow_dml": bool(policy.allow_dml),
        }

    @staticmethod
    def _normalize_dataset_request(request) -> LocalRuntimeDatasetConfig:
        return LocalRuntimeDatasetConfig.model_validate(request.model_dump(mode="json"))

    @staticmethod
    def _normalize_dataset_name(name: str | None) -> str:
        return str(name or "").strip()

    @staticmethod
    def _dataset_description(
        *,
        description: str | None,
        materialization_mode: DatasetMaterializationMode,
        sync_resource_name: str,
    ) -> str | None:
        if description:
            return description
        if materialization_mode == DatasetMaterializationMode.SYNCED:
            return (
                "Runtime-managed synced dataset awaiting connector sync for resource "
                f"'{sync_resource_name}'."
            )
        return None

    @staticmethod
    def _dataset_tags(
        *,
        existing: list[str],
        materialization_mode: DatasetMaterializationMode,
        connector: ConnectorMetadata | None,
        sync_resource_name: str,
    ) -> list[str]:
        required_tags: list[str] = []
        if materialization_mode == DatasetMaterializationMode.SYNCED and connector is not None:
            required_tags = [
                "managed",
                "api-connector",
                str(
                    connector.connector_type.value
                    if connector.connector_type is not None
                    else ""
                ).strip().lower(),
                f"resource:{sync_resource_name.strip().lower()}",
            ]
        return _merge_dataset_tags(existing=existing, required=required_tags)

    @staticmethod
    def _runtime_dataset_label(name: str) -> str:
        return name.replace("_", " ").title()

    @staticmethod
    def _runtime_record_connector_name(connector: ConnectorMetadata | None) -> str | None:
        return connector.name if connector is not None else None

    @staticmethod
    def _require_storage_uri(*, dataset_name: str, source: _DatasetSourceInput) -> str:
        if source.storage_uri:
            return source.storage_uri
        raise BusinessValidationError(
            f"Dataset '{dataset_name}' must define source.path or source.storage_uri for file-backed datasets."
        )

    def _resolve_dataset_connector(
        self,
        *,
        dataset_name: str,
        connector_name: str | None,
        materialization_mode: DatasetMaterializationMode,
        source: _DatasetSourceInput,
        existing_connector: ConnectorMetadata | None = None,
    ) -> ConnectorMetadata | None:
        normalized_connector_name = str(connector_name or "").strip() or None
        if normalized_connector_name:
            return self._host._resolve_connector(normalized_connector_name)
        if existing_connector is not None:
            return existing_connector
        if materialization_mode == DatasetMaterializationMode.SYNCED or source.requires_connector:
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requires a connector for table, sql, or synced sources."
            )
        return None

    @staticmethod
    def _require_connector(
        *,
        dataset_name: str,
        connector: ConnectorMetadata | None,
    ) -> ConnectorMetadata:
        if connector is not None:
            return connector
        raise BusinessValidationError(
            f"Dataset '{dataset_name}' requires a connector for table, sql, or synced sources."
        )

    def _validate_synced_dataset(
        self,
        *,
        dataset_name: str,
        connector: ConnectorMetadata | None,
        source: _DatasetSourceInput,
    ) -> ConnectorMetadata:
        resolved_connector = self._require_connector(dataset_name=dataset_name, connector=connector)
        connector_capabilities = self._host._connector_capabilities(resolved_connector)
        if not connector_capabilities.supports_synced_datasets:
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requests materialization_mode 'synced', "
                f"but connector '{resolved_connector.name}' does not support synced datasets."
            )
        plugin = self._host._resolve_connector_plugin_for_type(resolved_connector.connector_type_value)
        if plugin is None or plugin.api_connector_class is None:
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requests materialization_mode 'synced', "
                f"but connector '{resolved_connector.name}' does not expose a runtime sync path yet."
            )
        if not source.resource_name:
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requests materialization_mode 'synced', "
                "but is missing source.resource for the connector resource name."
            )
        supported_resources = {
            str(item or "").strip()
            for item in (resolved_connector.supported_resources or [])
            if str(item or "").strip()
        }
        if supported_resources and source.resource_name not in supported_resources:
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requests synced resource '{source.resource_name}', "
                f"but connector '{resolved_connector.name}' only exposes: {', '.join(sorted(supported_resources))}."
            )
        return resolved_connector

    def _validate_dataset_mutation(
        self,
        *,
        dataset_name: str,
        materialization_mode: DatasetMaterializationMode,
        connector: ConnectorMetadata | None,
        source: _DatasetSourceInput,
    ) -> ConnectorMetadata | None:
        if materialization_mode == DatasetMaterializationMode.SYNCED:
            return self._validate_synced_dataset(
                dataset_name=dataset_name,
                connector=connector,
                source=source,
            )
        if not source.requires_connector:
            return connector
        resolved_connector = self._require_connector(dataset_name=dataset_name, connector=connector)
        connector_capabilities = self._host._connector_capabilities(resolved_connector)
        if (
            materialization_mode == DatasetMaterializationMode.LIVE
            and not connector_capabilities.supports_live_datasets
        ):
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requests materialization_mode 'live', "
                f"but connector '{resolved_connector.name}' does not support live datasets."
            )
        if (
            materialization_mode == DatasetMaterializationMode.LIVE
            and (source.is_table or source.is_sql)
            and not connector_capabilities.supports_query_pushdown
        ):
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' uses a live table/sql source, "
                f"but connector '{resolved_connector.name}' does not expose live query pushdown."
            )
        return resolved_connector

    def _resolve_file_format(
        self,
        *,
        dataset_name: str,
        source: _DatasetSourceInput,
        connector: ConnectorMetadata | None,
        storage_uri: str,
    ) -> str:
        connector_config = ((connector.config or {}).get("config") or {}) if connector is not None else {}
        file_format = (
            source.requested_file_format
            or str(connector_config.get("format") or connector_config.get("file_format") or "").strip().lower()
            or infer_file_storage_kind(file_config=None, storage_uri=storage_uri).value
        )
        if file_format not in {"csv", "parquet"}:
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' must declare a supported file format (csv or parquet)."
            )
        return file_format

    @staticmethod
    def _file_config_from_source(
        *,
        file_format: str,
        source: _DatasetSourceInput,
    ) -> dict[str, Any]:
        file_config: dict[str, Any] = {"format": file_format}
        if source.header is not None:
            file_config["header"] = source.header
        if source.delimiter is not None:
            file_config["delimiter"] = source.delimiter
        if source.quote is not None:
            file_config["quote"] = source.quote
        return file_config

    @staticmethod
    def _synced_file_config(
        *,
        connector: ConnectorMetadata,
        resource_name: str,
    ) -> dict[str, Any]:
        return {
            "format": "parquet",
            "managed_dataset": True,
            "connector_sync": {
                "connector_id": str(connector.id),
                "connector_type": connector.connector_type_value,
                "connector_family": connector.connector_family_value,
                "resource_name": resource_name,
                "root_resource_name": resource_name,
                "parent_resource_name": None,
            },
        }

    def _build_dataset_definition(
        self,
        *,
        dataset_name: str,
        sql_alias: str,
        materialization_mode: DatasetMaterializationMode,
        connector: ConnectorMetadata | None,
        source: _DatasetSourceInput,
    ) -> _DatasetDefinition:
        if materialization_mode == DatasetMaterializationMode.SYNCED:
            resolved_connector = self._require_connector(dataset_name=dataset_name, connector=connector)
            return _DatasetDefinition(
                catalog_name=None,
                schema_name=None,
                table_name=sql_alias,
                relation_name=sql_alias,
                dataset_type=DatasetType.FILE,
                sql_text=None,
                storage_kind=DatasetStorageKind.PARQUET,
                source_kind=DatasetSourceKind.API,
                dialect="duckdb",
                storage_uri=None,
                file_config=self._synced_file_config(
                    connector=resolved_connector,
                    resource_name=source.resource_name,
                ),
            )
        if source.is_table:
            resolved_connector = self._require_connector(dataset_name=dataset_name, connector=connector)
            catalog_name, schema_name, table_name = _relation_parts(source.table_name)
            return _DatasetDefinition(
                catalog_name=catalog_name,
                schema_name=schema_name,
                table_name=table_name,
                relation_name=source.table_name,
                dataset_type=DatasetType.TABLE,
                sql_text=None,
                storage_kind=DatasetStorageKind.TABLE,
                source_kind=DatasetSourceKind.DATABASE,
                dialect=self._host._connector_dialect(resolved_connector.connector_type or ""),
                storage_uri=None,
                file_config=None,
            )
        if source.is_sql:
            resolved_connector = self._require_connector(dataset_name=dataset_name, connector=connector)
            return _DatasetDefinition(
                catalog_name=None,
                schema_name=None,
                table_name=sql_alias,
                relation_name=sql_alias,
                dataset_type=DatasetType.SQL,
                sql_text=source.sql_text,
                storage_kind=DatasetStorageKind.VIEW,
                source_kind=DatasetSourceKind.DATABASE,
                dialect=self._host._connector_dialect(resolved_connector.connector_type or ""),
                storage_uri=None,
                file_config=None,
            )
        storage_uri = self._require_storage_uri(dataset_name=dataset_name, source=source)
        file_format = self._resolve_file_format(
            dataset_name=dataset_name,
            source=source,
            connector=connector,
            storage_uri=storage_uri,
        )
        return _DatasetDefinition(
            catalog_name=None,
            schema_name=None,
            table_name=sql_alias,
            relation_name=sql_alias,
            dataset_type=DatasetType.FILE,
            sql_text=None,
            storage_kind=DatasetStorageKind(file_format),
            source_kind=DatasetSourceKind.FILE,
            dialect="duckdb",
            storage_uri=storage_uri,
            file_config=self._file_config_from_source(file_format=file_format, source=source),
        )

    def _build_relation_identity(
        self,
        *,
        dataset_id: uuid.UUID,
        dataset_name: str,
        connector: ConnectorMetadata | None,
        definition: _DatasetDefinition,
    ):
        return build_dataset_relation_identity(
            dataset_id=dataset_id,
            connector_id=None if connector is None else connector.id,
            dataset_name=dataset_name,
            catalog_name=definition.catalog_name,
            schema_name=definition.schema_name,
            table_name=definition.table_name,
            storage_uri=definition.storage_uri,
            source_kind=definition.source_kind,
            storage_kind=definition.storage_kind,
        )

    @staticmethod
    def _build_execution_capabilities(*, definition: _DatasetDefinition):
        return build_dataset_execution_capabilities(
            source_kind=definition.source_kind,
            storage_kind=definition.storage_kind,
        )

    def _build_dataset_policy(
        self,
        *,
        dataset_id: uuid.UUID,
        policy_config: LocalRuntimeDatasetPolicyConfig | None,
        now: datetime,
        existing_policy: DatasetPolicyMetadata | None = None,
    ) -> DatasetPolicyMetadata:
        resolved_policy_config = policy_config or LocalRuntimeDatasetPolicyConfig()
        policy = (
            existing_policy
            if existing_policy is not None
            else DatasetPolicyMetadata(
                id=uuid.uuid4(),
                dataset_id=dataset_id,
                workspace_id=self._host.context.workspace_id,
                created_at=now,
                updated_at=now,
            )
        )
        policy.max_rows_preview = (
            resolved_policy_config.max_rows_preview or settings.SQL_DEFAULT_MAX_PREVIEW_ROWS
        )
        policy.max_export_rows = (
            resolved_policy_config.max_export_rows or settings.SQL_DEFAULT_MAX_EXPORT_ROWS
        )
        policy.redaction_rules = dict(resolved_policy_config.redaction_rules or {})
        policy.row_filters = list(resolved_policy_config.row_filters or [])
        policy.allow_dml = bool(resolved_policy_config.allow_dml)
        policy.updated_at = now
        return policy

    async def _assert_dataset_name_is_available(
        self,
        *,
        dataset_name: str,
        dataset_alias: str,
    ) -> None:
        existing = await self._host._dataset_repository.list_for_workspace(
            workspace_id=self._host.context.workspace_id,
            limit=1000,
            offset=0,
        )
        if any(candidate.name == dataset_name for candidate in existing):
            raise BusinessValidationError(f"Dataset '{dataset_name}' already exists.")
        existing_alias = await self._host._dataset_repository.get_for_workspace_by_sql_alias(
            workspace_id=self._host.context.workspace_id,
            sql_alias=dataset_alias,
        )
        if existing_alias is not None:
            raise BusinessValidationError(
                f"Dataset sql_alias '{dataset_alias}' is already in use by dataset '{existing_alias.name}'."
            )

    def _upsert_runtime_dataset_record(
        self,
        *,
        dataset: DatasetMetadata,
        connector: ConnectorMetadata | None,
        relation_name: str,
    ) -> None:
        from langbridge.runtime.bootstrap.configured_runtime import LocalRuntimeDatasetRecord

        self._host._upsert_runtime_dataset_record(
            LocalRuntimeDatasetRecord(
                id=dataset.id,
                name=dataset.name,
                label=self._runtime_dataset_label(dataset.name),
                description=dataset.description,
                connector_name=self._runtime_record_connector_name(connector),
                relation_name=relation_name,
                semantic_model_name=None,
                default_time_dimension=None,
            )
        )

    async def create_dataset(self, *, request) -> dict[str, Any]:
        normalized_request = self._normalize_dataset_request(request)
        dataset_name = self._normalize_dataset_name(normalized_request.name)
        if not dataset_name:
            raise BusinessValidationError("Dataset name is required.")

        materialization_mode = resolve_dataset_materialization_mode(
            explicit_materialization_mode=normalized_request.materialization_mode,
        )
        source = _DatasetSourceInput.from_config(normalized_request)
        connector = self._resolve_dataset_connector(
            dataset_name=dataset_name,
            connector_name=normalized_request.connector,
            materialization_mode=materialization_mode,
            source=source,
        )
        connector = self._validate_dataset_mutation(
            dataset_name=dataset_name,
            materialization_mode=materialization_mode,
            connector=connector,
            source=source,
        )

        dataset_id = uuid.uuid4()
        dataset_alias = _dataset_sql_alias(dataset_name)
        definition = self._build_dataset_definition(
            dataset_name=dataset_name,
            sql_alias=dataset_alias,
            materialization_mode=materialization_mode,
            connector=connector,
            source=source,
        )
        actor_id = self._host._resolve_actor_id()
        now = datetime.now(timezone.utc)
        relation_identity = self._build_relation_identity(
            dataset_id=dataset_id,
            dataset_name=dataset_name,
            connector=connector,
            definition=definition,
        )
        execution_capabilities = self._build_execution_capabilities(definition=definition)
        policy = self._build_dataset_policy(
            dataset_id=dataset_id,
            policy_config=normalized_request.policy,
            now=now,
        )

        dataset = DatasetMetadata(
            id=dataset_id,
            workspace_id=self._host.context.workspace_id,
            connection_id=None if connector is None else connector.id,
            owner_id=actor_id,
            created_by=actor_id,
            updated_by=actor_id,
            name=dataset_name,
            sql_alias=dataset_alias,
            description=self._dataset_description(
                description=normalized_request.description,
                materialization_mode=materialization_mode,
                sync_resource_name=source.resource_name,
            ),
            tags=self._dataset_tags(
                existing=list(normalized_request.tags or []),
                materialization_mode=materialization_mode,
                connector=connector,
                sync_resource_name=source.resource_name,
            ),
            dataset_type=definition.dataset_type,
            materialization_mode=materialization_mode,
            source_kind=definition.source_kind,
            connector_kind=(
                connector.connector_type.value.lower()
                if connector is not None and connector.connector_type is not None
                else None
            ),
            storage_kind=definition.storage_kind,
            dialect=definition.dialect,
            catalog_name=definition.catalog_name,
            schema_name=definition.schema_name,
            table_name=definition.table_name,
            storage_uri=definition.storage_uri,
            sql_text=definition.sql_text,
            relation_identity=relation_identity.model_dump(mode="json"),
            execution_capabilities=execution_capabilities.model_dump(mode="json"),
            referenced_dataset_ids=[],
            federated_plan=None,
            file_config=definition.file_config,
            status=(
                DatasetStatus.PENDING_SYNC
                if materialization_mode == DatasetMaterializationMode.SYNCED
                else DatasetStatus.PUBLISHED
            ),
            revision_id=None,
            row_count_estimate=None,
            bytes_estimate=None,
            last_profiled_at=None,
            columns=[],
            policy=policy,
            created_at=now,
            updated_at=now,
            management_mode=ManagementMode.RUNTIME_MANAGED,
            lifecycle_state=LifecycleState.ACTIVE,
        )

        async with self._host._runtime_operation_scope() as uow:
            await self._assert_dataset_name_is_available(
                dataset_name=dataset_name,
                dataset_alias=dataset_alias,
            )
            dataset = self._host._dataset_repository.add(dataset)
            policy = self._host._dataset_policy_repository.add(policy)
            await self._create_dataset_revision_and_lineage(
                dataset=dataset,
                policy=policy,
                actor_id=actor_id,
            )
            if uow is not None:
                await uow.commit()

        self._upsert_runtime_dataset_record(
            dataset=dataset,
            connector=connector,
            relation_name=definition.relation_name,
        )
        return await self.get_dataset(dataset_ref=str(dataset.id))

    async def update_dataset(self, *, dataset_ref: str, request) -> dict[str, Any]:
        async with self._host._runtime_operation_scope() as uow:
            dataset = await self._host._resolve_dataset_record(dataset_ref)
            self._require_runtime_managed_dataset(dataset)
            existing_policy = await self._host._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
            connector = self._host._connector_for_id(dataset.connection_id)

            fields_set = set(getattr(request, "model_fields_set", set()))
            payload = {
                "name": dataset.name,
                "description": (
                    request.description
                    if "description" in fields_set
                    else dataset.description
                ),
                "connector": None if connector is None else connector.name,
                "materialization_mode": (
                    request.materialization_mode
                    if "materialization_mode" in fields_set and request.materialization_mode is not None
                    else dataset.materialization_mode_value
                ),
                "source": (
                    request.source.model_dump(mode="json")
                    if "source" in fields_set and request.source is not None
                    else self._source_request_from_dataset(dataset=dataset)
                ),
                "tags": (
                    list(request.tags or [])
                    if "tags" in fields_set and request.tags is not None
                    else list(dataset.tags_json or [])
                ),
                "policy": (
                    request.policy.model_dump(mode="json")
                    if "policy" in fields_set and request.policy is not None
                    else self._policy_request_from_dataset(existing_policy)
                ),
            }
            normalized_request = LocalRuntimeDatasetConfig.model_validate(payload)
            materialization_mode = resolve_dataset_materialization_mode(
                explicit_materialization_mode=normalized_request.materialization_mode,
            )
            source = _DatasetSourceInput.from_config(normalized_request)
            connector = self._resolve_dataset_connector(
                dataset_name=dataset.name,
                connector_name=normalized_request.connector,
                materialization_mode=materialization_mode,
                source=source,
                existing_connector=connector,
            )
            connector = self._validate_dataset_mutation(
                dataset_name=dataset.name,
                materialization_mode=materialization_mode,
                connector=connector,
                source=source,
            )
            actor_id = self._host._resolve_actor_id()
            now = datetime.now(timezone.utc)
            definition = self._build_dataset_definition(
                dataset_name=dataset.name,
                sql_alias=dataset.sql_alias,
                materialization_mode=materialization_mode,
                connector=connector,
                source=source,
            )
            relation_identity = self._build_relation_identity(
                dataset_id=dataset.id,
                dataset_name=dataset.name,
                connector=connector,
                definition=definition,
            )
            execution_capabilities = self._build_execution_capabilities(definition=definition)
            policy = self._build_dataset_policy(
                dataset_id=dataset.id,
                policy_config=normalized_request.policy,
                now=now,
                existing_policy=existing_policy,
            )
            dataset.description = self._dataset_description(
                description=normalized_request.description,
                materialization_mode=materialization_mode,
                sync_resource_name=source.resource_name,
            )
            dataset.connection_id = None if connector is None else connector.id
            dataset.updated_by = actor_id
            dataset.tags = self._dataset_tags(
                existing=list(normalized_request.tags or []),
                materialization_mode=materialization_mode,
                connector=connector,
                sync_resource_name=source.resource_name,
            )
            dataset.dataset_type = definition.dataset_type
            dataset.materialization_mode = materialization_mode
            dataset.source_kind = definition.source_kind
            dataset.connector_kind = (
                connector.connector_type.value.lower()
                if connector is not None and connector.connector_type is not None
                else None
            )
            dataset.storage_kind = definition.storage_kind
            dataset.dialect = definition.dialect
            dataset.catalog_name = definition.catalog_name
            dataset.schema_name = definition.schema_name
            dataset.table_name = definition.table_name
            dataset.storage_uri = definition.storage_uri
            dataset.sql_text = definition.sql_text
            dataset.relation_identity = relation_identity.model_dump(mode="json")
            dataset.execution_capabilities = execution_capabilities.model_dump(mode="json")
            dataset.referenced_dataset_ids = []
            dataset.federated_plan = None
            dataset.file_config = definition.file_config
            dataset.status = (
                DatasetStatus.PENDING_SYNC
                if materialization_mode == DatasetMaterializationMode.SYNCED
                else DatasetStatus.PUBLISHED
            )
            dataset.row_count_estimate = None if materialization_mode == DatasetMaterializationMode.SYNCED else dataset.row_count_estimate
            dataset.bytes_estimate = None if materialization_mode == DatasetMaterializationMode.SYNCED else dataset.bytes_estimate
            dataset.last_profiled_at = None if materialization_mode == DatasetMaterializationMode.SYNCED else dataset.last_profiled_at
            dataset.updated_at = now

            await self._host._dataset_repository.save(dataset)
            if existing_policy is None:
                self._host._dataset_policy_repository.add(policy)
            else:
                await self._host._dataset_policy_repository.save(policy)
            await self._host._dataset_column_repository.delete_for_dataset(dataset_id=dataset.id)
            await self._create_dataset_revision_and_lineage(
                dataset=dataset,
                policy=policy,
                actor_id=actor_id,
                change_summary=f"Runtime dataset '{dataset.name}' updated.",
            )
            if uow is not None:
                await uow.commit()

        self._upsert_runtime_dataset_record(
            dataset=dataset,
            connector=connector,
            relation_name=definition.relation_name,
        )
        return await self.get_dataset(dataset_ref=str(dataset.id))

    async def delete_dataset(self, *, dataset_ref: str) -> dict[str, Any]:
        async with self._host._runtime_operation_scope() as uow:
            dataset = await self._host._resolve_dataset_record(dataset_ref)
            self._require_runtime_managed_dataset(dataset)
            await self._host._lineage_edge_repository.delete_for_node(
                workspace_id=self._host.context.workspace_id,
                node_type="dataset",
                node_id=str(dataset.id),
            )
            await self._host._dataset_repository.delete(dataset)
            if uow is not None:
                await uow.commit()

        self._host._remove_runtime_dataset_record(
            dataset_name=dataset.name,
            dataset_id=dataset.id,
        )
        return {"ok": True, "deleted": True, "id": dataset.id, "name": dataset.name}

    async def query_dataset(self, *, request) -> dict[str, Any]:
        async with self._host._runtime_operation_scope() as uow:
            payload = await self._host._runtime_host.query_dataset(request=request)
            if uow is not None:
                await uow.commit()
            return self._host._normalize_dataset_query_payload(payload)
