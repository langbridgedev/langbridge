
import inspect
import re
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

from langbridge.connectors.base import ApiResource, SqlConnectorFactory
from langbridge.connectors.base.config import ConnectorSyncStrategy
from langbridge.connectors.base.resource_paths import (
    api_resource_root,
    normalize_api_flatten_paths,
    normalize_api_resource_path,
)
from langbridge.runtime.application.errors import BusinessValidationError
from langbridge.runtime.config.models import (
    LocalRuntimeDatasetConfig,
    LocalRuntimeDatasetPolicyConfig,
)
from langbridge.runtime.models import (
    ConnectorMetadata,
    DatasetColumnMetadata,
    DatasetMaterializationConfig,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetSchemaHint,
    DatasetSource,
    DatasetSyncConfig,
)
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
from langbridge.runtime.models.state import ConnectorSyncMode, ConnectorSyncStatus
from langbridge.runtime.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
    infer_file_storage_kind,
    resolve_dataset_materialization_mode,
)
from langbridge.runtime.utils.lineage import stable_payload_hash
from langbridge.runtime.services.dataset_execution import describe_file_source_schema

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
    request_config: dict[str, Any] | None
    flatten_paths: list[str]
    extraction_config: dict[str, Any] | None
    sql_text: str
    storage_uri: str | None
    requested_file_format: str
    header: bool | None
    delimiter: str | None
    quote: str | None

    @classmethod
    def from_source_config(cls, source_config: Any | None) -> "_DatasetSourceInput":
        if source_config is None:
            return cls(
                table_name="",
                resource_name="",
                request_config=None,
                flatten_paths=[],
                extraction_config=None,
                sql_text="",
                storage_uri=None,
                requested_file_format="",
                header=None,
                delimiter=None,
                quote=None,
            )
        source_path = getattr(source_config, "path", None)
        storage_uri = str(getattr(source_config, "storage_uri", None) or "").strip() or None
        if storage_uri is None and source_path:
            storage_uri = Path(str(source_path)).resolve().as_uri()
        request_config = getattr(source_config, "request", None)
        request_payload = None
        if request_config is not None:
            request_payload = (
                request_config.model_dump(mode="json", exclude_none=True)
                if hasattr(request_config, "model_dump")
                else dict(request_config)
            )
            request_path = str(request_payload.get("path") or "").strip()
            if request_path:
                request_payload["path"] = request_path
        extraction_config = getattr(source_config, "extraction", None)
        extraction_payload = None
        if extraction_config is not None:
            extraction_payload = (
                extraction_config.model_dump(mode="json", exclude_none=True)
                if hasattr(extraction_config, "model_dump")
                else dict(extraction_config)
            )
        return cls(
            table_name=str(getattr(source_config, "table", None) or "").strip(),
            resource_name=(
                normalize_api_resource_path(str(getattr(source_config, "resource", None) or "").strip())
                if str(getattr(source_config, "resource", None) or "").strip()
                else ""
            ),
            request_config=request_payload,
            flatten_paths=normalize_api_flatten_paths(getattr(source_config, "flatten", None)),
            extraction_config=extraction_payload,
            sql_text=str(getattr(source_config, "sql", None) or "").strip(),
            storage_uri=storage_uri,
            requested_file_format=str(
                getattr(source_config, "format", None) or getattr(source_config, "file_format", None) or ""
            ).strip().lower(),
            header=getattr(source_config, "header", None),
            delimiter=getattr(source_config, "delimiter", None),
            quote=getattr(source_config, "quote", None),
        )

    @classmethod
    def from_config(cls, request: LocalRuntimeDatasetConfig) -> "_DatasetSourceInput":
        return cls.from_source_config(request.source)

    @property
    def is_table(self) -> bool:
        return bool(self.table_name)

    @property
    def is_resource(self) -> bool:
        return bool(self.resource_name)

    @property
    def is_request(self) -> bool:
        return isinstance(self.request_config, dict) and bool(str(self.request_config.get("path") or "").strip())

    @property
    def api_identity(self) -> str:
        if self.is_resource:
            return self.resource_name
        if self.is_request:
            request_path = str(self.request_config.get("path") or "").strip()
            if request_path:
                return request_path
            return f"request:{stable_payload_hash(str(self.request_config))}"
        return ""

    @property
    def is_sql(self) -> bool:
        return bool(self.sql_text)

    @property
    def is_file(self) -> bool:
        return self.storage_uri is not None

    @property
    def requires_connector(self) -> bool:
        return self.is_table or self.is_sql or self.is_resource or self.is_request


@dataclass(frozen=True, slots=True)
class _DatasetSyncInput:
    source: _DatasetSourceInput
    strategy: ConnectorSyncStrategy | None
    cadence: str | None
    sync_on_start: bool
    cursor_field: str | None
    initial_cursor: str | None
    lookback_window: str | None
    backfill_start: str | None
    backfill_end: str | None

    @classmethod
    def from_config(cls, request: LocalRuntimeDatasetConfig) -> "_DatasetSyncInput":
        sync_config = request.sync
        if sync_config is None:
            return cls(
                source=_DatasetSourceInput.from_source_config(None),
                strategy=None,
                cadence=None,
                sync_on_start=False,
                cursor_field=None,
                initial_cursor=None,
                lookback_window=None,
                backfill_start=None,
                backfill_end=None,
            )
        return cls(
            source=_DatasetSourceInput.from_source_config(sync_config.source),
            strategy=sync_config.strategy,
            cadence=str(sync_config.cadence or "").strip() or None,
            sync_on_start=bool(sync_config.sync_on_start),
            cursor_field=str(sync_config.cursor_field or "").strip() or None,
            initial_cursor=str(sync_config.initial_cursor or "").strip() or None,
            lookback_window=str(sync_config.lookback_window or "").strip() or None,
            backfill_start=str(sync_config.backfill_start or "").strip() or None,
            backfill_end=str(sync_config.backfill_end or "").strip() or None,
        )

    @property
    def has_sync(self) -> bool:
        source = self.source
        return source.is_table or source.is_resource or source.is_request or source.is_sql or source.is_file


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


def _sync_source_key(source: _DatasetSourceInput) -> str:
    if source.is_resource:
        return f"resource:{source.resource_name}"
    if source.is_request:
        request_path = source.api_identity
        return f"request:{request_path}"
    if source.is_table:
        return f"table:{source.table_name}"
    if source.is_sql:
        return f"sql:{stable_payload_hash(source.sql_text)}"
    if source.is_file:
        return f"storage:{source.storage_uri}"
    raise BusinessValidationError("Synced datasets must define sync.source.")


def _sync_source_label(source: _DatasetSourceInput) -> str:
    if source.is_resource:
        return f"API resource path '{source.resource_name}'"
    if source.is_request:
        return f"API request '{source.api_identity}'"
    if source.is_table:
        return f"table '{source.table_name}'"
    if source.is_sql:
        return "SQL query"
    if source.is_file:
        return f"storage source '{source.storage_uri}'"
    return "sync source"


def _sync_source_payload(source: _DatasetSourceInput) -> dict[str, Any]:
    if source.is_table:
        return {"kind": "table", "table": source.table_name}
    if source.is_resource:
        payload: dict[str, Any] = {"kind": "resource", "resource": source.resource_name}
        if source.flatten_paths:
            payload["flatten"] = list(source.flatten_paths)
        if source.extraction_config is not None:
            payload["extraction"] = dict(source.extraction_config)
        return payload
    if source.is_request:
        payload = {
            "kind": "request",
            "request": dict(source.request_config or {}),
        }
        if source.flatten_paths:
            payload["flatten"] = list(source.flatten_paths)
        if source.extraction_config is not None:
            payload["extraction"] = dict(source.extraction_config)
        return payload
    if source.is_sql:
        return {"kind": "sql", "sql": source.sql_text}
    if source.is_file:
        payload = {"kind": "file", "storage_uri": source.storage_uri}
        if source.requested_file_format:
            payload["format"] = source.requested_file_format
        if source.header is not None:
            payload["header"] = source.header
        if source.delimiter is not None:
            payload["delimiter"] = source.delimiter
        if source.quote is not None:
            payload["quote"] = source.quote
        return payload
    return {}


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
    def _semantic_dataset_relation_name(*, dataset: DatasetMetadata, configured_record) -> str | None:
        if configured_record is not None:
            return configured_record.relation_name
        relation_identity = dict(dataset.relation_identity_json or {})
        relation_name = str(relation_identity.get("relation_name") or "").strip()
        if relation_name:
            return relation_name
        table_name = str(dataset.table_name or "").strip()
        if table_name:
            return table_name
        return str(dataset.sql_alias or "").strip() or None

    def _dataset_semantic_models(self, *, dataset: DatasetMetadata, configured_record) -> list[str]:
        relation_name = self._semantic_dataset_relation_name(dataset=dataset, configured_record=configured_record)
        if not relation_name:
            return []
        normalized_relation_name = relation_name.strip().lower()
        matches: list[str] = []
        for model_name, record in self._host._semantic_models.items():
            semantic_model = record.semantic_model
            datasets_payload = (
                semantic_model.datasets
                if semantic_model is not None
                else ((record.content_json or {}).get("datasets") or {})
            )
            if not isinstance(datasets_payload, dict):
                continue
            for dataset_key, dataset_entry in datasets_payload.items():
                candidate_relation_name = ""
                if semantic_model is not None and hasattr(dataset_entry, "get_relation_name"):
                    candidate_relation_name = str(dataset_entry.get_relation_name(dataset_key) or "").strip()
                elif isinstance(dataset_entry, dict):
                    candidate_relation_name = str(
                        dataset_entry.get("relation_name")
                        or dataset_entry.get("relationName")
                        or dataset_key
                    ).strip()
                if candidate_relation_name.lower() == normalized_relation_name:
                    matches.append(model_name)
                    break
        return matches

    @staticmethod
    def _sync_source_payload(dataset) -> dict[str, Any]:
        payload = dict(getattr(dataset, "sync_json", None) or {})
        source = payload.get("source")
        return dict(source) if isinstance(source, dict) else {}

    @classmethod
    def _sync_source_key(cls, dataset) -> str | None:
        payload = cls._sync_source_payload(dataset)
        if not payload:
            return None
        return _sync_source_key(
            _DatasetSourceInput.from_source_config(DatasetSource.model_validate(payload))
        )

    def _build_dataset_sync_state_payload(
        self,
        *,
        dataset: DatasetMetadata,
        connector: ConnectorMetadata,
        source_key: str,
        source: Mapping[str, Any],
        sync_strategy: ConnectorSyncStrategy,
        state,
    ) -> dict[str, Any]:
        if state is None:
            return {
                "id": None,
                "workspace_id": self._host.context.workspace_id,
                "connection_id": connector.id,
                "connector_name": connector.name,
                "connector_type": connector.connector_type_value,
                "source_key": source_key,
                "source": dict(source),
                "sync_mode": sync_strategy.value,
                "last_cursor": None,
                "last_sync_at": None,
                "state": {},
                "status": "never_synced",
                "error_message": None,
                "records_synced": 0,
                "bytes_synced": None,
                "dataset_ids": [dataset.id],
                "dataset_names": [dataset.name],
                "created_at": None,
                "updated_at": None,
            }
        return {
            "id": state.id,
            "workspace_id": state.workspace_id,
            "connection_id": state.connection_id,
            "connector_name": connector.name,
            "connector_type": state.connector_type_value,
            "source_key": state.source_key,
            "source": dict(getattr(state, "source_json", None) or source),
            "sync_mode": state.sync_mode_value,
            "last_cursor": state.last_cursor,
            "last_sync_at": state.last_sync_at,
            "state": dict(state.state_json or {}),
            "status": state.status_value,
            "error_message": state.error_message,
            "records_synced": int(state.records_synced or 0),
            "bytes_synced": state.bytes_synced,
            "dataset_ids": [dataset.id],
            "dataset_names": [dataset.name],
            "created_at": state.created_at,
            "updated_at": state.updated_at,
        }

    async def _sync_state_snapshot(self, *, dataset) -> dict[str, Any] | None:
        source_key = self._sync_source_key(dataset)
        if not source_key or dataset.connection_id is None:
            return None
        state = await self._host._connector_sync_state_repository.get_for_resource(
            workspace_id=self._host.context.workspace_id,
            connection_id=dataset.connection_id,
            resource_name=source_key,
        )
        if state is None:
            return {
                "status": "never_synced",
                "source_key": source_key,
                "last_cursor": None,
                "last_sync_at": None,
                "records_synced": 0,
                "bytes_synced": None,
            }
        return {
            "status": state.status_value,
            "source_key": source_key,
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
            sync_state = await self._sync_state_snapshot(dataset=dataset)
            management_mode = self._management_mode_value(dataset.management_mode)
            semantic_models = self._dataset_semantic_models(
                dataset=dataset,
                configured_record=configured_record,
            )
            items.append(
                {
                    "id": dataset.id,
                    "name": dataset.name,
                    "label": self._dataset_label(dataset=dataset, configured_record=configured_record),
                    "description": dataset.description,
                    "connector": connector_name,
                    "semantic_models": semantic_models,
                    "semantic_model": next(iter(semantic_models), None),
                    "materialization": dataset.materialization_json,
                    "materialization_mode": resolve_dataset_materialization_mode(
                        explicit_materialization_mode=dataset.materialization_mode_value,
                    ).value,
                    "source": dataset.source_json,
                    "schema_hint": dataset.schema_hint_json,
                    "sync": dataset.sync_json,
                    "status": dataset.status_value,
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
        semantic_models = self._dataset_semantic_models(
            dataset=dataset,
            configured_record=configured_record,
        )
        return {
            "id": dataset.id,
            "name": dataset.name,
            "label": self._dataset_label(dataset=dataset, configured_record=configured_record),
            "description": dataset.description,
            "sql_alias": dataset.sql_alias,
            "connector": connector.name if connector is not None else None,
            "connector_id": connector.id if connector is not None else None,
            "semantic_models": semantic_models,
            "semantic_model": next(iter(semantic_models), None),
            "dataset_type": dataset.dataset_type_value,
            "materialization": dataset.materialization_json,
            "materialization_mode": resolve_dataset_materialization_mode(
                explicit_materialization_mode=dataset.materialization_mode_value,
            ).value,
            "source": dataset.source_json,
            "schema_hint": dataset.schema_hint_json,
            "sync": dataset.sync_json,
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

    def _source_request_from_dataset(self, *, dataset: DatasetMetadata) -> dict[str, Any] | None:
        if dataset.source is None:
            return None
        return dict(dataset.source_json or {})

    def _materialization_request_from_dataset(self, *, dataset: DatasetMetadata) -> dict[str, Any] | None:
        if dataset.materialization is None:
            return None
        return dict(dataset.materialization_json or {})

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
        sync_source: _DatasetSourceInput,
    ) -> str | None:
        if description:
            return description
        if materialization_mode == DatasetMaterializationMode.SYNCED:
            return (
                "Runtime-managed synced dataset awaiting dataset sync for "
                f"{_sync_source_label(sync_source)}."
            )
        return None

    @staticmethod
    def _dataset_tags(
        *,
        existing: list[str],
        materialization_mode: DatasetMaterializationMode,
        connector: ConnectorMetadata | None,
        sync_source: _DatasetSourceInput,
    ) -> list[str]:
        required_tags: list[str] = []
        if materialization_mode == DatasetMaterializationMode.SYNCED and connector is not None:
            connector_type = str(
                connector.connector_type.value if connector.connector_type is not None else ""
            ).strip().lower()
            if sync_source.is_resource or sync_source.is_request:
                required_tags = [
                    "managed",
                    "api-connector",
                    connector_type,
                    f"resource:{sync_source.api_identity.strip().lower()}",
                ]
            elif sync_source.is_table:
                required_tags = [
                    "managed",
                    "database-connector",
                    connector_type,
                    f"table:{sync_source.table_name.strip().lower()}",
                ]
            elif sync_source.is_sql:
                required_tags = [
                    "managed",
                    "database-connector",
                    connector_type,
                    "sql-sync",
                ]
        return _merge_dataset_tags(existing=existing, required=required_tags)

    @staticmethod
    def _connector_supports_sql_sync_runtime(connector: ConnectorMetadata) -> bool:
        if connector.connector_type is None:
            return False
        try:
            SqlConnectorFactory.get_sql_connector_class_reference(connector.connector_type)
        except ValueError:
            return False
        return True

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
                f"Dataset '{dataset_name}' requires a connector for table, sql, api, or synced sources."
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
            f"Dataset '{dataset_name}' requires a connector for table, sql, api, or synced sources."
        )

    def _validate_synced_dataset(
        self,
        *,
        dataset_name: str,
        connector: ConnectorMetadata | None,
        sync: _DatasetSyncInput,
    ) -> ConnectorMetadata:
        resolved_connector = self._require_connector(dataset_name=dataset_name, connector=connector)
        connector_capabilities = self._host._connector_capabilities(resolved_connector)
        if not connector_capabilities.supports_synced_datasets:
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requests materialization_mode 'synced', "
                f"but connector '{resolved_connector.name}' does not support synced datasets."
            )
        if not sync.has_sync:
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requests materialization_mode 'synced', "
                "but is missing source/materialization.sync."
            )
        sync_source = sync.source
        if sync_source.is_resource or sync_source.is_request:
            plugin = self._host._resolve_connector_plugin_for_type(resolved_connector.connector_type_value)
            if plugin is None or plugin.api_connector_class is None:
                raise BusinessValidationError(
                    f"Dataset '{dataset_name}' uses an API sync source, "
                    f"but connector '{resolved_connector.name}' does not expose a runtime API sync path yet."
                )
            return resolved_connector
        if sync_source.is_table or sync_source.is_sql:
            if not connector_capabilities.supports_query_pushdown:
                raise BusinessValidationError(
                    f"Dataset '{dataset_name}' uses a SQL/table sync source, "
                    f"but connector '{resolved_connector.name}' does not expose SQL query execution."
                )
            if not self._connector_supports_sql_sync_runtime(resolved_connector):
                raise BusinessValidationError(
                    f"Dataset '{dataset_name}' uses a SQL/table sync source, "
                    f"but connector '{resolved_connector.name}' does not expose a SQL sync runtime path yet."
                )
            return resolved_connector
        raise BusinessValidationError(
            f"Dataset '{dataset_name}' uses unsupported sync.source shape. "
            "Supported synced sources are resource, table, and sql."
        )
        return resolved_connector

    def _require_dataset_sync_contract(
        self,
        *,
        dataset: DatasetMetadata,
        connector: ConnectorMetadata | None,
    ) -> tuple[ConnectorMetadata, DatasetSyncConfig]:
        if dataset.materialization_mode != DatasetMaterializationMode.SYNCED:
            raise BusinessValidationError(
                f"Dataset '{dataset.name}' is not a synced dataset."
            )
        sync_config = dataset.sync
        if sync_config is None:
            raise BusinessValidationError(
                f"Dataset '{dataset.name}' is missing a sync contract."
            )
        resolved_connector = self._require_connector(
            dataset_name=dataset.name,
            connector=connector,
        )
        connector_capabilities = self._host._connector_capabilities(resolved_connector)
        if not connector_capabilities.supports_synced_datasets:
            raise BusinessValidationError(
                f"Dataset '{dataset.name}' is bound to connector '{resolved_connector.name}', "
                "but that connector does not support synced datasets."
            )
        sync_source = _DatasetSourceInput.from_source_config(sync_config.source)
        if not (sync_source.is_resource or sync_source.is_table or sync_source.is_sql or sync_source.is_request):
            raise BusinessValidationError(
                f"Dataset '{dataset.name}' is missing sync.source."
            )
        self._validate_synced_dataset(
            dataset_name=dataset.name,
            connector=resolved_connector,
            sync=_DatasetSyncInput(
                source=sync_source,
                strategy=sync_config.strategy,
                cadence=sync_config.cadence,
                sync_on_start=bool(sync_config.sync_on_start),
                cursor_field=sync_config.cursor_field,
                initial_cursor=sync_config.initial_cursor,
                lookback_window=sync_config.lookback_window,
                backfill_start=sync_config.backfill_start,
                backfill_end=sync_config.backfill_end,
            ),
        )
        return resolved_connector, sync_config

    async def _resolve_sync_root_resource(
        self,
        *,
        dataset: DatasetMetadata,
        connector: ConnectorMetadata,
        api_connector: Any,
        resource_name: str,
    ) -> ApiResource:
        discovered_resources = {
            resource.name: resource
            for resource in await api_connector.discover_resources()
        }
        resource = discovered_resources.get(resource_name)
        if resource is not None:
            return resource

        resolver = getattr(api_connector, "resolve_resource", None)
        if callable(resolver):
            try:
                resolved_resource = resolver(resource_name)
                if inspect.isawaitable(resolved_resource):
                    resolved_resource = await resolved_resource
                if isinstance(resolved_resource, ApiResource):
                    return resolved_resource
            except Exception as exc:
                raise BusinessValidationError(
                    f"Dataset '{dataset.name}' could not resolve connector resource '{resource_name}'."
                ) from exc

        raise BusinessValidationError(
            f"Dataset '{dataset.name}' is bound to connector '{connector.name}', "
            f"but connector resource '{resource_name}' was not found."
        )

    def _resolve_sync_config(
        self,
        *,
        dataset_name: str,
        connector: ConnectorMetadata,
        sync: _DatasetSyncInput,
    ) -> DatasetSyncConfig:
        requested_strategy = (
            sync.strategy
            or connector.default_sync_strategy
            or ConnectorSyncStrategy.FULL_REFRESH
        )
        if requested_strategy not in {
            ConnectorSyncStrategy.FULL_REFRESH,
            ConnectorSyncStrategy.INCREMENTAL,
        }:
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requests unsupported sync strategy '{requested_strategy.value}'."
            )
        connector_capabilities = self._host._connector_capabilities(connector)
        if (
            requested_strategy == ConnectorSyncStrategy.INCREMENTAL
            and not connector_capabilities.supports_incremental_sync
        ):
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requests incremental sync, "
                f"but connector '{connector.name}' does not support incremental sync."
            )
        return DatasetSyncConfig(
            source=_sync_source_payload(sync.source),
            strategy=requested_strategy,
            cadence=sync.cadence,
            sync_on_start=sync.sync_on_start,
            cursor_field=sync.cursor_field,
            initial_cursor=sync.initial_cursor,
            lookback_window=sync.lookback_window,
            backfill_start=sync.backfill_start,
            backfill_end=sync.backfill_end,
        )

    async def build_scheduled_sync_tasks(self):
        from langbridge.runtime.hosting.background import (
            RuntimeBackgroundTaskDefinition,
            background_task_schedule_from_dataset_cadence,
            build_dataset_sync_default_task,
        )

        async with self._host._runtime_operation_scope():
            datasets = await self._host._dataset_repository.list_for_workspace(
                workspace_id=self._host.context.workspace_id,
                limit=1000,
                offset=0,
            )

        tasks: list[RuntimeBackgroundTaskDefinition] = []
        for dataset in datasets:
            if dataset.materialization_mode != DatasetMaterializationMode.SYNCED:
                continue
            connector = self._host._connector_for_id(dataset.connection_id)
            _, sync_config = self._require_dataset_sync_contract(
                dataset=dataset,
                connector=connector,
            )
            if not sync_config.cadence and not sync_config.sync_on_start:
                continue
            schedule = None
            if sync_config.cadence:
                schedule = background_task_schedule_from_dataset_cadence(
                    sync_config.cadence
                )
            tasks.append(
                build_dataset_sync_default_task(
                    dataset_ref=str(dataset.id),
                    dataset_name=dataset.name,
                    schedule=schedule,
                    run_on_startup=bool(sync_config.sync_on_start),
                    description=(
                        f"Sync dataset '{dataset.name}' from its dataset-owned sync contract."
                    ),
                )
            )
        return tuple(tasks)

    def _validate_dataset_mutation(
        self,
        *,
        dataset_name: str,
        materialization_mode: DatasetMaterializationMode,
        connector: ConnectorMetadata | None,
        source: _DatasetSourceInput,
        sync: _DatasetSyncInput,
    ) -> ConnectorMetadata | None:
        if materialization_mode == DatasetMaterializationMode.SYNCED:
            return self._validate_synced_dataset(
                dataset_name=dataset_name,
                connector=connector,
                sync=sync,
            )
        if not source.requires_connector:
            return connector
        resolved_connector = self._require_connector(dataset_name=dataset_name, connector=connector)
        connector_capabilities = self._host._connector_capabilities(resolved_connector)
        if not connector_capabilities.supports_live_datasets:
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' requests materialization_mode 'live', "
                f"but connector '{resolved_connector.name}' does not support live datasets."
            )
        if source.is_resource or source.is_request:
            plugin = self._host._resolve_connector_plugin_for_type(resolved_connector.connector_type_value)
            if plugin is None or plugin.api_connector_class is None:
                raise BusinessValidationError(
                    f"Dataset '{dataset_name}' uses a live API resource source, "
                    f"but connector '{resolved_connector.name}' does not expose a live API execution path yet."
                )
            if not connector_capabilities.supports_federated_execution:
                raise BusinessValidationError(
                    f"Dataset '{dataset_name}' uses a live API resource source, "
                    f"but connector '{resolved_connector.name}' does not support federated execution."
                )
            return resolved_connector
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
    def _synced_file_config() -> dict[str, Any]:
        return {
            "format": "parquet",
            "managed_dataset": True,
        }

    def _build_dataset_definition(
        self,
        *,
        dataset_name: str,
        sql_alias: str,
        materialization_mode: DatasetMaterializationMode,
        connector: ConnectorMetadata | None,
        source: _DatasetSourceInput,
        sync: DatasetSyncConfig | None,
    ) -> _DatasetDefinition:
        if materialization_mode == DatasetMaterializationMode.SYNCED:
            self._require_connector(dataset_name=dataset_name, connector=connector)
            if sync is None:
                raise BusinessValidationError(
                    f"Dataset '{dataset_name}' is missing a sync contract."
                )
            sync_source = _DatasetSourceInput.from_source_config(sync.source)
            if sync_source.is_resource or sync_source.is_request:
                source_kind = DatasetSourceKind.API
            elif sync_source.is_table or sync_source.is_sql:
                source_kind = DatasetSourceKind.DATABASE
            elif sync_source.is_file:
                source_kind = DatasetSourceKind.FILE
            else:
                raise BusinessValidationError(
                    f"Dataset '{dataset_name}' is missing sync.source."
                )
            return _DatasetDefinition(
                catalog_name=None,
                schema_name=None,
                table_name=sql_alias,
                relation_name=sql_alias,
                dataset_type=DatasetType.FILE,
                sql_text=None,
                storage_kind=DatasetStorageKind.PARQUET,
                source_kind=source_kind,
                dialect="duckdb",
                storage_uri=None,
                file_config=self._synced_file_config(),
            )
        if source.is_resource or source.is_request:
            self._require_connector(dataset_name=dataset_name, connector=connector)
            return _DatasetDefinition(
                catalog_name=None,
                schema_name=None,
                table_name=sql_alias,
                relation_name=sql_alias,
                dataset_type=DatasetType.API,
                sql_text=None,
                storage_kind=DatasetStorageKind.MEMORY,
                source_kind=DatasetSourceKind.API,
                dialect="duckdb",
                storage_uri=None,
                file_config=None,
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

    @staticmethod
    def _build_dataset_source(
        *,
        materialization_mode: DatasetMaterializationMode,
        source: _DatasetSourceInput,
        definition: _DatasetDefinition,
    ) -> DatasetSource | None:
        if source.is_table:
            return DatasetSource(kind="table", table=source.table_name)
        if source.is_resource:
            return DatasetSource(
                kind="resource",
                resource=source.resource_name,
                flatten=source.flatten_paths or None,
                extraction=source.extraction_config,
            )
        if source.is_request:
            return DatasetSource(
                kind="request",
                request=dict(source.request_config or {}),
                flatten=source.flatten_paths or None,
                extraction=source.extraction_config,
            )
        if source.is_sql:
            return DatasetSource(kind="sql", sql=source.sql_text)
        file_storage_uri = source.storage_uri or definition.storage_uri
        if file_storage_uri is None:
            raise BusinessValidationError("File-backed datasets require storage_uri.")
        payload: dict[str, Any] = {
            "kind": "file",
            "storage_uri": file_storage_uri,
            "format": str(
                (definition.file_config or {}).get("format")
                or (definition.file_config or {}).get("file_format")
                or ""
            ).strip().lower()
            or None,
            "header": source.header,
            "delimiter": source.delimiter,
            "quote": source.quote,
        }
        return DatasetSource.model_validate(
            {key: value for key, value in payload.items() if value is not None}
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

    def _build_file_dataset_columns(
        self,
        *,
        dataset: DatasetMetadata | None,
        dataset_id: uuid.UUID,
        dataset_name: str,
        definition: _DatasetDefinition,
        now: datetime,
    ) -> list[DatasetColumnMetadata]:
        if definition.dataset_type != DatasetType.FILE or definition.storage_uri is None:
            return []
        try:
            described_columns = describe_file_source_schema(
                storage_uri=definition.storage_uri,
                file_config=definition.file_config,
            )
        except Exception as exc:  # pragma: no cover - surfaced as validation error
            raise BusinessValidationError(
                f"Dataset '{dataset_name}' file source could not be inspected for schema inference: {exc}"
            ) from exc

        workspace_id = (
            dataset.workspace_id
            if dataset is not None
            else self._host.context.workspace_id
        )
        created_at = dataset.created_at if dataset is not None else now
        return [
            DatasetColumnMetadata(
                id=uuid.uuid4(),
                dataset_id=dataset_id,
                workspace_id=workspace_id,
                name=column.name,
                data_type=column.data_type,
                nullable=column.nullable,
                ordinal_position=index,
                created_at=created_at,
                updated_at=now,
            )
            for index, column in enumerate(described_columns)
        ]

    async def _replace_dataset_columns(
        self,
        *,
        dataset: DatasetMetadata,
        columns: list[DatasetColumnMetadata],
    ) -> None:
        await self._host._dataset_column_repository.delete_for_dataset(dataset_id=dataset.id)
        flush = getattr(self._host._dataset_column_repository, "flush", None)
        if flush is not None:
            await flush()
        for column in columns:
            self._host._dataset_column_repository.add(column)
        dataset.columns = list(columns)

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

    async def _assert_sync_source_is_available(
        self,
        *,
        connector: ConnectorMetadata,
        source_key: str,
        current_dataset_id: uuid.UUID | None = None,
    ) -> None:
        existing = await self._host._dataset_repository.list_for_connection(
            workspace_id=self._host.context.workspace_id,
            connection_id=connector.id,
            limit=1000,
        )
        for candidate in existing:
            if current_dataset_id is not None and candidate.id == current_dataset_id:
                continue
            if candidate.materialization_mode != DatasetMaterializationMode.SYNCED:
                continue
            candidate_source = dict((candidate.sync_json or {}).get("source") or {})
            if not candidate_source:
                continue
            candidate_source_key = _sync_source_key(
                _DatasetSourceInput.from_source_config(DatasetSource.model_validate(candidate_source))
            )
            if candidate_source_key == source_key:
                raise BusinessValidationError(
                    f"Connector '{connector.name}' already has dataset '{candidate.name}' bound to sync.source "
                    f"'{source_key}'. Sync source keys must be unique per connector."
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
                label=dataset.label or self._runtime_dataset_label(dataset.name),
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
        sync = _DatasetSyncInput.from_config(normalized_request)
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
            sync=sync,
        )
        resolved_sync = (
            self._resolve_sync_config(
                dataset_name=dataset_name,
                connector=self._require_connector(dataset_name=dataset_name, connector=connector),
                sync=sync,
            )
            if materialization_mode == DatasetMaterializationMode.SYNCED
            else None
        )

        dataset_id = uuid.uuid4()
        dataset_alias = _dataset_sql_alias(dataset_name)
        definition = self._build_dataset_definition(
            dataset_name=dataset_name,
            sql_alias=dataset_alias,
            materialization_mode=materialization_mode,
            connector=connector,
            source=source,
            sync=resolved_sync,
        )
        live_source = self._build_dataset_source(
            materialization_mode=materialization_mode,
            source=source,
            definition=definition,
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
        inferred_columns = (
            self._build_file_dataset_columns(
                dataset=None,
                dataset_id=dataset_id,
                dataset_name=dataset_name,
                definition=definition,
                now=now,
            )
            if materialization_mode == DatasetMaterializationMode.LIVE
            else []
        )

        dataset = DatasetMetadata(
            id=dataset_id,
            workspace_id=self._host.context.workspace_id,
            connection_id=None if connector is None else connector.id,
            owner_id=actor_id,
            created_by=actor_id,
            updated_by=actor_id,
            name=dataset_name,
            label=normalized_request.label or self._runtime_dataset_label(dataset_name),
            sql_alias=dataset_alias,
            description=self._dataset_description(
                description=normalized_request.description,
                materialization_mode=materialization_mode,
                sync_source=sync.source,
            ),
            tags=self._dataset_tags(
                existing=list(normalized_request.tags or []),
                materialization_mode=materialization_mode,
                connector=connector,
                sync_source=sync.source,
            ),
            dataset_type=definition.dataset_type,
            materialization={
                "mode": materialization_mode,
                "sync": None if resolved_sync is None else {
                    "strategy": resolved_sync.strategy,
                    "cadence": resolved_sync.cadence,
                    "sync_on_start": resolved_sync.sync_on_start,
                    "cursor_field": resolved_sync.cursor_field,
                    "initial_cursor": resolved_sync.initial_cursor,
                    "lookback_window": resolved_sync.lookback_window,
                    "backfill_start": resolved_sync.backfill_start,
                    "backfill_end": resolved_sync.backfill_end,
                },
            },
            source=live_source,
            schema_hint=(
                None
                if normalized_request.schema_hint is None
                else normalized_request.schema_hint.model_dump(mode="json")
            ),
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
            if materialization_mode == DatasetMaterializationMode.SYNCED and connector is not None:
                await self._assert_sync_source_is_available(
                    connector=connector,
                    source_key=_sync_source_key(sync.source),
                )
            dataset = self._host._dataset_repository.add(dataset)
            policy = self._host._dataset_policy_repository.add(policy)
            if inferred_columns:
                await self._replace_dataset_columns(dataset=dataset, columns=inferred_columns)
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
            existing_columns = await self._host._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
            connector = self._host._connector_for_id(dataset.connection_id)

            fields_set = set(getattr(request, "model_fields_set", set()))
            payload = {
                "name": dataset.name,
                "label": (
                    request.label
                    if "label" in fields_set
                    else dataset.label
                ),
                "description": (
                    request.description
                    if "description" in fields_set
                    else dataset.description
                ),
                "connector": None if connector is None else connector.name,
                "materialization": (
                    request.materialization.model_dump(mode="json")
                    if "materialization" in fields_set and request.materialization is not None
                    else self._materialization_request_from_dataset(dataset=dataset)
                ),
                "source": (
                    request.source.model_dump(mode="json")
                    if "source" in fields_set and request.source is not None
                    else self._source_request_from_dataset(dataset=dataset)
                ),
                "schema_hint": (
                    request.schema_hint.model_dump(mode="json")
                    if "schema_hint" in fields_set and request.schema_hint is not None
                    else dataset.schema_hint_json
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
            sync = _DatasetSyncInput.from_config(normalized_request)
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
                sync=sync,
            )
            resolved_sync = (
                self._resolve_sync_config(
                    dataset_name=dataset.name,
                    connector=self._require_connector(dataset_name=dataset.name, connector=connector),
                    sync=sync,
                )
                if materialization_mode == DatasetMaterializationMode.SYNCED
                else None
            )
            actor_id = self._host._resolve_actor_id()
            now = datetime.now(timezone.utc)
            definition = self._build_dataset_definition(
                dataset_name=dataset.name,
                sql_alias=dataset.sql_alias,
                materialization_mode=materialization_mode,
                connector=connector,
                source=source,
                sync=resolved_sync,
            )
            live_source = self._build_dataset_source(
                materialization_mode=materialization_mode,
                source=source,
                definition=definition,
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
            next_columns: list[DatasetColumnMetadata] | None = None
            if materialization_mode == DatasetMaterializationMode.SYNCED:
                next_columns = []
            elif definition.dataset_type == DatasetType.FILE:
                if (
                    "source" in fields_set
                    or "materialization" in fields_set
                    or dataset.dataset_type != DatasetType.FILE
                    or not existing_columns
                ):
                    next_columns = self._build_file_dataset_columns(
                        dataset=dataset,
                        dataset_id=dataset.id,
                        dataset_name=dataset.name,
                        definition=definition,
                        now=now,
                    )
            elif "source" in fields_set or "materialization" in fields_set:
                next_columns = []
            dataset.description = self._dataset_description(
                description=normalized_request.description,
                materialization_mode=materialization_mode,
                sync_source=sync.source,
            )
            dataset.label = normalized_request.label or self._runtime_dataset_label(dataset.name)
            dataset.connection_id = None if connector is None else connector.id
            dataset.updated_by = actor_id
            dataset.tags = self._dataset_tags(
                existing=list(normalized_request.tags or []),
                materialization_mode=materialization_mode,
                connector=connector,
                sync_source=sync.source,
            )
            dataset.dataset_type = definition.dataset_type
            dataset.materialization = DatasetMaterializationConfig.model_validate(
                {
                    "mode": materialization_mode,
                    "sync": None if resolved_sync is None else {
                        "strategy": resolved_sync.strategy,
                        "cadence": resolved_sync.cadence,
                        "sync_on_start": resolved_sync.sync_on_start,
                        "cursor_field": resolved_sync.cursor_field,
                        "initial_cursor": resolved_sync.initial_cursor,
                        "lookback_window": resolved_sync.lookback_window,
                        "backfill_start": resolved_sync.backfill_start,
                        "backfill_end": resolved_sync.backfill_end,
                    },
                }
            )
            dataset.source = live_source
            dataset.schema_hint = (
                None
                if normalized_request.schema_hint is None
                else DatasetSchemaHint.model_validate(normalized_request.schema_hint.model_dump(mode="json"))
            )
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

            if materialization_mode == DatasetMaterializationMode.SYNCED and connector is not None:
                await self._assert_sync_source_is_available(
                    connector=connector,
                    source_key=_sync_source_key(sync.source),
                    current_dataset_id=dataset.id,
                )
            await self._host._dataset_repository.save(dataset)
            if existing_policy is None:
                self._host._dataset_policy_repository.add(policy)
            else:
                await self._host._dataset_policy_repository.save(policy)
            if next_columns is not None:
                await self._replace_dataset_columns(dataset=dataset, columns=next_columns)
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

    async def get_dataset_sync(
        self,
        *,
        dataset_ref: str,
    ) -> dict[str, Any]:
        async with self._host._runtime_operation_scope():
            dataset = await self._host._resolve_dataset_record(dataset_ref)
            connector = self._host._connector_for_id(dataset.connection_id)
            connector, sync_config = self._require_dataset_sync_contract(
                dataset=dataset,
                connector=connector,
            )
            state = await self._host._connector_sync_state_repository.get_for_resource(
                workspace_id=self._host.context.workspace_id,
                connection_id=connector.id,
                resource_name=_sync_source_key(
                    _DatasetSourceInput.from_source_config(sync_config.source)
                ),
            )

        sync_source = _DatasetSourceInput.from_source_config(sync_config.source)
        source_key = _sync_source_key(sync_source)
        source_payload = _sync_source_payload(sync_source)

        return {
            "dataset_id": dataset.id,
            "dataset_name": dataset.name,
            "connector_id": connector.id,
            "connector_name": connector.name,
            "connector_type": connector.connector_type_value,
            "materialization_mode": dataset.materialization_mode_value,
            "source_key": source_key,
            "source": source_payload,
            "sync": dataset.sync_json,
            "sync_state": self._build_dataset_sync_state_payload(
                dataset=dataset,
                connector=connector,
                source_key=source_key,
                source=source_payload,
                sync_strategy=sync_config.strategy,
                state=state,
            ),
        }

    async def sync_dataset(
        self,
        *,
        dataset_ref: str,
        sync_mode: str = "INCREMENTAL",
        force_full_refresh: bool = False,
    ) -> dict[str, Any]:
        async with self._host._runtime_operation_scope():
            dataset = await self._host._resolve_dataset_record(dataset_ref)
            connector = self._host._connector_for_id(dataset.connection_id)
            connector, sync_config = self._require_dataset_sync_contract(
                dataset=dataset,
                connector=connector,
            )

        requested_sync_mode = self._host._normalize_sync_mode(sync_mode)

        active_state = None
        try:
            async with self._host._runtime_operation_scope() as uow:
                dataset = await self._host._resolve_dataset_record(dataset_ref)
                summary = await self._host._runtime_host.sync_dataset(
                    workspace_id=self._host.context.workspace_id,
                    actor_id=self._host.context.actor_id,
                    connector_record=connector,
                    dataset=dataset,
                    sync_mode=(
                        ConnectorSyncMode.FULL_REFRESH
                        if force_full_refresh
                        else requested_sync_mode
                    ),
                )
                sync_source = _DatasetSourceInput.from_source_config(sync_config.source)
                active_state = await self._host.services.dataset_sync.get_or_create_state(
                    workspace_id=self._host.context.workspace_id,
                    connection_id=connector.id,
                    connector_type=self._host._resolve_connector_runtime_type(connector),
                    resource_name=_sync_source_key(sync_source),
                    sync_mode=requested_sync_mode,
                )
                if uow is not None:
                    await uow.commit()
        except Exception as exc:
            if active_state is not None:
                async with self._host._runtime_operation_scope() as failure_uow:
                    await self._host.services.dataset_sync.mark_failed(
                        state=active_state,
                        error_message=str(exc),
                    )
                    if failure_uow is not None:
                        await failure_uow.commit()
            raise

        return {
            "status": "succeeded",
            "dataset_id": dataset.id,
            "dataset_name": dataset.name,
            "connector_id": connector.id,
            "connector_name": connector.name,
            "sync_mode": summary.get("sync_mode"),
            "resources": [summary],
            "summary": f"Dataset sync completed for '{dataset.name}'.",
        }
