
import re
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Any, Mapping

from langbridge.runtime.config.models import (
    LocalRuntimeDatasetConfig,
    LocalRuntimeDatasetPolicyConfig,
)
from langbridge.runtime.models import DatasetMetadata, DatasetPolicyMetadata
from langbridge.runtime.models.metadata import (
    DatasetMaterializationMode,
    DatasetSourceKind,
    DatasetStorageKind,
    LifecycleState,
    ManagementMode,
)
from langbridge.runtime.settings import runtime_settings as settings
from langbridge.runtime.utils.datasets import (
    build_dataset_execution_capabilities,
    build_dataset_relation_identity,
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
        raise ValueError("Dataset table source must not be empty.")
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
            "status": state.status,
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
                        file_config=dict(dataset.file_config_json or {}),
                    ).value,
                    "status": dataset.status,
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
            "dataset_type": dataset.dataset_type,
            "materialization_mode": resolve_dataset_materialization_mode(
                explicit_materialization_mode=dataset.materialization_mode_value,
                file_config=dict(dataset.file_config_json or {}),
            ).value,
            "source_kind": dataset.source_kind,
            "storage_kind": dataset.storage_kind,
            "table_name": dataset.table_name,
            "storage_uri": dataset.storage_uri,
            "dialect": dataset.dialect,
            "status": dataset.status,
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
    ) -> None:
        change_summary = f"Runtime dataset '{dataset.name}' created."
        dataset_sync = self._host.services.dataset_sync
        if (
            dataset.materialization_mode_value == DatasetMaterializationMode.SYNCED.value
            and dataset_sync is not None
        ):
            await dataset_sync._create_dataset_revision(
                dataset=dataset,
                policy=policy,
                created_by=actor_id,
                change_summary=change_summary,
            )
            await dataset_sync._replace_dataset_lineage(dataset=dataset)
            return

        dataset_query = self._host.services.dataset_query
        if dataset_query is not None:
            await dataset_query._create_dataset_revision(
                dataset=dataset,
                policy=policy,
                created_by=actor_id,
                change_summary=change_summary,
            )
            await dataset_query._replace_dataset_lineage(dataset)
            return

        if dataset_sync is not None:
            await dataset_sync._create_dataset_revision(
                dataset=dataset,
                policy=policy,
                created_by=actor_id,
                change_summary=change_summary,
            )
            await dataset_sync._replace_dataset_lineage(dataset=dataset)

    async def create_dataset(self, *, request) -> dict[str, Any]:
        from langbridge.runtime.bootstrap.configured_runtime import LocalRuntimeDatasetRecord

        normalized_request = LocalRuntimeDatasetConfig.model_validate(
            request.model_dump(mode="json")
        )
        dataset_name = str(normalized_request.name or "").strip()
        if not dataset_name:
            raise ValueError("Dataset name is required.")

        connector = self._host._resolve_connector(normalized_request.connector)
        connector_capabilities = self._host._connector_capabilities(connector)
        materialization_mode = resolve_dataset_materialization_mode(
            explicit_materialization_mode=normalized_request.materialization_mode,
            file_config=None,
        )

        source_table = str(normalized_request.source.table or "").strip()
        sync_resource_name = str(normalized_request.source.resource or "").strip()
        source_sql = str(normalized_request.source.sql or "").strip()
        source_storage_uri = str(normalized_request.source.storage_uri or "").strip() or None
        if source_storage_uri is None and normalized_request.source.path:
            source_storage_uri = Path(str(normalized_request.source.path)).resolve().as_uri()

        if (
            materialization_mode == DatasetMaterializationMode.LIVE
            and not connector_capabilities.supports_live_datasets
        ):
            raise ValueError(
                f"Dataset '{dataset_name}' requests materialization_mode 'live', "
                f"but connector '{connector.name}' does not support live datasets."
            )
        if materialization_mode == DatasetMaterializationMode.SYNCED:
            if not connector_capabilities.supports_synced_datasets:
                raise ValueError(
                    f"Dataset '{dataset_name}' requests materialization_mode 'synced', "
                    f"but connector '{connector.name}' does not support synced datasets."
                )
            plugin = self._host._resolve_connector_plugin_for_type(connector.connector_type)
            if plugin is None or plugin.api_connector_class is None:
                raise ValueError(
                    f"Dataset '{dataset_name}' requests materialization_mode 'synced', "
                    f"but connector '{connector.name}' does not expose a runtime sync path yet."
                )
            if source_sql or source_storage_uri:
                raise ValueError(
                    f"Dataset '{dataset_name}' requests materialization_mode 'synced', "
                    "but synced datasets must use source.resource to name the connector resource to materialize."
                )
            if not sync_resource_name:
                raise ValueError(
                    f"Dataset '{dataset_name}' requests materialization_mode 'synced', "
                    "but is missing source.resource for the connector resource name."
                )
            supported_resources = {
                str(item or "").strip()
                for item in (connector.supported_resources or [])
                if str(item or "").strip()
            }
            if supported_resources and sync_resource_name not in supported_resources:
                raise ValueError(
                    f"Dataset '{dataset_name}' requests synced resource '{sync_resource_name}', "
                    f"but connector '{connector.name}' only exposes: {', '.join(sorted(supported_resources))}."
                )
        if (
            materialization_mode == DatasetMaterializationMode.LIVE
            and (source_table or source_sql)
            and not connector_capabilities.supports_query_pushdown
        ):
            raise ValueError(
                f"Dataset '{dataset_name}' uses a live table/sql source, "
                f"but connector '{connector.name}' does not expose live query pushdown."
            )

        dataset_id = uuid.uuid4()
        dataset_alias = _dataset_sql_alias(dataset_name)
        actor_id = self._host._resolve_actor_id()
        now = datetime.now(timezone.utc)

        if materialization_mode == DatasetMaterializationMode.SYNCED:
            catalog_name = None
            schema_name = None
            table_name = dataset_alias
            relation_name = table_name
            dataset_type = "FILE"
            sql_text = None
            storage_kind = DatasetStorageKind.PARQUET.value
            source_kind = DatasetSourceKind.API.value
            dialect = "duckdb"
            storage_uri = None
            file_config = {
                "format": "parquet",
                "managed_dataset": True,
                "connector_sync": {
                    "connector_id": str(connector.id),
                    "connector_type": connector.connector_type,
                    "connector_family": connector.connector_family,
                    "resource_name": sync_resource_name,
                    "root_resource_name": sync_resource_name,
                    "parent_resource_name": None,
                },
            }
        elif source_table:
            catalog_name, schema_name, table_name = _relation_parts(source_table)
            relation_name = source_table
            dataset_type = "TABLE"
            sql_text = None
            storage_kind = DatasetStorageKind.TABLE.value
            source_kind = DatasetSourceKind.DATABASE.value
            dialect = self._host._connector_dialect(connector.connector_type or "")
            storage_uri = None
            file_config = None
        else:
            catalog_name = None
            schema_name = None
            table_name = dataset_alias
            relation_name = table_name
            if source_sql:
                dataset_type = "SQL"
                sql_text = source_sql
                storage_kind = DatasetStorageKind.VIEW.value
                source_kind = DatasetSourceKind.DATABASE.value
                dialect = self._host._connector_dialect(connector.connector_type or "")
                storage_uri = None
                file_config = None
            else:
                dataset_type = "FILE"
                sql_text = None
                storage_uri = source_storage_uri
                if not storage_uri:
                    raise ValueError(
                        f"Dataset '{dataset_name}' must define source.path or source.storage_uri for file-backed datasets."
                    )
                file_format = str(
                    normalized_request.source.format
                    or normalized_request.source.file_format
                    or (((connector.config or {}).get("config") or {}).get("format"))
                    or (((connector.config or {}).get("config") or {}).get("file_format"))
                    or ""
                ).strip().lower()
                if file_format not in {"csv", "parquet"}:
                    raise ValueError(
                        f"Dataset '{dataset_name}' must declare a supported file format (csv or parquet)."
                    )
                source_kind = DatasetSourceKind.FILE.value
                storage_kind = file_format
                dialect = "duckdb"
                file_config = {"format": file_format}
                if normalized_request.source.header is not None:
                    file_config["header"] = normalized_request.source.header
                if normalized_request.source.delimiter is not None:
                    file_config["delimiter"] = normalized_request.source.delimiter
                if normalized_request.source.quote is not None:
                    file_config["quote"] = normalized_request.source.quote

        source_kind_enum = DatasetSourceKind(source_kind)
        storage_kind_enum = DatasetStorageKind(storage_kind)
        relation_identity = build_dataset_relation_identity(
            dataset_id=dataset_id,
            connector_id=connector.id,
            dataset_name=dataset_name,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            storage_uri=storage_uri,
            source_kind=source_kind_enum,
            storage_kind=storage_kind_enum,
        )
        execution_capabilities = build_dataset_execution_capabilities(
            source_kind=source_kind_enum,
            storage_kind=storage_kind_enum,
        )

        policy_config = normalized_request.policy or LocalRuntimeDatasetPolicyConfig()
        policy = DatasetPolicyMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=self._host.context.workspace_id,
            max_rows_preview=policy_config.max_rows_preview or settings.SQL_DEFAULT_MAX_PREVIEW_ROWS,
            max_export_rows=policy_config.max_export_rows or settings.SQL_DEFAULT_MAX_EXPORT_ROWS,
            redaction_rules=dict(policy_config.redaction_rules or {}),
            row_filters=list(policy_config.row_filters or []),
            allow_dml=bool(policy_config.allow_dml),
            created_at=now,
            updated_at=now,
        )

        dataset = DatasetMetadata(
            id=dataset_id,
            workspace_id=self._host.context.workspace_id,
            connection_id=connector.id,
            owner_id=actor_id,
            created_by=actor_id,
            updated_by=actor_id,
            name=dataset_name,
            sql_alias=dataset_alias,
            description=(
                normalized_request.description
                or (
                    f"Runtime-managed synced dataset awaiting connector sync for resource '{sync_resource_name}'."
                    if materialization_mode == DatasetMaterializationMode.SYNCED
                    else None
                )
            ),
            tags=_merge_dataset_tags(
                existing=list(normalized_request.tags or []),
                required=(
                    [
                        "managed",
                        "api-connector",
                        str(connector.connector_type or "").strip().lower(),
                        f"resource:{sync_resource_name.strip().lower()}",
                    ]
                    if materialization_mode == DatasetMaterializationMode.SYNCED
                    else []
                ),
            ),
            dataset_type=dataset_type,
            materialization_mode=materialization_mode.value,
            source_kind=source_kind,
            connector_kind=(connector.connector_type or "").lower() or None,
            storage_kind=storage_kind,
            dialect=dialect,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
            storage_uri=storage_uri,
            sql_text=sql_text,
            relation_identity=relation_identity.model_dump(mode="json"),
            execution_capabilities=execution_capabilities.model_dump(mode="json"),
            referenced_dataset_ids=[],
            federated_plan=None,
            file_config=file_config,
            status=(
                "pending_sync"
                if materialization_mode == DatasetMaterializationMode.SYNCED
                else "published"
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
            existing = await self._host._dataset_repository.list_for_workspace(
                workspace_id=self._host.context.workspace_id,
                limit=1000,
                offset=0,
            )
            if any(candidate.name == dataset_name for candidate in existing):
                raise ValueError(f"Dataset '{dataset_name}' already exists.")
            existing_alias = await self._host._dataset_repository.get_for_workspace_by_sql_alias(
                workspace_id=self._host.context.workspace_id,
                sql_alias=dataset_alias,
            )
            if existing_alias is not None:
                raise ValueError(
                    f"Dataset sql_alias '{dataset_alias}' is already in use by dataset '{existing_alias.name}'."
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

        self._host._upsert_runtime_dataset_record(
            LocalRuntimeDatasetRecord(
                id=dataset.id,
                name=dataset.name,
                label=dataset.name.replace("_", " ").title(),
                description=dataset.description,
                connector_name=connector.name,
                relation_name=relation_name,
                semantic_model_name=None,
                default_time_dimension=None,
            )
        )
        return await self.get_dataset(dataset_ref=str(dataset.id))

    async def query_dataset(self, *, request) -> dict[str, Any]:
        async with self._host._runtime_operation_scope() as uow:
            payload = await self._host._runtime_host.query_dataset(request=request)
            if uow is not None:
                await uow.commit()
            return self._host._normalize_dataset_query_payload(payload)
