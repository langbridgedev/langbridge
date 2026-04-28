import uuid
from datetime import datetime, timezone
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from langbridge.connectors.base.config import ConnectorRuntimeType, ConnectorSyncStrategy
from langbridge.connectors.base.resource_paths import (
    api_resource_root,
    materialize_api_resource_rows,
    normalize_api_resource_path,
)
from langbridge.runtime.datasets.contracts import DatasetSyncPolicy
from langbridge.runtime.models import (
    ConnectorSyncState,
    DatasetColumnMetadata,
    DatasetMaterializationConfig,
    DatasetMaterializationMode,
    DatasetMetadata,
    DatasetPolicyMetadata,
    DatasetRevision,
    DatasetStatus,
    DatasetType,
)
from langbridge.runtime.models.metadata import (
    ConnectorMetadata,
    DatasetSource,
    DatasetStorageKind,
)
from langbridge.runtime.models.state import ConnectorSyncMode, ConnectorSyncStatus
from langbridge.runtime.ports import (
    ConnectorSyncStateStore,
    DatasetCatalogStore,
    DatasetColumnStore,
    DatasetPolicyStore,
    DatasetRevisionStore,
    LineageEdgeStore,
)
from langbridge.runtime.security import SecretProviderRegistry
from langbridge.runtime.services.dataset_sync.connectors import DatasetSyncConnectorFactory
from langbridge.runtime.services.dataset_sync.lineage import DatasetSyncLineageWriter
from langbridge.runtime.services.dataset_sync.materialization import DatasetMaterializer
from langbridge.runtime.services.dataset_sync.metadata import DatasetSyncMetadataBuilder
from langbridge.runtime.services.dataset_sync.sources import (
    DatasetSyncSourceResolver,
    enum_value,
    relation_parts,
)
from langbridge.runtime.services.dataset_sync.types import MaterializedDatasetResult
from langbridge.runtime.utils.lineage import stable_payload_hash


async def _flush_stores(*stores: Any) -> None:
    for store in stores:
        flush = getattr(store, "flush", None)
        if callable(flush):
            await flush()


class ConnectorSyncRuntime:
    """Coordinates connector extraction, parquet materialization, and metadata updates."""

    def __init__(
        self,
        *,
        connector_sync_state_repository: ConnectorSyncStateStore,
        dataset_repository: DatasetCatalogStore,
        dataset_column_repository: DatasetColumnStore,
        dataset_policy_repository: DatasetPolicyStore,
        dataset_revision_repository: DatasetRevisionStore | None = None,
        lineage_edge_repository: LineageEdgeStore | None = None,
        secret_provider_registry: SecretProviderRegistry | None = None,
    ) -> None:
        self._connector_sync_state_repository = connector_sync_state_repository
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._dataset_policy_repository = dataset_policy_repository
        self._dataset_revision_repository = dataset_revision_repository
        self._secret_provider_registry = secret_provider_registry or SecretProviderRegistry()

        self._source_resolver = DatasetSyncSourceResolver()
        self._connector_factory = DatasetSyncConnectorFactory(
            secret_provider_registry=self._secret_provider_registry,
        )
        self._materializer = DatasetMaterializer()
        self._metadata_builder = DatasetSyncMetadataBuilder(
            source_resolver=self._source_resolver,
        )
        self._lineage_writer = DatasetSyncLineageWriter(
            lineage_edge_repository=lineage_edge_repository,
        )

    def __getattr__(self, name: str) -> Any:
        """Expose legacy private helper names for tests and downstream extensions."""

        helper_methods = {
            "_sync_source": (self._source_resolver, "sync_source"),
            "_sync_source_key": (self._source_resolver, "sync_source_key"),
            "_sync_source_kind": (self._source_resolver, "sync_source_kind"),
            "_sync_source_payload": (self._source_resolver, "sync_source_payload"),
            "_sync_source_label": (self._source_resolver, "sync_source_label"),
            "_request_display_path": (self._source_resolver, "request_display_path"),
            "_request_signature": (self._source_resolver, "request_signature"),
            "_request_resource_path": (self._source_resolver, "request_resource_path"),
            "_dataset_parquet_path": (self._materializer, "dataset_parquet_path"),
            "_read_existing_rows": (self._materializer, "read_existing_rows"),
            "_merge_rows": (self._materializer, "merge_rows"),
            "_rows_to_table": (self._materializer, "rows_to_table"),
            "_ensure_pyarrow_compatible_pandas_stub": (
                self._materializer,
                "ensure_pyarrow_compatible_pandas_stub",
            ),
            "_normalize_rows_for_arrow": (self._materializer, "normalize_rows_for_arrow"),
            "_value_category": (self._materializer, "value_category"),
            "_row_identity": (self._materializer, "row_identity"),
            "_child_primary_key": (self._materializer, "child_primary_key"),
            "_materialization_primary_key": (
                self._materializer,
                "materialization_primary_key",
            ),
            "_pick_newer_cursor": (self._materializer, "pick_newer_cursor"),
            "_describe_schema_drift": (self._materializer, "describe_schema_drift"),
            "_sql_literal": (self._materializer, "sql_literal"),
            "_resolve_next_sql_cursor": (self._materializer, "resolve_next_sql_cursor"),
            "_column_snapshot": (self._metadata_builder, "column_snapshot"),
            "_policy_snapshot": (self._metadata_builder, "policy_snapshot"),
            "_build_dataset_definition_snapshot": (
                self._metadata_builder,
                "build_dataset_definition_snapshot",
            ),
            "_build_dataset_source_bindings": (
                self._metadata_builder,
                "build_dataset_source_bindings",
            ),
            "_resolve_dataset_descriptor_snapshot": (
                self._metadata_builder,
                "resolve_dataset_descriptor_snapshot",
            ),
            "_apply_dataset_descriptor_metadata": (
                self._metadata_builder,
                "apply_dataset_descriptor_metadata",
            ),
            "_dataset_description": (self._metadata_builder, "dataset_description"),
            "_dataset_sql_alias": (self._metadata_builder, "dataset_sql_alias"),
            "_dataset_tags": (self._metadata_builder, "dataset_tags"),
            "_merge_tags": (self._metadata_builder, "merge_tags"),
            "_sync_meta": (self._metadata_builder, "sync_meta"),
            "_replace_dataset_lineage": (self._lineage_writer, "replace_dataset_lineage"),
        }
        target = helper_methods.get(name)
        if target is None:
            raise AttributeError(f"{type(self).__name__!s} object has no attribute {name!r}")
        owner, method_name = target
        return getattr(owner, method_name)

    async def get_or_create_state(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        connector_type: ConnectorRuntimeType,
        resource_name: str,
        sync_mode: Any,
    ) -> ConnectorSyncState:
        sync_mode_value = ConnectorSyncMode(enum_value(sync_mode).upper())
        state = await self._connector_sync_state_repository.get_for_resource(
            workspace_id=workspace_id,
            connection_id=connection_id,
            resource_name=resource_name,
        )
        if state is not None:
            return state
        state = ConnectorSyncState(
            id=uuid.uuid4(),
            workspace_id=workspace_id,
            connection_id=connection_id,
            connector_type=connector_type,
            source_key=resource_name,
            source={},
            sync_mode=sync_mode_value,
            last_cursor=None,
            last_sync_at=None,
            state={},
            status=ConnectorSyncStatus.NEVER_SYNCED,
            error_message=None,
            records_synced=0,
            bytes_synced=None,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self._connector_sync_state_repository.add(state)
        return state

    def _build_api_connector(self, connector_record: ConnectorMetadata) -> Any:
        return self._connector_factory.build_api_connector(connector_record)

    def _build_sql_connector(self, connector_record: ConnectorMetadata) -> Any:
        return self._connector_factory.build_sql_connector(connector_record)

    async def _resolve_api_root_resource(
        self,
        *,
        dataset: DatasetMetadata,
        connector: ConnectorMetadata,
        api_connector: Any,
        resource_name: str,
    ) -> Any:
        return await self._connector_factory.resolve_api_root_resource(
            dataset=dataset,
            connector=connector,
            api_connector=api_connector,
            resource_name=resource_name,
        )

    async def sync_dataset(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID,
        connector_record: ConnectorMetadata,
        dataset: DatasetMetadata,
        sync_mode: ConnectorSyncMode,
        max_sync_retry: int = 3,
    ) -> dict[str, Any]:
        if connector_record.connector_type is None:
            raise ValueError(f"Connector '{connector_record.name}' is missing connector_type.")

        connection_id = connector_record.id
        connector_type = connector_record.connector_type
        normalized_sync_mode = ConnectorSyncMode(enum_value(sync_mode).upper())
        sync_source = self._source_resolver.sync_source(dataset)
        source_payload = self._source_resolver.sync_source_payload(sync_source)
        source_key = self._source_resolver.sync_source_key(sync_source)
        state = await self.get_or_create_state(
            workspace_id=workspace_id,
            connection_id=connection_id,
            connector_type=connector_type,
            resource_name=source_key,
            sync_mode=normalized_sync_mode,
        )
        state.status = ConnectorSyncStatus.RUNNING
        state.sync_mode = normalized_sync_mode
        state.error_message = None
        state.updated_at = datetime.now(timezone.utc)
        state.source_key = source_key
        state.source_kind = self._source_resolver.sync_source_kind(sync_source)
        state.source = source_payload
        await self._connector_sync_state_repository.save(state)

        if sync_source.resource or sync_source.request:
            return await self._sync_api_dataset(
                workspace_id=workspace_id,
                actor_id=actor_id,
                connector_record=connector_record,
                dataset=dataset,
                sync_source=sync_source,
                source_payload=source_payload,
                source_key=source_key,
                state=state,
                normalized_sync_mode=normalized_sync_mode,
                max_sync_retry=max_sync_retry,
            )

        if sync_source.table or sync_source.sql:
            return await self._sync_sql_dataset(
                actor_id=actor_id,
                connector_record=connector_record,
                dataset=dataset,
                sync_source=sync_source,
                source_payload=source_payload,
                source_key=source_key,
                state=state,
                normalized_sync_mode=normalized_sync_mode,
            )

        raise ValueError(
            f"Dataset '{dataset.name}' uses unsupported sync.source shape. "
            "Supported synced sources are resource, request, table, and sql."
        )

    async def _sync_api_dataset(
        self,
        *,
        workspace_id: uuid.UUID,
        actor_id: uuid.UUID,
        connector_record: ConnectorMetadata,
        dataset: DatasetMetadata,
        sync_source: DatasetSource,
        source_payload: dict[str, Any],
        source_key: str,
        state: ConnectorSyncState,
        normalized_sync_mode: ConnectorSyncMode,
        max_sync_retry: int,
    ) -> dict[str, Any]:
        connection_id = connector_record.id
        connector_type = connector_record.connector_type
        api_connector = self._build_api_connector(connector_record)
        page_count = 0
        extracted_records: list[dict[str, Any]] = []
        checkpoint_cursor = state.last_cursor
        if sync_source.resource:
            await api_connector.test_connection()
            resource_path = normalize_api_resource_path(str(sync_source.resource).strip())
            resolved_resource = await self._resolve_api_root_resource(
                dataset=dataset,
                connector=connector_record,
                api_connector=api_connector,
                resource_name=api_resource_root(resource_path),
            )
            effective_sync_mode = normalized_sync_mode
            if (
                normalized_sync_mode == ConnectorSyncMode.INCREMENTAL
                and not resolved_resource.supports_incremental
            ):
                effective_sync_mode = ConnectorSyncMode.FULL_REFRESH

            since = None
            if (
                effective_sync_mode == ConnectorSyncMode.INCREMENTAL
                and resolved_resource.supports_incremental
            ):
                since = state.last_cursor

            page_cursor: str | None = None
            for _ in range(max_sync_retry):
                extract_result = await api_connector.extract_resource(
                    resolved_resource.name,
                    since=since,
                    cursor=page_cursor,
                    limit=None,
                )
                extracted_records.extend(list(extract_result.records or []))
                checkpoint_cursor = self._materializer.pick_newer_cursor(
                    checkpoint_cursor,
                    extract_result.checkpoint_cursor,
                )
                page_count += 1
                page_cursor = extract_result.next_cursor
                if not page_cursor:
                    break
            root_resource_name = resolved_resource.name
            root_primary_key = resolved_resource.primary_key
            supports_incremental = resolved_resource.supports_incremental
        else:
            resource_path = normalize_api_resource_path(
                self._source_resolver.request_resource_path(sync_source)
            )
            extract_result = await api_connector.extract_request(
                sync_source.request.model_dump(mode="json", exclude_none=True),  # type: ignore[union-attr]
                since=(
                    state.last_cursor
                    if normalized_sync_mode == ConnectorSyncMode.INCREMENTAL
                    else None
                ),
                cursor=None,
                limit=None,
                extraction=(
                    sync_source.extraction.model_dump(mode="json", exclude_none=True)
                    if sync_source.extraction is not None
                    else None
                ),
            )
            extracted_records.extend(list(extract_result.records or []))
            checkpoint_cursor = self._materializer.pick_newer_cursor(
                checkpoint_cursor,
                extract_result.checkpoint_cursor,
            )
            page_count += 1
            effective_sync_mode = ConnectorSyncMode.FULL_REFRESH
            root_resource_name = api_resource_root(resource_path)
            root_primary_key = None
            supports_incremental = False

        now = datetime.now(timezone.utc)
        materialized_rows = materialize_api_resource_rows(
            resource_path=resource_path,
            records=extracted_records,
            primary_key=root_primary_key,
            flatten=source_payload.get("flatten"),
        )
        materialized = await self._materialize_existing_dataset(
            actor_id=actor_id,
            connection_id=connection_id,
            connector_record=connector_record,
            connector_type=connector_type,
            dataset=dataset,
            sync_source=sync_source,
            source_key=source_key,
            rows=materialized_rows.rows,
            primary_key=self._materializer.materialization_primary_key(
                resource_path=resource_path,
                root_resource_name=root_resource_name,
                root_primary_key=root_primary_key,
                rows=materialized_rows.rows,
            ),
            sync_mode=effective_sync_mode,
        )

        state.sync_mode = effective_sync_mode
        state.last_cursor = (
            checkpoint_cursor
            if effective_sync_mode == ConnectorSyncMode.INCREMENTAL and supports_incremental
            else state.last_cursor
        )
        state.last_sync_at = now
        state.status = ConnectorSyncStatus.SUCCEEDED
        state.error_message = None
        state.records_synced = len(materialized_rows.rows)
        state.bytes_synced = materialized.bytes_written
        state.state = {
            "page_count": page_count,
            "resource_path": resource_path,
            "root_resource_name": root_resource_name,
            "dataset_id": str(materialized.dataset_id),
            "dataset_name": materialized.dataset_name,
            "cardinality": materialized_rows.cardinality.value,
            "schema_drift": materialized.schema_drift,
            "child_resources": [
                {
                    "name": child.name,
                    "path": child.path,
                    "parent_path": child.parent_path,
                    "cardinality": child.cardinality.value,
                    "supports_flattening": child.supports_flattening,
                    "addressable": child.addressable,
                }
                for child in materialized_rows.child_resources
            ],
            "last_sync_at": now.isoformat(),
        }
        state.updated_at = now
        await self._connector_sync_state_repository.save(state)
        return {
            "source_key": source_key,
            "source": source_payload,
            "resource_name": resource_path,
            "root_resource_name": root_resource_name,
            "sync_mode": enum_value(effective_sync_mode),
            "records_synced": int(state.records_synced or 0),
            "bytes_synced": materialized.bytes_written,
            "last_cursor": state.last_cursor,
            "dataset_ids": [str(materialized.dataset_id)],
            "dataset_names": [materialized.dataset_name],
        }

    async def _sync_sql_dataset(
        self,
        *,
        actor_id: uuid.UUID,
        connector_record: ConnectorMetadata,
        dataset: DatasetMetadata,
        sync_source: DatasetSource,
        source_payload: dict[str, Any],
        source_key: str,
        state: ConnectorSyncState,
        normalized_sync_mode: ConnectorSyncMode,
    ) -> dict[str, Any]:
        sql_connector = self._build_sql_connector(connector_record)
        await sql_connector.test_connection()
        source_query = (
            f"SELECT * FROM {str(sync_source.table).strip()}"
            if sync_source.table
            else str(sync_source.sql).strip()
        )
        effective_sync_mode = normalized_sync_mode
        if (
            normalized_sync_mode == ConnectorSyncMode.INCREMENTAL
            and state.last_cursor is not None
            and str(dataset.sync.cursor_field or "").strip()
        ):
            cursor_field = str(dataset.sync.cursor_field or "").strip()
            cursor_literal = self._materializer.sql_literal(state.last_cursor)
            wrapped = f"SELECT * FROM ({source_query}) AS langbridge_sync_source"
            source_query = f"{wrapped} WHERE {cursor_field} >= {cursor_literal}"
        result = await sql_connector.execute(
            source_query,
            params={},
            max_rows=None,
            timeout_s=30,
        )
        rows = [
            {
                str(column): (raw_row[index] if index < len(raw_row) else None)
                for index, column in enumerate(result.columns)
            }
            for raw_row in result.rows
        ]
        primary_key = await self._resolve_sql_primary_key(
            sql_connector=sql_connector,
            sync_source=sync_source,
            rows=rows,
        )
        materialized = await self._materialize_existing_dataset(
            actor_id=actor_id,
            connection_id=connector_record.id,
            connector_record=connector_record,
            connector_type=connector_record.connector_type,
            dataset=dataset,
            sync_source=sync_source,
            source_key=source_key,
            rows=rows,
            primary_key=primary_key,
            sync_mode=effective_sync_mode,
        )
        now = datetime.now(timezone.utc)
        state.sync_mode = effective_sync_mode
        state.last_cursor = self._materializer.resolve_next_sql_cursor(
            rows=rows,
            cursor_field=str(dataset.sync.cursor_field or "").strip() or None,
            current_cursor=state.last_cursor,
            sync_mode=effective_sync_mode,
        )
        state.last_sync_at = now
        state.status = ConnectorSyncStatus.SUCCEEDED
        state.error_message = None
        state.records_synced = len(rows)
        state.bytes_synced = materialized.bytes_written
        state.state = {
            "query_sql": result.sql,
            "row_count": len(rows),
            "source_label": self._source_resolver.sync_source_label(sync_source),
            "schema_drift": materialized.schema_drift,
            "dataset_id": str(materialized.dataset_id),
            "dataset_name": materialized.dataset_name,
            "last_sync_at": now.isoformat(),
        }
        state.updated_at = now
        await self._connector_sync_state_repository.save(state)
        return {
            "source_key": source_key,
            "source": source_payload,
            "sync_mode": enum_value(effective_sync_mode),
            "records_synced": int(state.records_synced or 0),
            "bytes_synced": materialized.bytes_written,
            "last_cursor": state.last_cursor,
            "dataset_ids": [str(materialized.dataset_id)],
            "dataset_names": [materialized.dataset_name],
        }

    async def mark_failed(self, *, state: ConnectorSyncState, error_message: str) -> None:
        state.status = ConnectorSyncStatus.FAILED
        state.error_message = error_message
        state.updated_at = datetime.now(timezone.utc)
        await self._connector_sync_state_repository.save(state)

    async def _materialize_existing_dataset(
        self,
        *,
        actor_id: uuid.UUID,
        connection_id: uuid.UUID,
        connector_record: ConnectorMetadata,
        connector_type: ConnectorRuntimeType,
        dataset: DatasetMetadata,
        sync_source: DatasetSource,
        source_key: str,
        rows: list[dict[str, Any]],
        primary_key: str | None,
        sync_mode: ConnectorSyncMode,
    ) -> MaterializedDatasetResult:
        normalized_sync_mode = ConnectorSyncMode(enum_value(sync_mode).upper())
        parquet_path = self._materializer.dataset_parquet_path(
            workspace_id=dataset.workspace_id,
            connection_id=connection_id,
            dataset_name=dataset.name,
        )
        existing_rows, existing_schema = self._materializer.read_existing_rows(parquet_path)
        merged_rows = self._materializer.merge_rows(
            existing_rows=existing_rows,
            new_rows=rows,
            primary_key=primary_key,
            full_refresh=normalized_sync_mode == ConnectorSyncMode.FULL_REFRESH,
        )
        table = self._materializer.rows_to_table(
            rows=merged_rows,
            existing_schema=existing_schema,
        )
        schema_drift = self._materializer.describe_schema_drift(
            existing_schema=existing_schema,
            next_schema=table.schema,
        )
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        pq.write_table(table, parquet_path)
        storage_uri = parquet_path.resolve().as_uri()
        bytes_written = parquet_path.stat().st_size if parquet_path.exists() else None
        now = datetime.now(timezone.utc)

        file_config = {
            "format": "parquet",
            "managed_dataset": True,
        }

        existing_sync = dataset.sync
        if existing_sync is None:
            raise ValueError(f"Dataset '{dataset.name}' is missing a sync contract.")
        previously_materialized = bool(str(dataset.storage_uri or "").strip())
        dataset.connection_id = connection_id
        dataset.updated_by = actor_id
        if not str(dataset.description or "").strip():
            dataset.description = self._metadata_builder.dataset_description(
                connector_record.name,
                self._source_resolver.sync_source_label(sync_source),
            )
        dataset.tags = self._metadata_builder.merge_tags(
            existing=list(dataset.tags_json or []),
            required=self._metadata_builder.dataset_tags(
                connector_type=connector_type,
                source=sync_source,
            ),
        )
        dataset.dataset_type = DatasetType.FILE
        dataset.source = DatasetSource.model_validate(
            sync_source.model_dump(mode="json", exclude_none=True)
        )
        dataset.materialization = DatasetMaterializationConfig(
            mode=DatasetMaterializationMode.SYNCED,
            sync=DatasetSyncPolicy(
                strategy=existing_sync.strategy or ConnectorSyncStrategy(enum_value(normalized_sync_mode)),
                cadence=existing_sync.cadence,
                sync_on_start=bool(existing_sync.sync_on_start),
                cursor_field=existing_sync.cursor_field,
                initial_cursor=existing_sync.initial_cursor,
                lookback_window=existing_sync.lookback_window,
                backfill_start=existing_sync.backfill_start,
                backfill_end=existing_sync.backfill_end,
            ),
        )
        dataset.source_kind = self._source_resolver.sync_source_kind(sync_source)
        dataset.connector_kind = connector_type.value.lower()
        dataset.storage_kind = DatasetStorageKind.PARQUET
        dataset.dialect = "duckdb"
        dataset.schema_name = None
        dataset.table_name = dataset.table_name or self._metadata_builder.dataset_sql_alias(dataset.name)
        dataset.storage_uri = storage_uri
        dataset.file_config = file_config
        dataset.status = DatasetStatus.PUBLISHED
        dataset.row_count_estimate = len(merged_rows)
        dataset.bytes_estimate = bytes_written
        dataset.updated_at = now
        self._metadata_builder.apply_dataset_descriptor_metadata(dataset=dataset)
        if not previously_materialized:
            change_summary = (
                f"Initial sync materialized dataset '{dataset.name}' from "
                f"{self._source_resolver.sync_source_label(sync_source)}."
            )
        else:
            change_summary = (
                f"{self._materializer.sync_mode_label(normalized_sync_mode)} sync updated dataset "
                f"'{dataset.name}' from {self._source_resolver.sync_source_label(sync_source)}."
            )

        await self._replace_columns(dataset=dataset, table=table)
        policy = await self._get_or_create_policy(dataset=dataset)
        await self._create_dataset_revision(
            dataset=dataset,
            policy=policy,
            created_by=actor_id,
            change_summary=change_summary,
        )
        await self._lineage_writer.replace_dataset_lineage(dataset=dataset)
        await self._dataset_repository.save(dataset)

        return MaterializedDatasetResult(
            dataset_id=dataset.id,
            dataset_name=dataset.name,
            source_key=source_key,
            row_count=len(merged_rows),
            bytes_written=bytes_written,
            schema_drift=schema_drift,
        )

    async def _replace_columns(self, *, dataset: DatasetMetadata, table: pa.Table) -> None:
        await self._dataset_column_repository.delete_for_dataset(dataset_id=dataset.id)
        await _flush_stores(self._dataset_column_repository)
        now = datetime.now(timezone.utc)
        for ordinal, field in enumerate(table.schema):
            self._dataset_column_repository.add(
                DatasetColumnMetadata(
                    id=uuid.uuid4(),
                    dataset_id=dataset.id,
                    workspace_id=dataset.workspace_id,
                    name=str(field.name),
                    data_type=str(field.type),
                    nullable=field.nullable,
                    ordinal_position=ordinal,
                    description=None,
                    is_allowed=True,
                    is_computed=False,
                    expression=None,
                    created_at=now,
                    updated_at=now,
                )
            )

    async def _get_or_create_policy(self, *, dataset: DatasetMetadata) -> DatasetPolicyMetadata:
        existing = await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
        if existing is not None:
            existing.allow_dml = False
            existing.updated_at = datetime.now(timezone.utc)
            await self._dataset_policy_repository.save(existing)
            return existing
        policy = DatasetPolicyMetadata(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=dataset.workspace_id,
            max_rows_preview=1000,
            max_export_rows=100000,
            redaction_rules={},
            row_filters=[],
            allow_dml=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self._dataset_policy_repository.add(policy)
        return policy

    async def _create_dataset_revision(
        self,
        *,
        dataset: DatasetMetadata,
        policy: DatasetPolicyMetadata,
        created_by: uuid.UUID,
        change_summary: str,
    ) -> None:
        if self._dataset_revision_repository is None:
            return
        await _flush_stores(
            self._dataset_repository,
            self._dataset_column_repository,
            self._dataset_policy_repository,
        )
        columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
        next_revision = await self._dataset_revision_repository.next_revision_number(
            dataset_id=dataset.id
        )
        definition = self._metadata_builder.build_dataset_definition_snapshot(dataset)
        schema_snapshot = [self._metadata_builder.column_snapshot(column) for column in columns]
        policy_snapshot = self._metadata_builder.policy_snapshot(policy)
        source_bindings = self._metadata_builder.build_dataset_source_bindings(dataset)
        relation_identity, execution_capabilities = (
            self._metadata_builder.resolve_dataset_descriptor_snapshot(dataset)
        )
        execution_characteristics = {
            "row_count_estimate": dataset.row_count_estimate,
            "bytes_estimate": dataset.bytes_estimate,
            "last_profiled_at": dataset.last_profiled_at.isoformat() if dataset.last_profiled_at else None,
            "relation_identity": relation_identity,
            "execution_capabilities": execution_capabilities,
        }
        snapshot = {
            "dataset": definition,
            "columns": schema_snapshot,
            "policy": policy_snapshot,
            "source_bindings": source_bindings,
            "execution_characteristics": execution_characteristics,
        }
        revision_id = uuid.uuid4()
        self._dataset_revision_repository.add(
            DatasetRevision(
                id=revision_id,
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                revision_number=next_revision,
                revision_hash=stable_payload_hash(snapshot),
                change_summary=change_summary,
                definition=definition,
                schema_snapshot=schema_snapshot,
                policy=policy_snapshot,
                source_bindings=source_bindings,
                execution_characteristics=execution_characteristics,
                status=dataset.status_value,
                snapshot=snapshot,
                note=change_summary,
                created_by=created_by,
                created_at=datetime.now(timezone.utc),
            )
        )
        dataset.revision_id = revision_id

    async def _resolve_sql_primary_key(
        self,
        *,
        sql_connector: Any,
        sync_source: DatasetSource,
        rows: list[dict[str, Any]],
    ) -> str | None:
        if sync_source.table:
            try:
                _, schema_name, table_name = relation_parts(str(sync_source.table).strip())
                columns = await sql_connector.fetch_columns(schema_name or "public", table_name)
                for column in columns:
                    if bool(getattr(column, "is_primary_key", False)):
                        return str(getattr(column, "name"))
            except Exception:
                return None
        if rows and any("id" in row for row in rows):
            return "id"
        return None


DatasetSyncService = ConnectorSyncRuntime
