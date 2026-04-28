import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import pyarrow.parquet as pq
import pytest

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import ApiExtractResult, ApiResource, QueryResult
from langbridge.connectors.base.metadata import ColumnMetadata
from langbridge.runtime.models import ConnectorMetadata, DatasetMetadata
from langbridge.runtime.models.metadata import (
    DatasetMaterializationMode,
    DatasetSourceKind,
    DatasetStatus,
    DatasetStorageKind,
    DatasetType,
    LifecycleState,
    ManagementMode,
)
from langbridge.runtime.persistence.db import agent as _db_agent  # noqa: F401
from langbridge.runtime.persistence.db import semantic as _db_semantic  # noqa: F401
from langbridge.runtime.persistence.db.connector_sync import ConnectorSyncStateRecord
from langbridge.runtime.persistence.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)
from langbridge.runtime.persistence.db.lineage import LineageEdgeRecord
from langbridge.runtime.services.dataset_sync import ConnectorSyncRuntime
from langbridge.runtime.settings import runtime_settings
from langbridge.runtime.utils.lineage import stable_payload_hash


SYNC_MODE_INCREMENTAL = "INCREMENTAL"
SYNC_MODE_FULL_REFRESH = "FULL_REFRESH"
SYNC_STATUS_SUCCEEDED = "succeeded"
SYNC_STATUS_FAILED = "failed"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@pytest.fixture
def dataset_storage_dir(tmp_path: Path):
    original_dataset_dir = runtime_settings.DATASET_FILE_LOCAL_DIR
    object.__setattr__(
        runtime_settings,
        "DATASET_FILE_LOCAL_DIR",
        str((tmp_path / "datasets").resolve()),
    )
    try:
        yield tmp_path / "datasets"
    finally:
        object.__setattr__(runtime_settings, "DATASET_FILE_LOCAL_DIR", original_dataset_dir)


class _FakeConnectorSyncStateRepository:
    def __init__(self) -> None:
        self.items: dict[tuple[uuid.UUID, uuid.UUID, str], ConnectorSyncStateRecord] = {}

    def add(self, state: ConnectorSyncStateRecord) -> None:
        self.items[(state.workspace_id, state.connection_id, state.source_key)] = state

    async def save(self, state: ConnectorSyncStateRecord) -> ConnectorSyncStateRecord:
        self.add(state)
        return state

    async def get_for_resource(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        resource_name: str,
    ) -> ConnectorSyncStateRecord | None:
        return self.items.get((workspace_id, connection_id, resource_name))

    async def list_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
    ) -> list[ConnectorSyncStateRecord]:
        return [
            state
            for (row_workspace_id, row_connection_id, _), state in self.items.items()
            if row_workspace_id == workspace_id and row_connection_id == connection_id
        ]


class _FakeDatasetRepository:
    def __init__(self) -> None:
        self.items: dict[uuid.UUID, DatasetRecord] = {}

    def add(self, dataset: DatasetRecord) -> None:
        self.items[dataset.id] = dataset

    async def save(self, dataset: DatasetRecord) -> DatasetRecord:
        self.add(dataset)
        return dataset

    async def find_file_dataset_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        table_name: str,
    ) -> DatasetRecord | None:
        for dataset in self.items.values():
            if (
                dataset.workspace_id == workspace_id
                and dataset.connection_id == connection_id
                and dataset.dataset_type == "FILE"
                and str(dataset.materialization_mode or "").strip().lower() == "synced"
                and dataset.table_name == table_name
            ):
                return dataset
        return None

    async def list_for_connection(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        dataset_types: list[str] | None = None,
        limit: int = 500,
    ) -> list[DatasetRecord]:
        rows = [
            dataset
            for dataset in self.items.values()
            if dataset.workspace_id == workspace_id and dataset.connection_id == connection_id
        ]
        if dataset_types:
            allowed = {item.upper() for item in dataset_types}
            rows = [dataset for dataset in rows if dataset.dataset_type.upper() in allowed]
        rows.sort(key=lambda dataset: dataset.updated_at, reverse=True)
        return rows[:limit]


class _FakeDatasetColumnRepository:
    def __init__(self) -> None:
        self.by_dataset: dict[uuid.UUID, list[DatasetColumnRecord]] = {}

    def add(self, column: DatasetColumnRecord) -> None:
        self.by_dataset.setdefault(column.dataset_id, []).append(column)

    async def list_for_dataset(self, *, dataset_id: uuid.UUID) -> list[DatasetColumnRecord]:
        rows = list(self.by_dataset.get(dataset_id, []))
        rows.sort(key=lambda item: (item.ordinal_position, item.name))
        return rows

    async def delete_for_dataset(self, *, dataset_id: uuid.UUID) -> None:
        self.by_dataset[dataset_id] = []


class _FakeDatasetPolicyRepository:
    def __init__(self) -> None:
        self.by_dataset: dict[uuid.UUID, DatasetPolicyRecord] = {}

    def add(self, policy: DatasetPolicyRecord) -> None:
        self.by_dataset[policy.dataset_id] = policy

    async def save(self, policy: DatasetPolicyRecord) -> DatasetPolicyRecord:
        self.add(policy)
        return policy

    async def get_for_dataset(self, *, dataset_id: uuid.UUID) -> DatasetPolicyRecord | None:
        return self.by_dataset.get(dataset_id)


class _FakeDatasetRevisionRepository:
    def __init__(self) -> None:
        self.by_dataset: dict[uuid.UUID, list[DatasetRevisionRecord]] = {}

    def add(self, revision: DatasetRevisionRecord) -> None:
        self.by_dataset.setdefault(revision.dataset_id, []).append(revision)

    async def next_revision_number(self, *, dataset_id: uuid.UUID) -> int:
        rows = self.by_dataset.get(dataset_id) or []
        if not rows:
            return 1
        return max(row.revision_number for row in rows) + 1


class _FakeLineageEdgeRepository:
    def __init__(self) -> None:
        self.items: list[LineageEdgeRecord] = []

    def add(self, edge: LineageEdgeRecord) -> None:
        self.items.append(edge)

    async def delete_for_target(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: str,
        target_id: str,
    ) -> None:
        self.items = [
            edge
            for edge in self.items
            if not (
                edge.workspace_id == workspace_id
                and edge.target_type == target_type
                and edge.target_id == target_id
            )
        ]


class _ApiQueueConnector:
    def __init__(self, resource: ApiResource, *responses: ApiExtractResult | Exception) -> None:
        self._resource = resource
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []
        self.request_calls: list[dict[str, Any]] = []

    async def test_connection(self) -> None:
        return None

    async def discover_resources(self) -> list[ApiResource]:
        return [self._resource]

    async def extract_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiExtractResult:
        self.calls.append(
            {
                "resource_name": resource_name,
                "since": since,
                "cursor": cursor,
                "limit": limit,
            }
        )
        if not self._responses:
            raise AssertionError("No queued API response available.")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def extract_request(
        self,
        request: dict[str, Any],
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
        extraction: dict[str, Any] | None = None,
    ) -> ApiExtractResult:
        self.request_calls.append(
            {
                "request": dict(request),
                "since": since,
                "cursor": cursor,
                "limit": limit,
                "extraction": dict(extraction or {}),
            }
        )
        if not self._responses:
            raise AssertionError("No queued API response available.")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response


class _SqlQueueConnector:
    def __init__(
        self,
        *responses: QueryResult | Exception,
        columns: list[ColumnMetadata] | None = None,
    ) -> None:
        self._responses = list(responses)
        self._columns = list(columns or [])
        self.calls: list[dict[str, Any]] = []
        self.fetch_columns_calls: list[dict[str, Any]] = []

    async def test_connection(self) -> None:
        return None

    async def execute(
        self,
        sql: str,
        *,
        params: dict[str, Any],
        max_rows: int | None,
        timeout_s: int | None,
    ) -> QueryResult:
        self.calls.append(
            {
                "sql": sql,
                "params": dict(params),
                "max_rows": max_rows,
                "timeout_s": timeout_s,
            }
        )
        if not self._responses:
            raise AssertionError("No queued SQL response available.")
        response = self._responses.pop(0)
        if isinstance(response, Exception):
            raise response
        return response

    async def fetch_columns(self, schema: str, table: str) -> list[ColumnMetadata]:
        self.fetch_columns_calls.append({"schema": schema, "table": table})
        return list(self._columns)


def _build_runtime() -> tuple[
    ConnectorSyncRuntime,
    _FakeConnectorSyncStateRepository,
    _FakeDatasetRepository,
    _FakeDatasetRevisionRepository,
    _FakeLineageEdgeRepository,
]:
    state_repository = _FakeConnectorSyncStateRepository()
    dataset_repository = _FakeDatasetRepository()
    dataset_column_repository = _FakeDatasetColumnRepository()
    dataset_policy_repository = _FakeDatasetPolicyRepository()
    dataset_revision_repository = _FakeDatasetRevisionRepository()
    lineage_edge_repository = _FakeLineageEdgeRepository()
    runtime = ConnectorSyncRuntime(
        connector_sync_state_repository=state_repository,
        dataset_repository=dataset_repository,
        dataset_column_repository=dataset_column_repository,
        dataset_policy_repository=dataset_policy_repository,
        dataset_revision_repository=dataset_revision_repository,
        lineage_edge_repository=lineage_edge_repository,
    )
    return (
        runtime,
        state_repository,
        dataset_repository,
        dataset_revision_repository,
        lineage_edge_repository,
    )


def _resource(
    *,
    name: str = "orders",
    primary_key: str | None = "id",
    supports_incremental: bool = True,
    incremental_cursor_field: str | None = "updated_at",
) -> ApiResource:
    return ApiResource(
        name=name,
        label=name.title(),
        path=name,
        primary_key=primary_key,
        cursor_field="page_info" if supports_incremental else None,
        incremental_cursor_field=incremental_cursor_field,
        supports_incremental=supports_incremental,
        default_sync_mode="INCREMENTAL" if supports_incremental else "FULL_REFRESH",
    )


def _sync_source_kind(sync_source: dict[str, Any]) -> DatasetSourceKind:
    if sync_source.get("resource") or sync_source.get("request"):
        return DatasetSourceKind.API
    if sync_source.get("table") or sync_source.get("sql"):
        return DatasetSourceKind.DATABASE
    return DatasetSourceKind.FILE


def _declared_synced_dataset(
    *,
    workspace_id: uuid.UUID,
    actor_id: uuid.UUID,
    connection_id: uuid.UUID,
    connector_type: ConnectorRuntimeType,
    name: str,
    sync_source: dict[str, Any],
    strategy: str = SYNC_MODE_INCREMENTAL,
    cursor_field: str | None = None,
) -> DatasetMetadata:
    now = datetime.now(timezone.utc)
    sync_config: dict[str, Any] = {
        "source": dict(sync_source),
        "strategy": strategy,
    }
    if cursor_field:
        sync_config["cursor_field"] = cursor_field
    return DatasetMetadata(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=connection_id,
        created_by=actor_id,
        updated_by=actor_id,
        name=name,
        sql_alias=name,
        description=f"Declared synced dataset for source {sync_source}.",
        tags=[],
        dataset_type=DatasetType.FILE,
        materialization_mode=DatasetMaterializationMode.SYNCED,
        source_kind=_sync_source_kind(sync_source),
        connector_kind=connector_type.value.lower(),
        storage_kind=DatasetStorageKind.PARQUET,
        dialect="duckdb",
        catalog_name=None,
        schema_name=None,
        table_name=name,
        storage_uri=None,
        sql_text=None,
        source=None,
        sync=sync_config,
        relation_identity=None,
        execution_capabilities=None,
        referenced_dataset_ids=[],
        federated_plan=None,
        file_config={
            "format": "parquet",
            "managed_dataset": True,
        },
        status=DatasetStatus.PENDING_SYNC,
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        columns=[],
        policy=None,
        created_at=now,
        updated_at=now,
        management_mode=ManagementMode.CONFIG_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )


def _connector_record(
    *,
    connection_id: uuid.UUID,
    workspace_id: uuid.UUID,
    name: str,
    connector_type: ConnectorRuntimeType,
) -> ConnectorMetadata:
    return ConnectorMetadata(
        id=connection_id,
        workspace_id=workspace_id,
        name=name,
        connector_type=connector_type,
        config={},
        management_mode=ManagementMode.CONFIG_MANAGED,
        lifecycle_state=LifecycleState.ACTIVE,
    )


def _wire_api_runtime(
    runtime: ConnectorSyncRuntime,
    *,
    connector: _ApiQueueConnector,
    resource: ApiResource,
) -> None:
    runtime._build_api_connector = lambda connector_record: connector  # type: ignore[method-assign]

    async def _resolve_api_root_resource(**kwargs) -> ApiResource:
        return resource

    runtime._resolve_api_root_resource = _resolve_api_root_resource  # type: ignore[method-assign]


def _wire_sql_runtime(
    runtime: ConnectorSyncRuntime,
    *,
    connector: _SqlQueueConnector,
) -> None:
    runtime._build_sql_connector = lambda connector_record: connector  # type: ignore[method-assign]


def _parquet_rows(
    runtime: ConnectorSyncRuntime,
    *,
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    dataset_name: str,
) -> list[dict[str, Any]]:
    path = runtime._dataset_parquet_path(
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset_name,
    )
    table = pq.read_table(path)
    return table.to_pylist()


@pytest.mark.anyio
async def test_connector_sync_runtime_flattens_only_explicit_one_to_one_children(
    dataset_storage_dir: Path,
) -> None:
    runtime, state_repository, dataset_repository, dataset_revision_repository, lineage_edge_repository = _build_runtime()

    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _connector_record(
        connection_id=connection_id,
        workspace_id=workspace_id,
        name="Shopify storefront",
        connector_type=ConnectorRuntimeType.SHOPIFY,
    )
    dataset = _declared_synced_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        name="shopify_orders",
        sync_source={"resource": "orders", "flatten": ["customer"]},
    )
    dataset_repository.add(dataset)
    resource = _resource()
    connector = _ApiQueueConnector(
        resource,
        ApiExtractResult(
            resource="orders",
            records=[
                {
                    "id": 101,
                    "updated_at": "2026-03-01T00:00:00Z",
                    "total_price": "42.00",
                    "customer": {
                        "id": "cust_101",
                        "email": "ada@example.com",
                    },
                    "line_items": [
                        {"id": 9001, "title": "Hat"},
                    ],
                }
            ],
            checkpoint_cursor="2026-03-01T00:00:00Z",
        ),
    )
    _wire_api_runtime(runtime, connector=connector, resource=resource)

    summary = await runtime.sync_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connector_record=connector_record,
        dataset=dataset,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )
    state = await state_repository.get_for_resource(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name="resource:orders",
    )

    assert summary["source_key"] == "resource:orders"
    assert summary["source"] == {
        "kind": "resource",
        "resource": "orders",
        "flatten": ["customer"],
    }
    assert summary["resource_name"] == "orders"
    assert summary["records_synced"] == 1
    assert summary["dataset_names"] == ["shopify_orders"]
    assert state is not None
    assert state.status == SYNC_STATUS_SUCCEEDED
    assert state.last_cursor == "2026-03-01T00:00:00Z"
    assert state.source_kind == DatasetSourceKind.API
    assert len(dataset_repository.items) == 1

    rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset.name,
    )
    assert rows == [
        {
            "customer__email": "ada@example.com",
            "customer__id": "cust_101",
            "id": 101,
            "total_price": "42.00",
            "updated_at": "2026-03-01T00:00:00Z",
        }
    ]
    assert "line_items" not in rows[0]
    assert len(dataset_revision_repository.by_dataset[dataset.id]) == 1
    assert any(edge.source_type == "api_resource" for edge in lineage_edge_repository.items)
    assert any(edge.source_type == "file_resource" for edge in lineage_edge_repository.items)
    assert {child["path"] for child in state.state["child_resources"]} >= {
        "orders.customer",
        "orders.line_items",
    }


@pytest.mark.anyio
async def test_connector_sync_runtime_materializes_explicit_child_resource_path_dataset(
    dataset_storage_dir: Path,
) -> None:
    runtime, state_repository, dataset_repository, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _connector_record(
        connection_id=connection_id,
        workspace_id=workspace_id,
        name="Shopify storefront",
        connector_type=ConnectorRuntimeType.SHOPIFY,
    )
    dataset = _declared_synced_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        name="shopify_order_line_items",
        sync_source={"resource": "orders.line_items"},
    )
    dataset_repository.add(dataset)
    resource = _resource()
    connector = _ApiQueueConnector(
        resource,
        ApiExtractResult(
            resource="orders",
            records=[
                {
                    "id": 101,
                    "updated_at": "2026-03-01T00:00:00Z",
                    "line_items": [
                        {"id": 9001, "title": "Hat"},
                        {"id": 9002, "title": "Scarf"},
                    ],
                }
            ],
            checkpoint_cursor="2026-03-01T00:00:00Z",
        ),
    )
    _wire_api_runtime(runtime, connector=connector, resource=resource)

    summary = await runtime.sync_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connector_record=connector_record,
        dataset=dataset,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )
    state = await state_repository.get_for_resource(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name="resource:orders.line_items",
    )

    assert summary["source_key"] == "resource:orders.line_items"
    assert summary["resource_name"] == "orders.line_items"
    assert summary["root_resource_name"] == "orders"
    assert summary["records_synced"] == 2
    assert summary["dataset_names"] == ["shopify_order_line_items"]
    assert state is not None
    assert len(dataset_repository.items) == 1

    rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset.name,
    )
    assert rows == [
        {"_child_index": 0, "_parent_id": 101, "id": 9001, "title": "Hat"},
        {"_child_index": 1, "_parent_id": 101, "id": 9002, "title": "Scarf"},
    ]


@pytest.mark.anyio
async def test_connector_sync_runtime_materializes_request_source_dataset(
    dataset_storage_dir: Path,
) -> None:
    runtime, state_repository, dataset_repository, _, lineage_edge_repository = _build_runtime()

    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _connector_record(
        connection_id=connection_id,
        workspace_id=workspace_id,
        name="FX demo",
        connector_type=ConnectorRuntimeType.BASIC_HTTP,
    )
    dataset = _declared_synced_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.BASIC_HTTP,
        name="latest_usd_rates_snapshot",
        sync_source={
            "request": {
                "method": "get",
                "path": "/latest",
                "params": {"base": "USD"},
            },
            "extraction": {
                "type": "json",
                "options": {"path": "rates"},
            },
        },
        strategy=SYNC_MODE_FULL_REFRESH,
    )
    dataset_repository.add(dataset)
    resource = _resource(name="latest", supports_incremental=False)
    connector = _ApiQueueConnector(
        resource,
        ApiExtractResult(
            resource="/latest",
            records=[{"GBP": 0.8, "EUR": 0.92}],
            status="success",
        ),
    )
    _wire_api_runtime(runtime, connector=connector, resource=resource)

    summary = await runtime.sync_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connector_record=connector_record,
        dataset=dataset,
        sync_mode=SYNC_MODE_FULL_REFRESH,
    )
    state = await state_repository.get_for_resource(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name=summary["source_key"],
    )

    assert summary["resource_name"] == "latest"
    assert summary["root_resource_name"] == "latest"
    assert summary["records_synced"] == 1
    assert summary["source"]["kind"] == "request"
    assert summary["source_key"].startswith("request:")
    assert connector.request_calls[0]["request"]["path"] == "/latest"
    assert connector.request_calls[0]["extraction"]["options"]["path"] == "rates"
    assert state is not None
    assert state.status == SYNC_STATUS_SUCCEEDED
    rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset.name,
    )
    assert rows == [{"EUR": 0.92, "GBP": 0.8}]
    assert any(edge.source_type == "api_resource" for edge in lineage_edge_repository.items)


@pytest.mark.anyio
async def test_connector_sync_runtime_rejects_flattening_one_to_many_child_path(
    dataset_storage_dir: Path,
) -> None:
    runtime, _, dataset_repository, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _connector_record(
        connection_id=connection_id,
        workspace_id=workspace_id,
        name="Shopify storefront",
        connector_type=ConnectorRuntimeType.SHOPIFY,
    )
    dataset = _declared_synced_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        name="shopify_orders",
        sync_source={"resource": "orders", "flatten": ["line_items"]},
    )
    dataset_repository.add(dataset)
    resource = _resource()
    connector = _ApiQueueConnector(
        resource,
        ApiExtractResult(
            resource="orders",
            records=[
                {
                    "id": 101,
                    "updated_at": "2026-03-01T00:00:00Z",
                    "line_items": [{"id": 9001, "title": "Hat"}],
                }
            ],
            checkpoint_cursor="2026-03-01T00:00:00Z",
        ),
    )
    _wire_api_runtime(runtime, connector=connector, resource=resource)

    with pytest.raises(ValueError, match="cannot flatten a one-to-many child"):
        await runtime.sync_dataset(
            workspace_id=workspace_id,
            actor_id=actor_id,
            connector_record=connector_record,
            dataset=dataset,
            sync_mode=SYNC_MODE_INCREMENTAL,
        )

    assert dataset_repository.items[dataset.id].storage_uri is None


@pytest.mark.anyio
async def test_connector_sync_runtime_incremental_second_sync_only_upserts_new_records(
    dataset_storage_dir: Path,
) -> None:
    runtime, state_repository, dataset_repository, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _connector_record(
        connection_id=connection_id,
        workspace_id=workspace_id,
        name="Shopify",
        connector_type=ConnectorRuntimeType.SHOPIFY,
    )
    dataset = _declared_synced_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        name="shopify_orders",
        sync_source={"resource": "orders"},
    )
    dataset_repository.add(dataset)

    resource = _resource()
    first_connector = _ApiQueueConnector(
        resource,
        ApiExtractResult(
            resource="orders",
            records=[
                {"id": 1, "updated_at": "2026-03-01T00:00:00Z", "total_price": "10.00"},
                {"id": 2, "updated_at": "2026-03-01T00:30:00Z", "total_price": "20.00"},
            ],
            checkpoint_cursor="2026-03-01T00:30:00Z",
        ),
    )
    _wire_api_runtime(runtime, connector=first_connector, resource=resource)
    await runtime.sync_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connector_record=connector_record,
        dataset=dataset,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    second_connector = _ApiQueueConnector(
        resource,
        ApiExtractResult(
            resource="orders",
            records=[
                {"id": 2, "updated_at": "2026-03-02T00:00:00Z", "total_price": "25.00"},
                {"id": 3, "updated_at": "2026-03-02T01:00:00Z", "total_price": "30.00"},
            ],
            checkpoint_cursor="2026-03-02T01:00:00Z",
        ),
    )
    _wire_api_runtime(runtime, connector=second_connector, resource=resource)
    await runtime.sync_dataset(
        workspace_id=workspace_id,
        actor_id=uuid.uuid4(),
        connector_record=connector_record,
        dataset=dataset_repository.items[dataset.id],
        sync_mode=SYNC_MODE_INCREMENTAL,
    )
    state = await state_repository.get_for_resource(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name="resource:orders",
    )

    assert second_connector.calls[0]["since"] == "2026-03-01T00:30:00Z"
    assert state is not None
    assert state.last_cursor == "2026-03-02T01:00:00Z"

    rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset.name,
    )
    assert len(rows) == 3
    by_id = {row["id"]: row for row in rows}
    assert by_id[2]["total_price"] == "25.00"
    assert by_id[3]["updated_at"] == "2026-03-02T01:00:00Z"


@pytest.mark.anyio
async def test_connector_sync_runtime_falls_back_to_full_refresh_for_non_incremental_resources(
    dataset_storage_dir: Path,
) -> None:
    runtime, state_repository, dataset_repository, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _connector_record(
        connection_id=connection_id,
        workspace_id=workspace_id,
        name="Analytics",
        connector_type=ConnectorRuntimeType.GOOGLE_ANALYTICS,
    )
    resource = _resource(
        name="sessions",
        incremental_cursor_field=None,
        supports_incremental=False,
    )
    dataset = _declared_synced_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.GOOGLE_ANALYTICS,
        name="ga_sessions",
        sync_source={"resource": "sessions"},
    )
    dataset_repository.add(dataset)

    first_connector = _ApiQueueConnector(
        resource,
        ApiExtractResult(
            resource="sessions",
            records=[{"id": "a", "sessions": 12}, {"id": "b", "sessions": 6}],
        ),
    )
    _wire_api_runtime(runtime, connector=first_connector, resource=resource)
    await runtime.sync_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connector_record=connector_record,
        dataset=dataset,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    second_connector = _ApiQueueConnector(
        resource,
        ApiExtractResult(
            resource="sessions",
            records=[{"id": "c", "sessions": 99}],
        ),
    )
    _wire_api_runtime(runtime, connector=second_connector, resource=resource)
    await runtime.sync_dataset(
        workspace_id=workspace_id,
        actor_id=uuid.uuid4(),
        connector_record=connector_record,
        dataset=dataset_repository.items[dataset.id],
        sync_mode=SYNC_MODE_INCREMENTAL,
    )
    state = await state_repository.get_for_resource(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name="resource:sessions",
    )

    rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset.name,
    )
    assert rows == [{"id": "c", "sessions": 99}]
    assert state is not None
    assert state.sync_mode == SYNC_MODE_FULL_REFRESH
    assert first_connector.calls[0]["since"] is None
    assert second_connector.calls[0]["since"] is None


@pytest.mark.anyio
async def test_connector_sync_runtime_failure_does_not_advance_checkpoint_state(
    dataset_storage_dir: Path,
) -> None:
    runtime, state_repository, dataset_repository, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _connector_record(
        connection_id=connection_id,
        workspace_id=workspace_id,
        name="Shopify",
        connector_type=ConnectorRuntimeType.SHOPIFY,
    )
    dataset = _declared_synced_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        name="shopify_orders",
        sync_source={"resource": "orders"},
    )
    dataset_repository.add(dataset)

    resource = _resource()
    successful_connector = _ApiQueueConnector(
        resource,
        ApiExtractResult(
            resource="orders",
            records=[{"id": 1, "updated_at": "2026-03-01T00:00:00Z"}],
            checkpoint_cursor="2026-03-01T00:00:00Z",
        ),
    )
    _wire_api_runtime(runtime, connector=successful_connector, resource=resource)
    await runtime.sync_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connector_record=connector_record,
        dataset=dataset,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    failed_connector = _ApiQueueConnector(resource, RuntimeError("upstream timeout"))
    _wire_api_runtime(runtime, connector=failed_connector, resource=resource)
    with pytest.raises(RuntimeError):
        await runtime.sync_dataset(
            workspace_id=workspace_id,
            actor_id=uuid.uuid4(),
            connector_record=connector_record,
            dataset=dataset_repository.items[dataset.id],
            sync_mode=SYNC_MODE_INCREMENTAL,
        )
    state = await state_repository.get_for_resource(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name="resource:orders",
    )
    assert state is not None
    await runtime.mark_failed(state=state, error_message="upstream timeout")

    assert state.last_cursor == "2026-03-01T00:00:00Z"
    assert state.status == SYNC_STATUS_FAILED
    assert state.error_message == "upstream timeout"


@pytest.mark.anyio
async def test_connector_sync_runtime_materializes_sql_table_source(
    dataset_storage_dir: Path,
) -> None:
    runtime, state_repository, dataset_repository, dataset_revision_repository, lineage_edge_repository = _build_runtime()

    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _connector_record(
        connection_id=connection_id,
        workspace_id=workspace_id,
        name="Sales warehouse",
        connector_type=ConnectorRuntimeType.SQLITE,
    )
    dataset = _declared_synced_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SQLITE,
        name="sales_orders_snapshot",
        sync_source={"table": "orders"},
        cursor_field="updated_at",
    )
    dataset_repository.add(dataset)
    connector = _SqlQueueConnector(
        QueryResult(
            columns=["id", "updated_at", "total_amount"],
            rows=[[1, "2026-03-01T00:00:00Z", 10.5], [2, "2026-03-01T00:30:00Z", 20.0]],
            rowcount=2,
            elapsed_ms=1,
            sql="SELECT * FROM orders",
        ),
        columns=[
            ColumnMetadata(name="id", data_type="integer", is_primary_key=True),
            ColumnMetadata(name="updated_at", data_type="timestamp"),
            ColumnMetadata(name="total_amount", data_type="float"),
        ],
    )
    _wire_sql_runtime(runtime, connector=connector)

    summary = await runtime.sync_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connector_record=connector_record,
        dataset=dataset,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )
    state = await state_repository.get_for_resource(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name="table:orders",
    )

    assert connector.calls[0]["sql"] == "SELECT * FROM orders"
    assert summary["source_key"] == "table:orders"
    assert summary["source"] == {"kind": "table", "table": "orders"}
    assert summary["records_synced"] == 2
    assert state is not None
    assert state.source_kind == DatasetSourceKind.DATABASE
    assert state.last_cursor == "2026-03-01T00:30:00Z"
    assert state.status == SYNC_STATUS_SUCCEEDED
    assert len(dataset_revision_repository.by_dataset[dataset.id]) == 1

    rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset.name,
    )
    assert rows == [
        {"id": 1, "updated_at": "2026-03-01T00:00:00Z", "total_amount": 10.5},
        {"id": 2, "updated_at": "2026-03-01T00:30:00Z", "total_amount": 20.0},
    ]
    assert any(edge.source_type == "source_table" for edge in lineage_edge_repository.items)
    assert any(edge.source_type == "file_resource" for edge in lineage_edge_repository.items)


@pytest.mark.anyio
async def test_connector_sync_runtime_materializes_sql_query_source_with_cursor_filter(
    dataset_storage_dir: Path,
) -> None:
    runtime, state_repository, dataset_repository, _, lineage_edge_repository = _build_runtime()

    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _connector_record(
        connection_id=connection_id,
        workspace_id=workspace_id,
        name="Sales warehouse",
        connector_type=ConnectorRuntimeType.SQLITE,
    )
    source_sql = "SELECT id, updated_at, total_amount FROM orders"
    dataset = _declared_synced_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SQLITE,
        name="sales_orders_query_snapshot",
        sync_source={"sql": source_sql},
        cursor_field="updated_at",
    )
    dataset_repository.add(dataset)
    connector = _SqlQueueConnector(
        QueryResult(
            columns=["id", "updated_at", "total_amount"],
            rows=[[3, "2026-03-02T01:00:00Z", 33.0]],
            rowcount=1,
            elapsed_ms=1,
            sql="SELECT * FROM (SELECT id, updated_at, total_amount FROM orders) AS langbridge_sync_source WHERE updated_at >= '2026-03-01T00:30:00Z'",
        )
    )
    _wire_sql_runtime(runtime, connector=connector)

    state = await runtime.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SQLITE,
        resource_name=f"sql:{stable_payload_hash(source_sql)}",
        sync_mode=SYNC_MODE_INCREMENTAL,
    )
    state.last_cursor = "2026-03-01T00:30:00Z"
    await state_repository.save(state)

    summary = await runtime.sync_dataset(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connector_record=connector_record,
        dataset=dataset,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )
    reloaded_state = await state_repository.get_for_resource(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name=f"sql:{stable_payload_hash(source_sql)}",
    )

    assert "WHERE updated_at >= '2026-03-01T00:30:00Z'" in connector.calls[0]["sql"]
    assert connector.calls[0]["sql"].startswith(
        "SELECT * FROM (SELECT id, updated_at, total_amount FROM orders) AS langbridge_sync_source"
    )
    assert summary["source_key"] == f"sql:{stable_payload_hash(source_sql)}"
    assert summary["source"] == {"kind": "sql", "sql": source_sql}
    assert summary["records_synced"] == 1
    assert reloaded_state is not None
    assert reloaded_state.last_cursor == "2026-03-02T01:00:00Z"
    assert reloaded_state.source_kind == DatasetSourceKind.DATABASE
    assert any(edge.source_type == "file_resource" for edge in lineage_edge_repository.items)

    rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset.name,
    )
    assert rows == [{"id": 3, "updated_at": "2026-03-02T01:00:00Z", "total_amount": 33.0}]
