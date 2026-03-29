
import uuid
from dataclasses import dataclass
from pathlib import Path
from datetime import datetime, timezone
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest

from langbridge.runtime.persistence.db.connector_sync import ConnectorSyncStateRecord
from langbridge.runtime.persistence.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)
from langbridge.runtime.persistence.db.lineage import LineageEdgeRecord
from langbridge.runtime.persistence.db import agent as _db_agent  # noqa: F401
from langbridge.runtime.persistence.db import semantic as _db_semantic  # noqa: F401
from langbridge.connectors.base.connector import (
    ApiExtractResult,
    ApiResource,
)
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.saas.shopify.config import (
    ShopifyConnectorConfig,
)
from langbridge.connectors.saas.shopify.connector import (
    ShopifyApiConnector,
)
from langbridge.runtime.models import DatasetMetadata
from langbridge.runtime.models.metadata import LifecycleState, ManagementMode
from langbridge.runtime.settings import runtime_settings
from langbridge.runtime.services.dataset_sync_service import ConnectorSyncRuntime


SYNC_MODE_INCREMENTAL = "INCREMENTAL"
SYNC_MODE_FULL_REFRESH = "FULL_REFRESH"
SYNC_STATUS_SUCCEEDED = "succeeded"
SYNC_STATUS_FAILED = "failed"


@pytest.fixture
def anyio_backend():
    return "asyncio"


@dataclass
class _FakeConnectorRecord:
    name: str


class _FakeConnectorSyncStateRepository:
    def __init__(self) -> None:
        self.items: dict[tuple[uuid.UUID, uuid.UUID, str], ConnectorSyncStateRecord] = {}

    def add(self, state: ConnectorSyncStateRecord) -> None:
        self.items[(state.workspace_id, state.connection_id, state.resource_name)] = state

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
                and (
                    str(dataset.materialization_mode or "").strip().lower() == "synced"
                    or bool((dataset.file_config_json or {}).get("managed_dataset"))
                )
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


class _QueueConnector:
    def __init__(self, *responses: ApiExtractResult | Exception) -> None:
        self._responses = list(responses)
        self.calls: list[dict[str, Any]] = []

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
        primary_key=primary_key,
        cursor_field="page_info" if supports_incremental else None,
        incremental_cursor_field=incremental_cursor_field,
        supports_incremental=supports_incremental,
        default_sync_mode="INCREMENTAL" if supports_incremental else "FULL_REFRESH",
    )


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
async def test_connector_sync_runtime_materializes_parent_child_datasets_and_persists_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _, dataset_repository, dataset_revision_repository, lineage_edge_repository = _build_runtime()

    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _FakeConnectorRecord(name="Shopify storefront")

    state = await runtime.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource_name="orders",
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    connector = _QueueConnector(
        ApiExtractResult(
            resource="orders",
            records=[
                {
                    "id": 101,
                    "updated_at": "2026-03-01T00:00:00Z",
                    "total_price": "42.00",
                }
            ],
            checkpoint_cursor="2026-03-01T00:00:00Z",
            child_records={
                "orders__line_items": [
                    {
                        "id": 9001,
                        "_parent_id": 101,
                        "_child_index": 0,
                        "title": "Hat",
                    }
                ]
            },
        )
    )

    summary = await runtime.sync_resource(
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        connector_record=connector_record,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource=_resource(),
        api_connector=connector,
        state=state,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    assert summary["records_synced"] == 2
    assert state.status == SYNC_STATUS_SUCCEEDED
    assert state.last_cursor == "2026-03-01T00:00:00Z"
    assert len(summary["dataset_ids"]) == 2

    datasets = list(dataset_repository.items.values())
    assert len(datasets) == 2
    root_dataset = next(
        dataset
        for dataset in datasets
        if (dataset.file_config_json or {}).get("connector_sync", {}).get("resource_name") == "orders"
    )
    child_dataset = next(
        dataset
        for dataset in datasets
        if (dataset.file_config_json or {}).get("connector_sync", {}).get("resource_name") == "orders__line_items"
    )

    root_rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=root_dataset.name,
    )
    child_rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=child_dataset.name,
    )
    assert root_rows == [{"id": 101, "total_price": "42.00", "updated_at": "2026-03-01T00:00:00Z"}]
    assert child_rows == [{"_child_index": 0, "_parent_id": 101, "id": 9001, "title": "Hat"}]
    assert root_dataset.materialization_mode == "synced"
    assert child_dataset.materialization_mode == "synced"
    assert root_dataset.schema_name is None
    assert child_dataset.schema_name is None

    assert len(dataset_revision_repository.by_dataset[root_dataset.id]) == 1
    assert len(dataset_revision_repository.by_dataset[child_dataset.id]) == 1
    assert any(edge.source_type == "api_resource" for edge in lineage_edge_repository.items)
    assert any(edge.source_type == "file_resource" for edge in lineage_edge_repository.items)


@pytest.mark.anyio
async def test_connector_sync_runtime_reuses_declared_synced_dataset_for_matching_resource(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _, dataset_repository, _, _ = _build_runtime()
    original_dataset_dir = runtime_settings.DATASET_FILE_LOCAL_DIR
    object.__setattr__(
        runtime_settings,
        "DATASET_FILE_LOCAL_DIR",
        str((tmp_path / "datasets").resolve()),
    )

    try:
        workspace_id = uuid.uuid4()
        actor_id = uuid.uuid4()
        connection_id = uuid.uuid4()
        now = datetime.now(timezone.utc)
        declared_dataset = DatasetMetadata(
            id=uuid.uuid4(),
            workspace_id=workspace_id,
            connection_id=connection_id,
            created_by=actor_id,
            updated_by=actor_id,
            name="billing_customers",
            sql_alias="billing_customers",
            description="Declared synced dataset.",
            tags=["managed", "api-connector", "stripe", "resource:customers"],
            dataset_type="FILE",
            materialization_mode="synced",
            source_kind="api",
            connector_kind="stripe",
            storage_kind="parquet",
            dialect="duckdb",
            catalog_name=None,
            schema_name=None,
            table_name="billing_customers",
            storage_uri=None,
            sql_text=None,
            relation_identity=None,
            execution_capabilities=None,
            referenced_dataset_ids=[],
            federated_plan=None,
            file_config={
                "format": "parquet",
                "managed_dataset": True,
                "connector_sync": {
                    "connector_type": "STRIPE",
                    "resource_name": "customers",
                    "root_resource_name": "customers",
                    "parent_resource_name": None,
                },
            },
            status="pending_sync",
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
        dataset_repository.add(declared_dataset)

        connector = _QueueConnector(
            ApiExtractResult(
                resource="customers",
                records=[
                    {"id": "cus_001", "email": "ada@example.com"},
                    {"id": "cus_002", "email": "grace@example.com"},
                ],
                child_records={},
                next_cursor=None,
                checkpoint_cursor="1710003600",
            )
        )
        state = await runtime.get_or_create_state(
            workspace_id=workspace_id,
            connection_id=connection_id,
            connector_type=ConnectorRuntimeType.STRIPE,
            resource_name="customers",
            sync_mode=SYNC_MODE_INCREMENTAL,
        )

        summary = await runtime.sync_resource(
            workspace_id=workspace_id,
            actor_id=actor_id,
            connection_id=connection_id,
            connector_record=_FakeConnectorRecord(name="billing_demo"),
            connector_type=ConnectorRuntimeType.STRIPE,
            resource=ApiResource(
                name="customers",
                label="Customers",
                primary_key="id",
                parent_resource=None,
                cursor_field="created",
                incremental_cursor_field="created",
                supports_incremental=True,
                default_sync_mode=SYNC_MODE_INCREMENTAL,
            ),
            api_connector=connector,
            state=state,
            sync_mode=SYNC_MODE_INCREMENTAL,
        )

        assert summary["dataset_names"] == ["billing_customers"]
        assert summary["dataset_ids"] == [str(declared_dataset.id)]
        updated_dataset = dataset_repository.items[declared_dataset.id]
        assert updated_dataset.name == "billing_customers"
        assert updated_dataset.table_name == "billing_customers"
        assert updated_dataset.status == "published"
        assert updated_dataset.storage_uri is not None
        assert len(dataset_repository.items) == 1
    finally:
        object.__setattr__(runtime_settings, "DATASET_FILE_LOCAL_DIR", original_dataset_dir)


@pytest.mark.anyio
async def test_connector_sync_runtime_incremental_second_sync_only_upserts_new_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _, dataset_repository, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    state = await runtime.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource_name="orders",
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    first_connector = _QueueConnector(
        ApiExtractResult(
            resource="orders",
            records=[
                {"id": 1, "updated_at": "2026-03-01T00:00:00Z", "total_price": "10.00"},
                {"id": 2, "updated_at": "2026-03-01T00:30:00Z", "total_price": "20.00"},
            ],
            checkpoint_cursor="2026-03-01T00:30:00Z",
            child_records={},
        )
    )
    await runtime.sync_resource(
        workspace_id=workspace_id,
        actor_id=uuid.uuid4(),
        connection_id=connection_id,
        connector_record=_FakeConnectorRecord(name="Shopify"),
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource=_resource(),
        api_connector=first_connector,
        state=state,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    second_connector = _QueueConnector(
        ApiExtractResult(
            resource="orders",
            records=[
                {"id": 2, "updated_at": "2026-03-02T00:00:00Z", "total_price": "25.00"},
                {"id": 3, "updated_at": "2026-03-02T01:00:00Z", "total_price": "30.00"},
            ],
            checkpoint_cursor="2026-03-02T01:00:00Z",
            child_records={},
        )
    )
    await runtime.sync_resource(
        workspace_id=workspace_id,
        actor_id=uuid.uuid4(),
        connection_id=connection_id,
        connector_record=_FakeConnectorRecord(name="Shopify"),
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource=_resource(),
        api_connector=second_connector,
        state=state,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    assert second_connector.calls[0]["since"] == "2026-03-01T00:30:00Z"
    assert state.last_cursor == "2026-03-02T01:00:00Z"

    root_dataset = next(iter(dataset_repository.items.values()))
    rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=root_dataset.name,
    )
    assert len(rows) == 3
    by_id = {row["id"]: row for row in rows}
    assert by_id[2]["total_price"] == "25.00"
    assert by_id[3]["updated_at"] == "2026-03-02T01:00:00Z"


@pytest.mark.anyio
async def test_connector_sync_runtime_falls_back_to_full_refresh_for_non_incremental_resources(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _, dataset_repository, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    state = await runtime.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.GOOGLE_ANALYTICS,
        resource_name="sessions",
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    first_connector = _QueueConnector(
        ApiExtractResult(
            resource="sessions",
            records=[{"id": "a", "sessions": 12}, {"id": "b", "sessions": 6}],
            child_records={},
        )
    )
    await runtime.sync_resource(
        workspace_id=workspace_id,
        actor_id=uuid.uuid4(),
        connection_id=connection_id,
        connector_record=_FakeConnectorRecord(name="Analytics"),
        connector_type=ConnectorRuntimeType.GOOGLE_ANALYTICS,
        resource=_resource(
            name="sessions",
            incremental_cursor_field=None,
            supports_incremental=False,
        ),
        api_connector=first_connector,
        state=state,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    second_connector = _QueueConnector(
        ApiExtractResult(
            resource="sessions",
            records=[{"id": "c", "sessions": 99}],
            child_records={},
        )
    )
    await runtime.sync_resource(
        workspace_id=workspace_id,
        actor_id=uuid.uuid4(),
        connection_id=connection_id,
        connector_record=_FakeConnectorRecord(name="Analytics"),
        connector_type=ConnectorRuntimeType.GOOGLE_ANALYTICS,
        resource=_resource(
            name="sessions",
            incremental_cursor_field=None,
            supports_incremental=False,
        ),
        api_connector=second_connector,
        state=state,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    dataset = next(iter(dataset_repository.items.values()))
    rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset.name,
    )
    assert rows == [{"id": "c", "sessions": 99}]
    assert state.sync_mode == SYNC_MODE_FULL_REFRESH
    assert first_connector.calls[0]["since"] is None
    assert second_connector.calls[0]["since"] is None


@pytest.mark.anyio
async def test_connector_sync_runtime_failure_does_not_advance_checkpoint_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    runtime, _, _, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    state = await runtime.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource_name="orders",
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    successful_connector = _QueueConnector(
        ApiExtractResult(
            resource="orders",
            records=[{"id": 1, "updated_at": "2026-03-01T00:00:00Z"}],
            checkpoint_cursor="2026-03-01T00:00:00Z",
            child_records={},
        )
    )
    await runtime.sync_resource(
        workspace_id=workspace_id,
        actor_id=uuid.uuid4(),
        connection_id=connection_id,
        connector_record=_FakeConnectorRecord(name="Shopify"),
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource=_resource(),
        api_connector=successful_connector,
        state=state,
        sync_mode=SYNC_MODE_INCREMENTAL,
    )

    failed_connector = _QueueConnector(RuntimeError("upstream timeout"))
    with pytest.raises(RuntimeError):
        await runtime.sync_resource(
            workspace_id=workspace_id,
            actor_id=uuid.uuid4(),
            connection_id=connection_id,
            connector_record=_FakeConnectorRecord(name="Shopify"),
            connector_type=ConnectorRuntimeType.SHOPIFY,
            resource=_resource(),
            api_connector=failed_connector,
            state=state,
            sync_mode=SYNC_MODE_INCREMENTAL,
        )
    await runtime.mark_failed(state=state, error_message="upstream timeout")

    assert state.last_cursor == "2026-03-01T00:00:00Z"
    assert state.status == SYNC_STATUS_FAILED
    assert state.error_message == "upstream timeout"
