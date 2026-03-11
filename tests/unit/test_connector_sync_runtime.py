from __future__ import annotations

import uuid
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import httpx
import pyarrow.parquet as pq
import pytest

from langbridge.apps.worker.langbridge_worker.connector_sync_runtime import ConnectorSyncRuntime
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.contracts.connectors import (
    ConnectorSyncMode,
    ConnectorSyncStatus,
)
from langbridge.packages.common.langbridge_common.db.connector_sync import ConnectorSyncStateRecord
from langbridge.packages.common.langbridge_common.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)
from langbridge.packages.common.langbridge_common.db.lineage import LineageEdgeRecord
from langbridge.packages.common.langbridge_common.db import agent as _db_agent  # noqa: F401
from langbridge.packages.common.langbridge_common.db import semantic as _db_semantic  # noqa: F401
from langbridge.packages.connectors.langbridge_connectors.api.connector import (
    ApiExtractResult,
    ApiResource,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorRuntimeType
from langbridge.packages.connectors.langbridge_connectors.api.shopify.config import (
    ShopifyConnectorConfig,
)
from langbridge.packages.connectors.langbridge_connectors.api.shopify.connector import (
    ShopifyApiConnector,
)


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
    monkeypatch.setattr(settings, "DATASET_FILE_LOCAL_DIR", str(tmp_path))
    runtime, _, dataset_repository, dataset_revision_repository, lineage_edge_repository = _build_runtime()

    workspace_id = uuid.uuid4()
    project_id = uuid.uuid4()
    user_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    connector_record = _FakeConnectorRecord(name="Shopify storefront")

    state = await runtime.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource_name="orders",
        sync_mode=ConnectorSyncMode.INCREMENTAL,
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
        project_id=project_id,
        user_id=user_id,
        connection_id=connection_id,
        connector_record=connector_record,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource=_resource(),
        api_connector=connector,
        state=state,
        sync_mode=ConnectorSyncMode.INCREMENTAL,
    )

    assert summary["records_synced"] == 2
    assert state.status == ConnectorSyncStatus.SUCCEEDED.value
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
    assert root_dataset.schema_name is None
    assert child_dataset.schema_name is None

    assert len(dataset_revision_repository.by_dataset[root_dataset.id]) == 1
    assert len(dataset_revision_repository.by_dataset[child_dataset.id]) == 1
    assert any(edge.source_type == "api_resource" for edge in lineage_edge_repository.items)
    assert any(edge.source_type == "file_resource" for edge in lineage_edge_repository.items)


@pytest.mark.anyio
async def test_connector_sync_runtime_incremental_second_sync_only_upserts_new_records(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "DATASET_FILE_LOCAL_DIR", str(tmp_path))
    runtime, _, dataset_repository, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    state = await runtime.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource_name="orders",
        sync_mode=ConnectorSyncMode.INCREMENTAL,
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
        project_id=None,
        user_id=uuid.uuid4(),
        connection_id=connection_id,
        connector_record=_FakeConnectorRecord(name="Shopify"),
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource=_resource(),
        api_connector=first_connector,
        state=state,
        sync_mode=ConnectorSyncMode.INCREMENTAL,
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
        project_id=None,
        user_id=uuid.uuid4(),
        connection_id=connection_id,
        connector_record=_FakeConnectorRecord(name="Shopify"),
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource=_resource(),
        api_connector=second_connector,
        state=state,
        sync_mode=ConnectorSyncMode.INCREMENTAL,
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
    monkeypatch.setattr(settings, "DATASET_FILE_LOCAL_DIR", str(tmp_path))
    runtime, _, dataset_repository, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    state = await runtime.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.GOOGLE_ANALYTICS,
        resource_name="sessions",
        sync_mode=ConnectorSyncMode.INCREMENTAL,
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
        project_id=None,
        user_id=uuid.uuid4(),
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
        sync_mode=ConnectorSyncMode.INCREMENTAL,
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
        project_id=None,
        user_id=uuid.uuid4(),
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
        sync_mode=ConnectorSyncMode.INCREMENTAL,
    )

    dataset = next(iter(dataset_repository.items.values()))
    rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset.name,
    )
    assert rows == [{"id": "c", "sessions": 99}]
    assert state.sync_mode == ConnectorSyncMode.FULL_REFRESH.value
    assert first_connector.calls[0]["since"] is None
    assert second_connector.calls[0]["since"] is None


@pytest.mark.anyio
async def test_connector_sync_runtime_failure_does_not_advance_checkpoint_state(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "DATASET_FILE_LOCAL_DIR", str(tmp_path))
    runtime, _, _, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    state = await runtime.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource_name="orders",
        sync_mode=ConnectorSyncMode.INCREMENTAL,
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
        project_id=None,
        user_id=uuid.uuid4(),
        connection_id=connection_id,
        connector_record=_FakeConnectorRecord(name="Shopify"),
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource=_resource(),
        api_connector=successful_connector,
        state=state,
        sync_mode=ConnectorSyncMode.INCREMENTAL,
    )

    failed_connector = _QueueConnector(RuntimeError("upstream timeout"))
    with pytest.raises(RuntimeError):
        await runtime.sync_resource(
            workspace_id=workspace_id,
            project_id=None,
            user_id=uuid.uuid4(),
            connection_id=connection_id,
            connector_record=_FakeConnectorRecord(name="Shopify"),
            connector_type=ConnectorRuntimeType.SHOPIFY,
            resource=_resource(),
            api_connector=failed_connector,
            state=state,
            sync_mode=ConnectorSyncMode.INCREMENTAL,
        )
    await runtime.mark_failed(state=state, error_message="upstream timeout")

    assert state.last_cursor == "2026-03-01T00:00:00Z"
    assert state.status == ConnectorSyncStatus.FAILED.value
    assert state.error_message == "upstream timeout"


@pytest.mark.anyio
async def test_shopify_connector_sync_runtime_full_then_incremental_with_mocked_api(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(settings, "DATASET_FILE_LOCAL_DIR", str(tmp_path))
    monkeypatch.setattr(settings, "SHOPIFY_APP_CLIENT_ID", "client-id")
    monkeypatch.setattr(settings, "SHOPIFY_APP_CLIENT_SECRET", "client-secret")
    runtime, _, dataset_repository, _, _ = _build_runtime()

    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    user_id = uuid.uuid4()
    state = await runtime.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource_name="orders",
        sync_mode=ConnectorSyncMode.INCREMENTAL,
    )

    order_requests: list[httpx.Request] = []

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.endswith("/oauth/access_token"):
            return httpx.Response(200, json={"access_token": "oauth-token"})
        if request.url.path.endswith("/shop.json"):
            assert request.headers["X-Shopify-Access-Token"] == "oauth-token"
            return httpx.Response(200, json={"shop": {"id": 1}})
        if request.url.path.endswith("/orders.json"):
            order_requests.append(request)
            assert request.headers["X-Shopify-Access-Token"] == "oauth-token"
            if request.url.params.get("updated_at_min"):
                assert request.url.params["updated_at_min"] == "2026-03-01T00:00:00Z"
                return httpx.Response(
                    200,
                    json={
                        "orders": [
                            {
                                "id": 102,
                                "updated_at": "2026-03-02T00:00:00Z",
                                "line_items": [{"id": 2, "title": "Bag"}],
                            }
                        ]
                    },
                )
            return httpx.Response(
                200,
                json={
                    "orders": [
                        {
                            "id": 101,
                            "updated_at": "2026-03-01T00:00:00Z",
                            "line_items": [{"id": 1, "title": "Hat"}],
                        }
                    ]
                },
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    connector = ShopifyApiConnector(
        ShopifyConnectorConfig(
            shop_domain="acme.myshopify.com",
        ),
        transport=httpx.MockTransport(handler),
    )
    await connector.test_connection()
    orders_resource = next(
        resource for resource in await connector.discover_resources() if resource.name == "orders"
    )

    await runtime.sync_resource(
        workspace_id=workspace_id,
        project_id=None,
        user_id=user_id,
        connection_id=connection_id,
        connector_record=_FakeConnectorRecord(name="Shopify"),
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource=orders_resource,
        api_connector=connector,
        state=state,
        sync_mode=ConnectorSyncMode.INCREMENTAL,
    )
    await runtime.sync_resource(
        workspace_id=workspace_id,
        project_id=None,
        user_id=user_id,
        connection_id=connection_id,
        connector_record=_FakeConnectorRecord(name="Shopify"),
        connector_type=ConnectorRuntimeType.SHOPIFY,
        resource=orders_resource,
        api_connector=connector,
        state=state,
        sync_mode=ConnectorSyncMode.INCREMENTAL,
    )

    assert len(order_requests) == 2
    assert state.last_cursor == "2026-03-02T00:00:00Z"

    datasets = list(dataset_repository.items.values())
    dataset_names = {
        (dataset.file_config_json or {}).get("connector_sync", {}).get("resource_name"): dataset.name
        for dataset in datasets
    }
    assert "orders" in dataset_names
    assert "orders__line_items" in dataset_names

    root_rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset_names["orders"],
    )
    child_rows = _parquet_rows(
        runtime,
        workspace_id=workspace_id,
        connection_id=connection_id,
        dataset_name=dataset_names["orders__line_items"],
    )

    assert {row["id"] for row in root_rows} == {101, 102}
    assert {row["id"] for row in child_rows} == {1, 2}
