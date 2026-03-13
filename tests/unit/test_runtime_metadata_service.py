from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from langbridge.apps.api.langbridge_api.services.runtime_metadata_service import (
    RuntimeMetadataService,
)


@pytest.fixture
def anyio_backend() -> str:
    return "asyncio"


@dataclass
class _SemanticModelRecord:
    id: uuid.UUID
    organization_id: uuid.UUID
    project_id: uuid.UUID | None
    name: str
    description: str | None
    content_yaml: str
    created_at: datetime
    updated_at: datetime
    connector_id: uuid.UUID | None


class _DatasetRepository:
    def __init__(self, dataset) -> None:
        self._dataset = dataset

    async def get_for_workspace(self, *, dataset_id, workspace_id):
        if self._dataset.id == dataset_id and self._dataset.workspace_id == workspace_id:
            return self._dataset
        return None


class _DatasetColumnRepository:
    def __init__(self, columns) -> None:
        self._columns = columns

    async def list_for_dataset(self, *, dataset_id):
        return [item for item in self._columns if item.dataset_id == dataset_id]


class _DatasetPolicyRepository:
    def __init__(self, policy) -> None:
        self._policy = policy

    async def get_for_dataset(self, *, dataset_id):
        if self._policy.dataset_id == dataset_id:
            return self._policy
        return None


class _ConnectorRepository:
    def __init__(self, connector) -> None:
        self._connector = connector

    async def get_by_id(self, connector_id):
        if self._connector.id == connector_id:
            return self._connector
        return None


class _SemanticModelRepository:
    def __init__(self, record: _SemanticModelRecord) -> None:
        self._record = record

    async def get_for_scope(self, *, model_id, organization_id):
        if self._record.id == model_id and self._record.organization_id == organization_id:
            return self._record
        return None


class _ConnectorSyncStateRepository:
    def __init__(self, state) -> None:
        self._state = state

    async def get_for_resource(self, *, workspace_id, connection_id, resource_name):
        state = self._state
        if (
            state.workspace_id == workspace_id
            and state.connection_id == connection_id
            and state.resource_name == resource_name
        ):
            return state
        return None


@pytest.mark.anyio
async def test_runtime_metadata_service_serializes_dataset_and_policy() -> None:
    now = datetime.now(timezone.utc)
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    dataset_id = uuid.uuid4()
    dataset = SimpleNamespace(
        id=dataset_id,
        workspace_id=workspace_id,
        project_id=None,
        connection_id=connection_id,
        created_by=None,
        updated_by=None,
        name="orders",
        sql_alias="orders",
        description="Orders dataset",
        tags_json=["sales"],
        dataset_type="TABLE",
        source_kind="database",
        connector_kind="postgres",
        storage_kind="table",
        dialect="postgres",
        catalog_name=None,
        schema_name="public",
        table_name="orders",
        storage_uri=None,
        sql_text=None,
        relation_identity_json={"canonical_reference": "public.orders"},
        execution_capabilities_json={"supports_sql_federation": True},
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=42,
        bytes_estimate=512,
        last_profiled_at=now,
        created_at=now,
        updated_at=now,
    )
    column = SimpleNamespace(
        id=uuid.uuid4(),
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        name="order_id",
        data_type="integer",
        nullable=False,
        ordinal_position=1,
        description=None,
        is_allowed=True,
        is_computed=False,
        expression=None,
        created_at=now,
        updated_at=now,
    )
    policy = SimpleNamespace(
        id=uuid.uuid4(),
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        max_rows_preview=100,
        max_export_rows=1000,
        redaction_rules_json={"email": "mask"},
        row_filters_json=["region = 'EMEA'"],
        allow_dml=False,
        created_at=now,
        updated_at=now,
    )
    connector = SimpleNamespace(
        id=connection_id,
        name="warehouse",
        description=None,
        connector_type="POSTGRES",
        config_json={"config": {}},
        connection_metadata_json=None,
        secret_references_json={},
        access_policy_json=None,
        is_managed=False,
        organizations=[SimpleNamespace(id=workspace_id)],
    )
    semantic_model = _SemanticModelRecord(
        id=uuid.uuid4(),
        organization_id=workspace_id,
        project_id=None,
        name="orders_model",
        description=None,
        content_yaml="version: '1.0'\ntables: {}\n",
        created_at=now,
        updated_at=now,
        connector_id=connection_id,
    )
    sync_state = SimpleNamespace(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        connection_id=connection_id,
        connector_type="POSTGRES",
        resource_name="orders",
        sync_mode="INCREMENTAL",
        last_cursor="cursor-1",
        last_sync_at=now,
        state_json={"checkpoint": "cursor-1"},
        status="succeeded",
        error_message=None,
        records_synced=42,
        bytes_synced=2048,
        created_at=now,
        updated_at=now,
    )
    service = RuntimeMetadataService(
        dataset_repository=_DatasetRepository(dataset),
        dataset_column_repository=_DatasetColumnRepository([column]),
        dataset_policy_repository=_DatasetPolicyRepository(policy),
        connector_repository=_ConnectorRepository(connector),
        semantic_model_repository=_SemanticModelRepository(semantic_model),
        connector_sync_state_repository=_ConnectorSyncStateRepository(sync_state),
    )

    dataset_payload = await service.get_dataset(
        workspace_id=workspace_id,
        dataset_id=dataset_id,
    )
    columns_payload = await service.get_dataset_columns(dataset_id=dataset_id)
    policy_payload = await service.get_dataset_policy(dataset_id=dataset_id)
    sync_payload = await service.get_sync_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name="orders",
    )

    assert dataset_payload is not None
    assert dataset_payload["id"] == str(dataset_id)
    assert dataset_payload["execution_capabilities_json"]["supports_sql_federation"] is True
    assert columns_payload[0]["name"] == "order_id"
    assert policy_payload is not None
    assert policy_payload["redaction_rules_json"] == {"email": "mask"}
    assert sync_payload is not None
    assert sync_payload["state_json"] == {"checkpoint": "cursor-1"}


@pytest.mark.anyio
async def test_runtime_metadata_service_serializes_semantic_model() -> None:
    now = datetime.now(timezone.utc)
    workspace_id = uuid.uuid4()
    connector_id = uuid.uuid4()
    semantic_model = _SemanticModelRecord(
        id=uuid.uuid4(),
        organization_id=workspace_id,
        project_id=None,
        name="orders_model",
        description="semantic",
        content_yaml="version: '1.0'\ntables: {}\n",
        created_at=now,
        updated_at=now,
        connector_id=connector_id,
    )
    empty_dataset = SimpleNamespace(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        project_id=None,
        connection_id=connector_id,
        created_by=None,
        updated_by=None,
        name="unused",
        sql_alias="unused",
        description=None,
        tags_json=[],
        dataset_type="TABLE",
        source_kind="database",
        connector_kind="postgres",
        storage_kind="table",
        dialect="postgres",
        catalog_name=None,
        schema_name="public",
        table_name="unused",
        storage_uri=None,
        sql_text=None,
        relation_identity_json={},
        execution_capabilities_json={},
        referenced_dataset_ids_json=[],
        federated_plan_json=None,
        file_config_json=None,
        status="published",
        revision_id=None,
        row_count_estimate=None,
        bytes_estimate=None,
        last_profiled_at=None,
        created_at=now,
        updated_at=now,
    )
    connector = SimpleNamespace(
        id=connector_id,
        name="warehouse",
        description=None,
        connector_type="POSTGRES",
        config_json={"config": {}},
        connection_metadata_json=None,
        secret_references_json={},
        access_policy_json=None,
        is_managed=False,
        organizations=[SimpleNamespace(id=workspace_id)],
    )
    service = RuntimeMetadataService(
        dataset_repository=_DatasetRepository(empty_dataset),
        dataset_column_repository=_DatasetColumnRepository([]),
        dataset_policy_repository=_DatasetPolicyRepository(
            SimpleNamespace(
                id=uuid.uuid4(),
                dataset_id=empty_dataset.id,
                workspace_id=workspace_id,
                max_rows_preview=100,
                max_export_rows=1000,
                redaction_rules_json={},
                row_filters_json=[],
                allow_dml=False,
                created_at=now,
                updated_at=now,
            )
        ),
        connector_repository=_ConnectorRepository(connector),
        semantic_model_repository=_SemanticModelRepository(semantic_model),
        connector_sync_state_repository=_ConnectorSyncStateRepository(
            SimpleNamespace(
                id=uuid.uuid4(),
                workspace_id=workspace_id,
                connection_id=connector_id,
                connector_type="POSTGRES",
                resource_name="unused",
                sync_mode="INCREMENTAL",
                last_cursor=None,
                last_sync_at=None,
                state_json={},
                status="never_synced",
                error_message=None,
                records_synced=0,
                bytes_synced=None,
                created_at=now,
                updated_at=now,
            )
        ),
    )

    payload = await service.get_semantic_model(
        organization_id=workspace_id,
        semantic_model_id=semantic_model.id,
    )

    assert payload is not None
    assert payload["id"] == str(semantic_model.id)
    assert payload["connector_id"] == str(connector_id)
    assert payload["name"] == "orders_model"
