
import uuid
from datetime import datetime, timezone
from types import SimpleNamespace

import pytest

from langbridge.runtime.persistence.mappers import (
    from_connector_record,
    from_dataset_record,
    from_sql_job_record,
)
from langbridge.runtime.models import ConnectorSyncState
from langbridge.runtime.providers.memory import (
    MemoryDatasetProvider,
    MemorySyncStateProvider,
)
from langbridge.runtime.utils import build_connector_runtime_payload


@pytest.fixture
def anyio_backend():
    return "asyncio"


def test_from_connector_record_maps_runtime_connector_shape() -> None:
    workspace_id = uuid.uuid4()
    connector_id = uuid.uuid4()
    connector_record = SimpleNamespace(
        id=connector_id,
        workspace_id=workspace_id,
        name="warehouse",
        description="Primary warehouse",
        connector_type="POSTGRES",
        config_json='{"config": {"host": "db.internal", "database": "analytics"}}',
        connection_metadata_json={
            "host": "db.internal",
            "database": "analytics",
            "extra": {"sslmode": "require"},
        },
        secret_references_json={
            "password": {
                "provider_type": "env",
                "identifier": "DB_PASSWORD",
            }
        },
        access_policy_json={
            "allowed_schemas": ["public"],
            "allowed_tables": ["orders"],
        },
        is_managed=False,
    )

    connector = from_connector_record(connector_record)

    assert connector is not None
    assert connector.id == connector_id
    assert connector.workspace_id == workspace_id
    assert connector.config == {"config": {"host": "db.internal", "database": "analytics"}}
    assert connector.connection_metadata is not None
    assert connector.connection_metadata.host == "db.internal"
    assert connector.secret_references["password"].identifier == "DB_PASSWORD"
    assert connector.connection_policy is not None
    assert connector.connection_policy.allowed_schemas == ["public"]


def test_build_connector_runtime_payload_uses_runtime_secret_references() -> None:
    payload = build_connector_runtime_payload(
        config_json='{"config": {"database": "analytics"}}',
        connection_metadata={
            "host": "db.internal",
            "extra": {"sslmode": "require"},
        },
        secret_references={
            "password": {
                "provider_type": "env",
                "identifier": "DB_PASSWORD",
            }
        },
        secret_resolver=lambda ref: f"resolved:{ref.identifier}",
    )

    assert payload["config"]["database"] == "analytics"
    assert payload["config"]["host"] == "db.internal"
    assert payload["config"]["sslmode"] == "require"
    assert payload["config"]["password"] == "resolved:DB_PASSWORD"


def test_from_dataset_record_maps_legacy_dataset_shape() -> None:
    dataset_id = uuid.uuid4()
    workspace_id = uuid.uuid4()
    created_at = datetime.now(timezone.utc)
    legacy_dataset = SimpleNamespace(
        id=dataset_id,
        workspace_id=workspace_id,
        connection_id=uuid.uuid4(),
        created_by=uuid.uuid4(),
        updated_by=None,
        name="orders",
        sql_alias="orders",
        description="Orders dataset",
        tags_json=["core", "finance"],
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
        file_config_json={"format": "csv"},
        status="published",
        revision_id=None,
        row_count_estimate=123,
        bytes_estimate=456,
        last_profiled_at=None,
        columns=[
            SimpleNamespace(
                id=uuid.uuid4(),
                dataset_id=dataset_id,
                name="order_id",
                data_type="uuid",
                nullable=False,
                description=None,
                is_allowed=True,
                is_computed=False,
                expression=None,
                ordinal_position=1,
            )
        ],
        policy=SimpleNamespace(
            id=uuid.uuid4(),
            dataset_id=dataset_id,
            workspace_id=workspace_id,
            max_rows_preview=100,
            max_export_rows=1000,
            redaction_rules_json={"email": "mask"},
            row_filters_json=["region = current_region()"],
            allow_dml=False,
        ),
        created_at=created_at,
        updated_at=created_at,
    )

    dataset = from_dataset_record(legacy_dataset)

    assert dataset is not None
    assert dataset.id == dataset_id
    assert dataset.tags == ["core", "finance"]
    assert dataset.tags_json == ["core", "finance"]
    assert dataset.file_config_json == {"format": "csv"}
    assert dataset.columns[0].name == "order_id"
    assert dataset.policy is not None
    assert dataset.policy.redaction_rules_json == {"email": "mask"}


def test_from_sql_job_record_maps_legacy_sql_job_shape() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    created_at = datetime.now(timezone.utc)
    legacy_job = SimpleNamespace(
        id=uuid.uuid4(),
        workspace_id=workspace_id,
        actor_id=actor_id,
        connection_id=connection_id,
        workbench_mode="dataset",
        selected_datasets_json=[{"dataset_id": str(uuid.uuid4()), "alias": "orders"}],
        execution_mode="single",
        status="queued",
        query_text="SELECT 1",
        query_hash="hash-123",
        query_params_json={"limit": 1},
        requested_limit=10,
        enforced_limit=10,
        requested_timeout_seconds=15,
        enforced_timeout_seconds=15,
        is_explain=False,
        is_federated=False,
        correlation_id="corr-1",
        policy_snapshot_json={"allow_dml": False},
        result_columns_json=[{"name": "id", "type": "integer"}],
        result_rows_json=[{"id": 1}],
        row_count_preview=1,
        total_rows_estimate=None,
        bytes_scanned=128,
        duration_ms=25,
        result_cursor="0",
        redaction_applied=False,
        error_json=None,
        warning_json=None,
        stats_json={"rows_returned": 1},
        created_at=created_at,
        started_at=created_at,
        finished_at=created_at,
        updated_at=created_at,
    )

    job = from_sql_job_record(legacy_job)

    assert job is not None
    assert job.workspace_id == workspace_id
    assert job.actor_id == actor_id
    assert job.connection_id == connection_id
    assert job.query_text == "SELECT 1"
    assert job.query_params_json == {"limit": 1}
    assert job.result_rows_json == [{"id": 1}]
    assert job.stats_json == {"rows_returned": 1}


@pytest.mark.anyio
async def test_memory_providers_support_ephemeral_runtime_access_patterns() -> None:
    workspace_id = uuid.uuid4()
    dataset_id = uuid.uuid4()
    state_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    dataset = from_dataset_record(
        SimpleNamespace(
            id=dataset_id,
            workspace_id=workspace_id,
            connection_id=None,
            created_by=None,
            updated_by=None,
            name="customers",
            sql_alias="customers",
            description=None,
            tags_json=[],
            dataset_type="FILE",
            source_kind="file",
            connector_kind=None,
            storage_kind="parquet",
            dialect="duckdb",
            catalog_name=None,
            schema_name=None,
            table_name="customers",
            storage_uri="file:///tmp/customers.parquet",
            sql_text=None,
            relation_identity_json={},
            execution_capabilities_json={},
            referenced_dataset_ids_json=[],
            federated_plan_json=None,
            file_config_json={"format": "parquet"},
            status="published",
            revision_id=None,
            row_count_estimate=None,
            bytes_estimate=None,
            last_profiled_at=None,
            columns=[],
            policy=None,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
    )
    assert dataset is not None

    dataset_provider = MemoryDatasetProvider({dataset_id: dataset})
    sync_state_provider = MemorySyncStateProvider()

    loaded_dataset = await dataset_provider.get_dataset(
        workspace_id=workspace_id,
        dataset_id=dataset_id,
    )
    loaded_columns = await dataset_provider.get_dataset_columns(dataset_id=dataset_id)
    loaded_policy = await dataset_provider.get_dataset_policy(dataset_id=dataset_id)
    state = await sync_state_provider.get_or_create_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name="customers",
        factory=lambda: ConnectorSyncState(
            id=state_id,
            workspace_id=workspace_id,
            connection_id=connection_id,
            connector_type="LOCAL_FILESYSTEM",
            source_key="customers",
            source_kind="file",
            source={"storage_uri": "file:///tmp/customers.parquet"},
        ),
    )

    assert loaded_dataset is not None
    assert loaded_dataset.name == "customers"
    assert loaded_columns == []
    assert loaded_policy is None
    assert state.id == state_id
