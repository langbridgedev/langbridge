
import asyncio
import enum
import sys
import uuid
from pathlib import Path
from types import SimpleNamespace

import pytest

from langbridge.connectors.base.config import ConnectorFamily
from langbridge.runtime.models import (
    ConnectorMetadata,
    CreateDatasetBulkCreateJobRequest,
    LifecycleState,
    ManagementMode,
)
from langbridge.runtime.providers import MemoryConnectorProvider

REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPO_ROOT
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from langbridge.runtime.persistence.db.job import JobStatus  # noqa: E402
from langbridge.runtime.services.dataset_query import (  # noqa: E402
    DatasetQueryService,
)
from langbridge.runtime.services.dataset_query.job_status import DatasetJobStatusWriter  # noqa: E402
from langbridge.runtime.services.errors import ExecutionValidationError  # noqa: E402


class _CloudJobStatus(enum.Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


def test_set_job_status_preserves_loaded_enum_type() -> None:
    job_record = SimpleNamespace(status=_CloudJobStatus.running)

    DatasetJobStatusWriter().set_status(job_record, JobStatus.succeeded)

    assert job_record.status is _CloudJobStatus.succeeded


class _DatasetRepository:
    def __init__(self) -> None:
        self.items = {}

    def add(self, dataset):
        self.items[dataset.id] = dataset
        return dataset

    async def save(self, dataset):
        self.items[dataset.id] = dataset
        return dataset

    async def list_for_workspace(self, *, workspace_id, dataset_types=None, limit=5000, **kwargs):
        items = [item for item in self.items.values() if item.workspace_id == workspace_id]
        if dataset_types is not None:
            allowed = {str(value) for value in dataset_types}
            items = [item for item in items if item.dataset_type_value in allowed]
        return items[:limit]


class _DatasetColumnRepository:
    def __init__(self) -> None:
        self.items = []

    def add(self, column):
        self.items.append(column)
        return column

    async def list_for_dataset(self, *, dataset_id):
        return [item for item in self.items if item.dataset_id == dataset_id]


class _DatasetPolicyRepository:
    def __init__(self) -> None:
        self.items = {}

    def add(self, policy):
        self.items[policy.dataset_id] = policy
        return policy

    async def get_for_dataset(self, *, dataset_id):
        return self.items.get(dataset_id)


def test_create_table_dataset_from_selection_sets_explicit_runtime_kinds() -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    service = DatasetQueryService(
        dataset_repository=_DatasetRepository(),
        dataset_column_repository=_DatasetColumnRepository(),
        dataset_policy_repository=_DatasetPolicyRepository(),
        connector_provider=MemoryConnectorProvider(
            {
                connection_id: ConnectorMetadata(
                    id=connection_id,
                    name="warehouse",
                    connector_type="POSTGRES",
                    connector_family=ConnectorFamily.DATABASE,
                    workspace_id=workspace_id,
                    config={"config": {"database": "analytics"}},
                    management_mode=ManagementMode.CONFIG_MANAGED,
                    lifecycle_state=LifecycleState.ACTIVE,
                )
            }
        ),
    )
    request = CreateDatasetBulkCreateJobRequest.model_validate(
        {
            "workspaceId": str(workspace_id),
            "actorId": str(actor_id),
            "connectionId": str(connection_id),
            "selections": [
                {
                    "schema": "public",
                    "table": "orders",
                    "columns": [{"name": "id", "dataType": "uuid"}],
                }
            ],
        }
    )

    dataset = asyncio.run(
        service._create_table_dataset_from_selection(
            request=request,
            schema_name="public",
            table_name="orders",
            columns=request.selections[0].columns,
        )
    )

    assert dataset.dataset_type_value == "TABLE"
    assert dataset.materialization_mode_value == "live"
    assert dataset.source_kind_value == "database"
    assert dataset.storage_kind_value == "table"
    assert dataset.connector_kind == "postgres"
    assert dataset.dialect == "postgres"
    assert dataset.relation_identity_json["canonical_reference"] == f"dataset:{dataset.id}"
    assert dataset.execution_capabilities_json["supports_sql_federation"] is True


def test_create_table_dataset_from_selection_requires_connector_metadata() -> None:
    workspace_id = uuid.uuid4()
    connection_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    service = DatasetQueryService(
        dataset_repository=_DatasetRepository(),
        dataset_column_repository=_DatasetColumnRepository(),
        dataset_policy_repository=_DatasetPolicyRepository(),
        connector_provider=None,
    )
    request = CreateDatasetBulkCreateJobRequest.model_validate(
        {
            "workspaceId": str(workspace_id),
            "actorId": str(actor_id),
            "connectionId": str(connection_id),
            "selections": [
                {
                    "schema": "public",
                    "table": "orders",
                    "columns": [{"name": "id"}],
                }
            ],
        }
    )

    with pytest.raises(ExecutionValidationError, match="Connector metadata is required"):
        asyncio.run(
            service._create_table_dataset_from_selection(
                request=request,
                schema_name="public",
                table_name="orders",
                columns=request.selections[0].columns,
            )
        )
