from __future__ import annotations

import asyncio
import uuid

from langbridge.runtime.models import DatasetMetadata
from langbridge.runtime.services.sql_query_service import SqlQueryService


class _DatasetRepository:
    def __init__(self, datasets: list[DatasetMetadata]) -> None:
        self._datasets = {dataset.id: dataset for dataset in datasets}

    async def get_by_ids_for_workspace(self, *, workspace_id, dataset_ids):
        return [
            dataset
            for dataset_id in dataset_ids
            if (dataset := self._datasets.get(dataset_id)) is not None
            and dataset.workspace_id == workspace_id
        ]

    async def list_for_workspace(self, *, workspace_id, limit=200, offset=0, **kwargs):
        datasets = [
            dataset
            for dataset in self._datasets.values()
            if dataset.workspace_id == workspace_id
        ]
        return datasets[offset : offset + limit]


def _dataset(
    *,
    workspace_id: uuid.UUID,
    name: str,
    sql_alias: str,
    supports_sql_federation: bool = True,
) -> DatasetMetadata:
    dataset_id = uuid.uuid4()
    table_name = name.strip().lower().replace(" ", "_")
    return DatasetMetadata(
        id=dataset_id,
        workspace_id=workspace_id,
        connection_id=uuid.uuid4(),
        name=name,
        sql_alias=sql_alias,
        dataset_type="TABLE",
        source_kind="database",
        storage_kind="table",
        dialect="postgres",
        schema_name="public",
        table_name=table_name,
        relation_identity={
            "canonical_reference": f"dataset:{dataset_id}",
            "relation_name": table_name,
            "source_kind": "database",
            "storage_kind": "table",
        },
        execution_capabilities={
            "supports_structured_scan": True,
            "supports_sql_federation": supports_sql_federation,
        },
        status="published",
    )


def test_sql_query_service_defaults_to_all_eligible_workspace_datasets() -> None:
    workspace_id = uuid.uuid4()
    sales = _dataset(workspace_id=workspace_id, name="Sales Orders", sql_alias="sales_orders")
    crm = _dataset(workspace_id=workspace_id, name="CRM Contacts", sql_alias="crm_contacts")
    ignored = _dataset(
        workspace_id=workspace_id,
        name="Images",
        sql_alias="images",
        supports_sql_federation=False,
    )
    service = SqlQueryService(
        sql_job_result_artifact_store=None,
        dataset_repository=_DatasetRepository([sales, crm, ignored]),
    )

    selected, datasets_by_id = asyncio.run(
        service._resolve_federated_datasets(
            workspace_id=workspace_id,
            selected_dataset_ids=[],
        )
    )

    assert {dataset.dataset_id for dataset in selected} == {sales.id, crm.id}
    assert set(datasets_by_id) == {sales.id, crm.id}
    aliases = {dataset.dataset_id: dataset.sql_alias for dataset in selected}
    assert aliases[sales.id] == "sales_orders"
    assert aliases[crm.id] == "crm_contacts"


def test_sql_query_service_selected_datasets_is_a_subset_selector() -> None:
    workspace_id = uuid.uuid4()
    sales = _dataset(workspace_id=workspace_id, name="Sales Orders", sql_alias="sales orders")
    crm = _dataset(workspace_id=workspace_id, name="CRM Contacts", sql_alias="")
    service = SqlQueryService(
        sql_job_result_artifact_store=None,
        dataset_repository=_DatasetRepository([sales, crm]),
    )

    selected, _datasets_by_id = asyncio.run(
        service._resolve_federated_datasets(
            workspace_id=workspace_id,
            selected_dataset_ids=[crm.id],
        )
    )

    assert [dataset.dataset_id for dataset in selected] == [crm.id]
    assert selected[0].sql_alias == "crm_contacts"
    assert selected[0].alias == "crm_contacts"


def test_sql_query_service_derives_stable_unique_aliases_for_collisions() -> None:
    workspace_id = uuid.uuid4()
    primary = _dataset(workspace_id=workspace_id, name="Sales Orders", sql_alias="sales-orders")
    duplicate = _dataset(workspace_id=workspace_id, name="Sales Orders Clone", sql_alias="sales_orders")
    service = SqlQueryService(
        sql_job_result_artifact_store=None,
        dataset_repository=_DatasetRepository([primary, duplicate]),
    )

    selected, _datasets_by_id = asyncio.run(
        service._resolve_federated_datasets(
            workspace_id=workspace_id,
            selected_dataset_ids=[],
        )
    )

    aliases = {dataset.dataset_id: dataset.sql_alias for dataset in selected}
    assert aliases[primary.id] == "sales_orders"
    assert aliases[duplicate.id] == "sales_orders_2"
