import re
import uuid
from typing import Any

from langbridge.federation.models import (
    ExecutionSummary,
    FederationWorkflow,
    LogicalPlan,
    PhysicalPlan,
    VirtualDataset,
    VirtualTableBinding,
)
from langbridge.runtime.models import SqlJob, SqlSelectedDataset
from langbridge.runtime.ports import DatasetCatalogStore
from langbridge.runtime.providers import DatasetMetadataProvider
from langbridge.runtime.services.dataset_execution import DatasetExecutionResolver
from langbridge.runtime.services.errors import ExecutionValidationError
from langbridge.runtime.services.federation_diagnostics import build_runtime_federation_diagnostics
from langbridge.runtime.settings import runtime_settings as settings
from langbridge.runtime.utils.datasets import dataset_supports_structured_federation


class SqlFederatedWorkflowBuilder:
    """Builds dataset-backed federation workflows for SQL jobs."""

    def __init__(
        self,
        *,
        dataset_repository: DatasetCatalogStore,
        dataset_provider: DatasetMetadataProvider,
        dataset_execution_resolver: DatasetExecutionResolver,
    ) -> None:
        self._dataset_repository = dataset_repository
        self._dataset_provider = dataset_provider
        self._dataset_execution_resolver = dataset_execution_resolver

    async def build_workflow(
        self,
        *,
        workspace_id: uuid.UUID,
        query: str,
        source_dialect: str,
        selected_dataset_ids: list[uuid.UUID],
        job: SqlJob,
    ) -> tuple[FederationWorkflow, list[SqlSelectedDataset]]:
        return await self.build_dataset_workflow(
            workspace_id=workspace_id,
            query=query,
            source_dialect=source_dialect,
            selected_dataset_ids=selected_dataset_ids,
            job=job,
        )

    async def build_dataset_workflow(
        self,
        *,
        workspace_id: uuid.UUID,
        query: str,
        source_dialect: str,
        selected_dataset_ids: list[uuid.UUID],
        job: SqlJob,
    ) -> tuple[FederationWorkflow, list[SqlSelectedDataset]]:
        federated_datasets, datasets_by_id = await self.resolve_datasets(
            workspace_id=workspace_id,
            selected_dataset_ids=selected_dataset_ids,
        )
        table_bindings = self.extract_table_bindings(
            selected_datasets=federated_datasets,
            datasets_by_id=datasets_by_id,
        )
        workflow_id = f"workflow_sql_{job.id.hex[:12]}"
        dataset_id = f"dataset_sql_{job.id.hex[:12]}"
        dataset_name = f"sql_job_{job.id.hex[:8]}"
        return (
            FederationWorkflow(
                id=workflow_id,
                workspace_id=str(workspace_id),
                dataset=VirtualDataset(
                    id=dataset_id,
                    name=dataset_name,
                    workspace_id=str(workspace_id),
                    tables=table_bindings,
                    relationships=[],
                ),
                broadcast_threshold_bytes=settings.FEDERATION_BROADCAST_THRESHOLD_BYTES,
                partition_count=settings.FEDERATION_PARTITION_COUNT,
                max_stage_retries=settings.FEDERATION_STAGE_MAX_RETRIES,
                stage_parallelism=settings.FEDERATION_STAGE_PARALLELISM,
            ),
            federated_datasets,
        )

    async def resolve_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        selected_dataset_ids: list[uuid.UUID],
    ) -> tuple[list[SqlSelectedDataset], dict[uuid.UUID, Any]]:
        if selected_dataset_ids:
            datasets = await self.get_datasets_for_workspace(
                workspace_id=workspace_id,
                dataset_ids=selected_dataset_ids,
            )
            datasets_by_id = {dataset.id: dataset for dataset in datasets}
            missing_dataset_ids = [
                dataset_id for dataset_id in selected_dataset_ids if dataset_id not in datasets_by_id
            ]
            if missing_dataset_ids:
                missing = ", ".join(str(dataset_id) for dataset_id in missing_dataset_ids)
                raise ExecutionValidationError(
                    f"Selected federated datasets were not found in workspace '{workspace_id}': {missing}."
                )
        else:
            datasets = await self.list_datasets_for_workspace(workspace_id=workspace_id)

        eligible_datasets: list[Any] = []
        ineligible_dataset_names: list[str] = []
        for dataset in datasets:
            descriptor = self._dataset_execution_resolver._build_dataset_execution_descriptor(dataset)
            if dataset_supports_structured_federation(
                source_kind=descriptor.source_kind,
                storage_kind=descriptor.storage_kind,
                capabilities=descriptor.execution_capabilities,
            ):
                eligible_datasets.append(dataset)
            else:
                ineligible_dataset_names.append(str(getattr(dataset, "name", dataset.id)))

        if selected_dataset_ids and ineligible_dataset_names:
            raise ExecutionValidationError(
                "Selected datasets do not support federated structured execution: "
                + ", ".join(sorted(ineligible_dataset_names))
            )
        if not eligible_datasets:
            raise ExecutionValidationError(
                "No eligible datasets are available for federated SQL in this workspace."
            )

        ordered_datasets = sorted(
            eligible_datasets,
            key=lambda dataset: (
                str(getattr(dataset, "name", "")).strip().lower(),
                str(getattr(dataset, "id", "")),
            ),
        )
        sql_aliases = self.derive_sql_aliases(ordered_datasets)
        resolved = [
            SqlSelectedDataset(
                alias=sql_aliases[dataset.id],
                sql_alias=sql_aliases[dataset.id],
                dataset_id=dataset.id,
                dataset_name=str(getattr(dataset, "name", "")).strip() or None,
                canonical_reference=self.dataset_canonical_reference(dataset),
                connector_id=getattr(dataset, "connection_id", None),
                source_kind=(
                    getattr(dataset, "source_kind_value", None)
                    or getattr(dataset, "source_kind", None)
                ),
                storage_kind=(
                    getattr(dataset, "storage_kind_value", None)
                    or getattr(dataset, "storage_kind", None)
                ),
            )
            for dataset in ordered_datasets
        ]
        return resolved, {dataset.id: dataset for dataset in ordered_datasets}

    async def list_datasets_for_workspace(self, *, workspace_id: uuid.UUID) -> list[Any]:
        if self._dataset_repository is None:
            raise ExecutionValidationError(
                "Dataset catalog store is required to enumerate workspace datasets for federated SQL."
            )
        limit = max(1, settings.SQL_FEDERATION_MAX_ELIGIBLE_DATASETS)
        datasets = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
            limit=limit + 1,
            offset=0,
        )
        if len(datasets) > limit:
            raise ExecutionValidationError(
                "Federated SQL scope exceeds the default dataset limit for this workspace. "
                "Pass selected_datasets to narrow planner scope."
            )
        return datasets

    def derive_sql_aliases(self, datasets: list[Any]) -> dict[uuid.UUID, str]:
        aliases: dict[uuid.UUID, str] = {}
        used_aliases: set[str] = set()
        for dataset in datasets:
            base_alias = self.normalize_dataset_sql_alias(
                getattr(dataset, "sql_alias", None) or getattr(dataset, "name", None)
            )
            alias = base_alias
            suffix = 2
            while alias in used_aliases:
                alias = f"{base_alias}_{suffix}"
                suffix += 1
            aliases[dataset.id] = alias
            used_aliases.add(alias)
        return aliases

    def normalize_dataset_sql_alias(self, value: Any) -> str:
        alias = re.sub(r"[^a-z0-9_]+", "_", str(value or "").strip().lower())
        alias = re.sub(r"_+", "_", alias).strip("_")
        if not alias:
            alias = "dataset"
        if alias[0].isdigit():
            alias = f"dataset_{alias}"
        return alias

    def dataset_canonical_reference(self, dataset: Any) -> str | None:
        relation_identity = getattr(dataset, "relation_identity_json", None)
        if relation_identity is None:
            relation_identity = getattr(dataset, "relation_identity", None)
        if isinstance(relation_identity, dict):
            value = str(relation_identity.get("canonical_reference") or "").strip()
            return value or None
        return None

    def extract_table_bindings(
        self,
        *,
        selected_datasets: list[SqlSelectedDataset],
        datasets_by_id: dict[uuid.UUID, Any],
    ) -> dict[str, VirtualTableBinding]:
        table_bindings: dict[str, VirtualTableBinding] = {}
        for selection in selected_datasets:
            dataset = datasets_by_id[selection.dataset_id]
            descriptor = self._dataset_execution_resolver._build_dataset_execution_descriptor(dataset)
            if not dataset_supports_structured_federation(
                source_kind=descriptor.source_kind,
                storage_kind=descriptor.storage_kind,
                capabilities=descriptor.execution_capabilities,
            ):
                raise ExecutionValidationError(
                    f"Dataset '{dataset.name}' does not support federated structured execution."
                )
            sql_alias = str(selection.sql_alias or "").strip().lower()
            if not sql_alias:
                raise ExecutionValidationError(f"Dataset '{dataset.name}' is missing a SQL alias.")
            try:
                binding, _dialect = self._dataset_execution_resolver._build_binding_from_dataset_record(
                    dataset=dataset,
                    table_key=sql_alias,
                    logical_schema=None,
                    logical_table_name=sql_alias,
                    catalog_name=None,
                )
            except ExecutionValidationError:
                continue
            binding = self.with_dataset_logical_alias(binding=binding, dataset_alias=sql_alias)
            table_bindings[binding.table_key] = binding
        return table_bindings

    async def get_datasets_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_ids: list[uuid.UUID],
    ) -> list[Any]:
        if self._dataset_repository is not None:
            return await self._dataset_repository.get_by_ids_for_workspace(
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )
        if self._dataset_provider is not None:
            return await self._dataset_provider.get_datasets(
                workspace_id=workspace_id,
                dataset_ids=dataset_ids,
            )
        raise ExecutionValidationError("Dataset metadata provider is required for dataset-backed federated SQL.")

    def with_dataset_logical_alias(
        self,
        *,
        binding: VirtualTableBinding,
        dataset_alias: str | None,
    ) -> VirtualTableBinding:
        metadata = dict(binding.metadata or {})
        if dataset_alias:
            metadata["dataset_alias"] = dataset_alias
        return binding.model_copy(update={"metadata": metadata})

    def build_diagnostics(
        self,
        *,
        workflow: FederationWorkflow,
        planning_payload: dict[str, Any] | None,
        execution_payload: dict[str, Any] | None,
    ):
        if not isinstance(planning_payload, dict):
            return None
        logical_plan_payload = planning_payload.get("logical_plan")
        physical_plan_payload = planning_payload.get("physical_plan")
        if not isinstance(logical_plan_payload, dict) or not isinstance(physical_plan_payload, dict):
            return None
        logical_plan = LogicalPlan.model_validate(logical_plan_payload)
        physical_plan = PhysicalPlan.model_validate(physical_plan_payload)
        execution_summary = (
            ExecutionSummary.model_validate(execution_payload)
            if isinstance(execution_payload, dict)
            else None
        )
        return build_runtime_federation_diagnostics(
            workflow=workflow,
            logical_plan=logical_plan,
            physical_plan=physical_plan,
            execution=execution_summary,
        )
