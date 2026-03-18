from __future__ import annotations

from typing import Any

from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import (
    SemanticModelRepository,
)
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlJobResultArtifactRepository,
)
from langbridge.packages.runtime.adapters import (
    to_legacy_sql_job_result_artifact,
    to_runtime_connector,
    to_runtime_secret_reference,
    to_runtime_semantic_model,
    to_runtime_sql_job_result_artifact,
)
from langbridge.packages.runtime.models import SqlJobResultArtifact
from langbridge.packages.runtime.ports import (
    ConnectorSyncStateStore,
    DatasetCatalogStore,
    DatasetColumnStore,
    DatasetPolicyStore,
)
from langbridge.packages.runtime.providers.protocols import (
    ConnectorMetadataProvider,
    CredentialProvider,
    DatasetMetadataProvider,
    SemanticModelMetadataProvider,
    SqlJobResultArtifactProvider,
    SyncStateProvider,
)
from langbridge.packages.runtime.security import SecretProviderRegistry


class RepositoryDatasetMetadataProvider(DatasetMetadataProvider):
    def __init__(
        self,
        *,
        dataset_repository: DatasetCatalogStore,
        dataset_column_repository: DatasetColumnStore | None = None,
        dataset_policy_repository: DatasetPolicyStore | None = None,
    ) -> None:
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._dataset_policy_repository = dataset_policy_repository

    async def get_dataset(self, *, workspace_id, dataset_id) -> Any:
        return await self._dataset_repository.get_for_workspace(
            dataset_id=dataset_id,
            workspace_id=workspace_id,
        )

    async def get_datasets(self, *, workspace_id, dataset_ids) -> list[Any]:
        return await self._dataset_repository.get_by_ids_for_workspace(
            workspace_id=workspace_id,
            dataset_ids=dataset_ids,
        )

    async def get_dataset_columns(self, *, dataset_id) -> list[Any]:
        if self._dataset_column_repository is None:
            return []
        return await self._dataset_column_repository.list_for_dataset(dataset_id=dataset_id)

    async def get_dataset_policy(self, *, dataset_id) -> Any | None:
        if self._dataset_policy_repository is None:
            return None
        return await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset_id)


class RepositoryConnectorMetadataProvider(ConnectorMetadataProvider):
    def __init__(self, *, connector_repository: ConnectorRepository) -> None:
        self._connector_repository = connector_repository

    async def get_connector(self, connector_id) -> Any | None:
        connector = await self._connector_repository.get_by_id(connector_id)
        return to_runtime_connector(connector)


class RepositorySemanticModelMetadataProvider(SemanticModelMetadataProvider):
    def __init__(self, *, semantic_model_repository: SemanticModelRepository) -> None:
        self._semantic_model_repository = semantic_model_repository

    async def get_semantic_model(self, *, organization_id, semantic_model_id) -> Any | None:
        semantic_model = await self._semantic_model_repository.get_for_scope(
            model_id=semantic_model_id,
            organization_id=organization_id,
        )
        return to_runtime_semantic_model(semantic_model)


class RepositorySyncStateProvider(SyncStateProvider):
    def __init__(self, *, connector_sync_state_repository: ConnectorSyncStateStore) -> None:
        self._connector_sync_state_repository = connector_sync_state_repository

    async def get_or_create_state(self, **kwargs: Any) -> Any:
        state = await self._connector_sync_state_repository.get_for_resource(
            workspace_id=kwargs["workspace_id"],
            connection_id=kwargs["connection_id"],
            resource_name=kwargs["resource_name"],
        )
        if state is not None:
            return state
        state = kwargs["factory"]()
        self._connector_sync_state_repository.add(state)
        return state

    async def mark_failed(self, **kwargs: Any) -> None:
        state = kwargs["state"]
        state.status = kwargs["status"]
        state.error_message = kwargs["error_message"]
        await self._connector_sync_state_repository.save(state)

class SqlArtifactRepository(SqlJobResultArtifactProvider):
    def __init__(self, *, sql_job_result_artifact_repository: SqlJobResultArtifactRepository) -> None:
        self._sql_job_result_artifact_repository = sql_job_result_artifact_repository

    async def create_sql_job_result_artifact(self, **kwargs: Any) -> Any:
        artifact = kwargs.get("artifact")
        if artifact is None:
            artifact = SqlJobResultArtifact.model_validate(kwargs)
        elif not isinstance(artifact, SqlJobResultArtifact):
            artifact = SqlJobResultArtifact.model_validate(artifact)
        record = self._sql_job_result_artifact_repository.add(
            to_legacy_sql_job_result_artifact(artifact)
        )
        return to_runtime_sql_job_result_artifact(record)

    async def list_sql_job_result_artifacts(self, **kwargs: Any) -> list[Any]:
        sql_job_id = kwargs.get("sql_job_id")
        if sql_job_id is None:
            return []
        artifacts = await self._sql_job_result_artifact_repository.list_for_job(
            sql_job_id=sql_job_id
        )
        return [
            artifact
            for item in artifacts
            if (artifact := to_runtime_sql_job_result_artifact(item)) is not None
        ]


class SecretRegistryCredentialProvider(CredentialProvider):
    def __init__(self, *, registry: SecretProviderRegistry | None = None) -> None:
        self._registry = registry or SecretProviderRegistry()

    def resolve_secret(self, reference) -> str:
        return self._registry.resolve(to_runtime_secret_reference(reference))
