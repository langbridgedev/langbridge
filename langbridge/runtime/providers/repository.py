
from typing import Any

from langbridge.runtime.models import SqlJobResultArtifact
from langbridge.runtime.persistence.mappers import (
    from_connector_record,
    from_semantic_model_record,
    from_semantic_vector_index_record,
    from_sql_job_result_artifact_record,
    to_secret_reference,
    to_sql_job_result_artifact_record,
)
from langbridge.runtime.persistence.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.runtime.persistence.repositories.semantic_model_repository import (
    SemanticModelRepository,
)
from langbridge.runtime.persistence.repositories.semantic_search_repository import (
    SemanticVectorIndexRepository,
)
from langbridge.runtime.persistence.repositories.sql_repository import (
    SqlJobResultArtifactRepository,
)
from langbridge.runtime.ports import (
    ConnectorSyncStateStore,
    DatasetCatalogStore,
    DatasetColumnStore,
    DatasetPolicyStore,
)
from langbridge.runtime.providers.protocols import (
    ConnectorMetadataProvider,
    CredentialProvider,
    DatasetMetadataProvider,
    SemanticModelMetadataProvider,
    SemanticVectorIndexMetadataProvider,
    SqlJobResultArtifactProvider,
    SyncStateProvider,
)
from langbridge.runtime.security import SecretProviderRegistry


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

    async def get_connector(self, *, workspace_id, connector_id) -> Any | None:
        connector = await self._connector_repository.get_by_id_for_workspace(
            connector_id=connector_id,
            workspace_id=workspace_id,
        )
        return from_connector_record(connector)

    async def get_connector_by_name(self, *, workspace_id, connector_name) -> Any | None:
        connector = await self._connector_repository.get_by_name(connector_name)
        runtime_connector = from_connector_record(connector)
        if runtime_connector is None:
            return None
        if runtime_connector.workspace_id is None or runtime_connector.workspace_id == workspace_id:
            return runtime_connector
        return None


class RepositorySemanticModelMetadataProvider(SemanticModelMetadataProvider):
    def __init__(self, *, semantic_model_repository: SemanticModelRepository) -> None:
        self._semantic_model_repository = semantic_model_repository

    async def get_semantic_model(self, *, workspace_id, semantic_model_id) -> Any | None:
        semantic_model = await self._semantic_model_repository.get_for_workspace(
            model_id=semantic_model_id,
            workspace_id=workspace_id,
        )
        return from_semantic_model_record(semantic_model)

    async def get_semantic_models(self, *, workspace_id, semantic_model_ids) -> list[Any]:
        if semantic_model_ids is None:
            semantic_models = await self._semantic_model_repository.list_for_workspace(workspace_id)
        else:
            semantic_models = await self._semantic_model_repository.get_by_ids_for_workspace(
                workspace_id=workspace_id,
                model_ids=semantic_model_ids,
            )
        return [
            runtime_model
            for model in semantic_models
            if (runtime_model := from_semantic_model_record(model)) is not None
        ]


class RepositorySemanticVectorIndexMetadataProvider(SemanticVectorIndexMetadataProvider):
    def __init__(self, *, semantic_vector_index_repository: SemanticVectorIndexRepository) -> None:
        self._semantic_vector_index_repository = semantic_vector_index_repository

    async def get_semantic_vector_index(self, *, workspace_id, semantic_vector_index_id) -> Any | None:
        index = await self._semantic_vector_index_repository.get_for_workspace(
            vector_index_id=semantic_vector_index_id,
            workspace_id=workspace_id,
        )
        return from_semantic_vector_index_record(index)

    async def get_semantic_vector_index_for_dimension(
        self,
        *,
        workspace_id,
        semantic_model_id,
        dataset_key,
        dimension_name,
    ) -> Any | None:
        index = await self._semantic_vector_index_repository.get_for_dimension(
            workspace_id=workspace_id,
            semantic_model_id=semantic_model_id,
            dataset_key=dataset_key,
            dimension_name=dimension_name,
        )
        return from_semantic_vector_index_record(index)

    async def list_semantic_vector_indexes(self, *, workspace_id, semantic_model_id=None) -> list[Any]:
        indexes = await self._semantic_vector_index_repository.list_for_workspace(
            workspace_id=workspace_id,
            semantic_model_id=semantic_model_id,
        )
        return [
            runtime_index
            for item in indexes
            if (runtime_index := from_semantic_vector_index_record(item)) is not None
        ]


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
            to_sql_job_result_artifact_record(artifact)
        )
        return from_sql_job_result_artifact_record(record)

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
            if (artifact := from_sql_job_result_artifact_record(item)) is not None
        ]


class SecretRegistryCredentialProvider(CredentialProvider):
    def __init__(self, *, registry: SecretProviderRegistry | None = None) -> None:
        self._registry = registry or SecretProviderRegistry()

    def resolve_secret(self, reference) -> str:
        return self._registry.resolve(to_secret_reference(reference))
