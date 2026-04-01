import uuid
from typing import Any

from langbridge.runtime.models import (
    ConnectorMetadata,
    ConnectorSyncState,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    SemanticModelMetadata,
    SemanticVectorIndexMetadata,
    SqlJobResultArtifact,
)
from langbridge.runtime.providers.protocols import (
    ConnectorMetadataProvider,
    DatasetMetadataProvider,
    SemanticModelMetadataProvider,
    SemanticVectorIndexMetadataProvider,
    SqlJobResultArtifactProvider,
    SyncStateProvider,
)
from langbridge.runtime.models.state import ConnectorSyncStatus


class MemoryDatasetProvider(DatasetMetadataProvider):
    """In-memory dataset metadata provider for ephemeral local runtimes."""

    def __init__(self, datasets: dict[uuid.UUID, DatasetMetadata] | None = None) -> None:
        self._datasets = dict(datasets or {})

    async def get_dataset(self, *, workspace_id, dataset_id) -> DatasetMetadata | None:
        dataset = self._datasets.get(dataset_id)
        if dataset is None or dataset.workspace_id != workspace_id:
            return None
        return dataset

    async def get_datasets(self, *, workspace_id, dataset_ids) -> list[DatasetMetadata]:
        items: list[DatasetMetadata] = []
        for dataset_id in dataset_ids:
            dataset = await self.get_dataset(workspace_id=workspace_id, dataset_id=dataset_id)
            if dataset is not None:
                items.append(dataset)
        return items

    async def get_dataset_columns(self, *, dataset_id) -> list[DatasetColumnMetadata]:
        dataset = self._datasets.get(dataset_id)
        return list(dataset.columns) if dataset is not None else []

    async def get_dataset_policy(self, *, dataset_id) -> DatasetPolicyMetadata | None:
        dataset = self._datasets.get(dataset_id)
        return dataset.policy if dataset is not None else None

    def upsert(self, dataset: DatasetMetadata) -> None:
        self._datasets[dataset.id] = dataset

    def remove(self, *, dataset_id: uuid.UUID) -> None:
        self._datasets.pop(dataset_id, None)


class MemoryConnectorProvider(ConnectorMetadataProvider):
    def __init__(self, connectors: dict[uuid.UUID, ConnectorMetadata] | None = None) -> None:
        self._connectors = dict(connectors or {})

    async def get_connector(
        self,
        *,
        workspace_id: uuid.UUID,
        connector_id: uuid.UUID,
    ) -> ConnectorMetadata | None:
        connector = self._connectors.get(connector_id)
        if connector is None:
            return None
        if connector.workspace_id is None or connector.workspace_id == workspace_id:
            return connector
        return None

    async def get_connector_by_name(
        self,
        *,
        workspace_id: uuid.UUID,
        connector_name: str,
    ) -> ConnectorMetadata | None:
        normalized_name = str(connector_name or "").strip()
        if not normalized_name:
            return None
        for connector in self._connectors.values():
            if connector.name != normalized_name:
                continue
            if connector.workspace_id is None or connector.workspace_id == workspace_id:
                return connector
        return None

    def upsert(self, connector: ConnectorMetadata) -> None:
        self._connectors[connector.id] = connector

    def remove(self, *, connector_id: uuid.UUID) -> None:
        self._connectors.pop(connector_id, None)


class MemorySemanticModelProvider(SemanticModelMetadataProvider):
    def __init__(
        self,
        semantic_models: dict[tuple[uuid.UUID, uuid.UUID], SemanticModelMetadata] | None = None,
    ) -> None:
        self._semantic_models = dict(semantic_models or {})

    async def get_semantic_model(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
    ) -> SemanticModelMetadata | None:
        return self._semantic_models.get((workspace_id, semantic_model_id))

    async def get_semantic_models(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_ids: list[uuid.UUID] | None = None,
    ) -> list[SemanticModelMetadata]:
        items = [
            model
            for (ws_id, _), model in self._semantic_models.items()
            if ws_id == workspace_id
        ]
        if semantic_model_ids is not None:
            items = [model for model in items if model.id in semantic_model_ids]
        return items

    def upsert(self, semantic_model: SemanticModelMetadata) -> None:
        self._semantic_models[(semantic_model.workspace_id, semantic_model.id)] = semantic_model

    def remove(self, *, workspace_id: uuid.UUID, semantic_model_id: uuid.UUID) -> None:
        self._semantic_models.pop((workspace_id, semantic_model_id), None)


class MemorySemanticVectorIndexProvider(SemanticVectorIndexMetadataProvider):
    def __init__(
        self,
        indexes: dict[tuple[uuid.UUID, uuid.UUID], SemanticVectorIndexMetadata] | None = None,
    ) -> None:
        self._indexes = dict(indexes or {})

    async def get_semantic_vector_index(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_vector_index_id: uuid.UUID,
    ) -> SemanticVectorIndexMetadata | None:
        return self._indexes.get((workspace_id, semantic_vector_index_id))

    async def get_semantic_vector_index_for_dimension(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
        dataset_key: str,
        dimension_name: str,
    ) -> SemanticVectorIndexMetadata | None:
        normalized_dataset = str(dataset_key or "").strip()
        normalized_dimension = str(dimension_name or "").strip()
        for (ws_id, _), index in self._indexes.items():
            if ws_id != workspace_id:
                continue
            if index.semantic_model_id != semantic_model_id:
                continue
            if index.dataset_key != normalized_dataset:
                continue
            if index.dimension_name != normalized_dimension:
                continue
            return index
        return None

    async def get_by_id(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_vector_index_id: uuid.UUID,
    ) -> SemanticVectorIndexMetadata | None:
        return await self.get_semantic_vector_index(
            workspace_id=workspace_id,
            semantic_vector_index_id=semantic_vector_index_id,
        )

    async def get_for_dimension(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
        dataset_key: str,
        dimension_name: str,
    ) -> SemanticVectorIndexMetadata | None:
        return await self.get_semantic_vector_index_for_dimension(
            workspace_id=workspace_id,
            semantic_model_id=semantic_model_id,
            dataset_key=dataset_key,
            dimension_name=dimension_name,
        )

    async def list_semantic_vector_indexes(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID | None = None,
    ) -> list[SemanticVectorIndexMetadata]:
        items = [
            index
            for (ws_id, _), index in self._indexes.items()
            if ws_id == workspace_id
        ]
        if semantic_model_id is not None:
            items = [index for index in items if index.semantic_model_id == semantic_model_id]
        return items

    async def list_for_workspace(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID | None = None,
    ) -> list[SemanticVectorIndexMetadata]:
        return await self.list_semantic_vector_indexes(
            workspace_id=workspace_id,
            semantic_model_id=semantic_model_id,
        )

    def upsert(self, index: SemanticVectorIndexMetadata) -> None:
        self._indexes[(index.workspace_id, index.id)] = index

    async def save(self, index: SemanticVectorIndexMetadata) -> SemanticVectorIndexMetadata:
        self.upsert(index)
        return index

    async def delete(self, *, workspace_id: uuid.UUID, semantic_vector_index_id: uuid.UUID) -> None:
        self._indexes.pop((workspace_id, semantic_vector_index_id), None)


class MemorySyncStateProvider(SyncStateProvider):
    def __init__(
        self,
        states: dict[tuple[uuid.UUID, uuid.UUID, str], ConnectorSyncState] | None = None,
    ) -> None:
        self._states = dict(states or {})

    async def get_or_create_state(self, **kwargs: Any) -> ConnectorSyncState:
        key = (
            kwargs["workspace_id"],
            kwargs["connection_id"],
            kwargs["resource_name"],
        )
        state = self._states.get(key)
        if state is not None:
            return state
        candidate = kwargs["factory"]()
        if isinstance(candidate, ConnectorSyncState):
            state = candidate
        else:
            state = ConnectorSyncState.model_validate(candidate)
        self._states[key] = state
        return state

    async def mark_failed(self, **kwargs: Any) -> None:
        state = kwargs["state"]
        state.status = ConnectorSyncStatus(
            str(
                getattr(
                    kwargs.get("status"),
                    "value",
                    kwargs.get("status") or ConnectorSyncStatus.FAILED.value,
                )
            ).lower()
        )
        state.error_message = str(kwargs.get("error_message") or "")
        key = (state.workspace_id, state.connection_id, state.resource_name)
        self._states[key] = state


class MemorySqlJobResultArtifactProvider(SqlJobResultArtifactProvider):
    def __init__(self, artifacts: dict[uuid.UUID, SqlJobResultArtifact] | None = None) -> None:
        self._artifacts = dict(artifacts or {})

    async def create_sql_job_result_artifact(self, **kwargs: Any) -> SqlJobResultArtifact:
        artifact = kwargs.get("artifact")
        if artifact is None:
            artifact = SqlJobResultArtifact.model_validate(kwargs)
        elif not isinstance(artifact, SqlJobResultArtifact):
            artifact = SqlJobResultArtifact.model_validate(artifact)
        self._artifacts[artifact.id] = artifact
        return artifact

    async def list_sql_job_result_artifacts(self, **kwargs: Any) -> list[SqlJobResultArtifact]:
        sql_job_id = kwargs.get("sql_job_id")
        workspace_id = kwargs.get("workspace_id")
        items = list(self._artifacts.values())
        if sql_job_id is not None:
            items = [artifact for artifact in items if artifact.sql_job_id == sql_job_id]
        if workspace_id is not None:
            items = [artifact for artifact in items if artifact.workspace_id == workspace_id]
        return items
