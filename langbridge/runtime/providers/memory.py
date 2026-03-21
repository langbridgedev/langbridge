from __future__ import annotations

import uuid
from typing import Any

from langbridge.runtime.models import (
    ConnectorMetadata,
    ConnectorSyncState,
    DatasetColumnMetadata,
    DatasetMetadata,
    DatasetPolicyMetadata,
    SemanticModelMetadata,
    SqlJobResultArtifact,
)
from langbridge.runtime.providers.protocols import (
    ConnectorMetadataProvider,
    DatasetMetadataProvider,
    SemanticModelMetadataProvider,
    SqlJobResultArtifactProvider,
    SyncStateProvider,
)


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

    def upsert(self, connector: ConnectorMetadata) -> None:
        self._connectors[connector.id] = connector


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

    def upsert(self, semantic_model: SemanticModelMetadata) -> None:
        self._semantic_models[(semantic_model.workspace_id, semantic_model.id)] = semantic_model


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
        state.status = str(kwargs.get("status") or "failed")
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
