from __future__ import annotations

import uuid
from typing import Any

from langbridge.runtime.providers.protocols import (
    ConnectorMetadataProvider,
    DatasetMetadataProvider,
    SemanticModelMetadataProvider,
)


class CachedDatasetMetadataProvider(DatasetMetadataProvider):
    def __init__(self, inner: DatasetMetadataProvider) -> None:
        self._inner = inner
        self._datasets: dict[tuple[uuid.UUID, uuid.UUID], Any] = {}

    async def get_dataset(self, *, workspace_id, dataset_id) -> Any:
        key = (workspace_id, dataset_id)
        if key not in self._datasets:
            self._datasets[key] = await self._inner.get_dataset(
                workspace_id=workspace_id,
                dataset_id=dataset_id,
            )
        return self._datasets[key]

    async def get_datasets(self, *, workspace_id, dataset_ids) -> list[Any]:
        return [
            await self.get_dataset(workspace_id=workspace_id, dataset_id=dataset_id)
            for dataset_id in dataset_ids
        ]

    async def get_dataset_columns(self, *, dataset_id) -> list[Any]:
        return await self._inner.get_dataset_columns(dataset_id=dataset_id)

    async def get_dataset_policy(self, *, dataset_id) -> Any | None:
        return await self._inner.get_dataset_policy(dataset_id=dataset_id)


class CachedConnectorMetadataProvider(ConnectorMetadataProvider):
    def __init__(self, inner: ConnectorMetadataProvider) -> None:
        self._inner = inner
        self._connectors: dict[tuple[uuid.UUID, uuid.UUID], Any | None] = {}
        self._connectors_by_name: dict[tuple[uuid.UUID, str], Any | None] = {}

    async def get_connector(self, *, workspace_id, connector_id) -> Any | None:
        key = (workspace_id, connector_id)
        if key not in self._connectors:
            self._connectors[key] = await self._inner.get_connector(
                workspace_id=workspace_id,
                connector_id=connector_id,
            )
        return self._connectors[key]

    async def get_connector_by_name(self, *, workspace_id, connector_name) -> Any | None:
        normalized_name = str(connector_name or "").strip()
        key = (workspace_id, normalized_name)
        if key not in self._connectors_by_name:
            self._connectors_by_name[key] = await self._inner.get_connector_by_name(
                workspace_id=workspace_id,
                connector_name=normalized_name,
            )
        return self._connectors_by_name[key]


class CachedSemanticModelMetadataProvider(SemanticModelMetadataProvider):
    def __init__(self, inner: SemanticModelMetadataProvider) -> None:
        self._inner = inner
        self._models: dict[tuple[uuid.UUID, uuid.UUID], Any | None] = {}

    async def get_semantic_model(self, *, workspace_id, semantic_model_id) -> Any | None:
        key = (workspace_id, semantic_model_id)
        if key not in self._models:
            self._models[key] = await self._inner.get_semantic_model(
                workspace_id=workspace_id,
                semantic_model_id=semantic_model_id,
            )
        return self._models[key]

    async def get_semantic_models(self, *, workspace_id, semantic_model_ids=None) -> list[Any]:
        if semantic_model_ids is None:
            # If no specific IDs are provided, we can't cache the list of all models effectively,
            # so we delegate directly to the inner provider without caching.
            return await self._inner.get_semantic_models(workspace_id=workspace_id, semantic_model_ids=None)

        # For specific IDs, we can utilize caching.
        return [
            await self.get_semantic_model(workspace_id=workspace_id, semantic_model_id=model_id)
            for model_id in semantic_model_ids
        ]
