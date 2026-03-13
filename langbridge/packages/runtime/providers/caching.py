from __future__ import annotations

import uuid
from typing import Any

from langbridge.packages.runtime.providers.protocols import (
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
        self._connectors: dict[uuid.UUID, Any | None] = {}

    async def get_connector(self, connector_id) -> Any | None:
        if connector_id not in self._connectors:
            self._connectors[connector_id] = await self._inner.get_connector(connector_id)
        return self._connectors[connector_id]


class CachedSemanticModelMetadataProvider(SemanticModelMetadataProvider):
    def __init__(self, inner: SemanticModelMetadataProvider) -> None:
        self._inner = inner
        self._models: dict[tuple[uuid.UUID, uuid.UUID], Any | None] = {}

    async def get_semantic_model(self, *, organization_id, semantic_model_id) -> Any | None:
        key = (organization_id, semantic_model_id)
        if key not in self._models:
            self._models[key] = await self._inner.get_semantic_model(
                organization_id=organization_id,
                semantic_model_id=semantic_model_id,
            )
        return self._models[key]
