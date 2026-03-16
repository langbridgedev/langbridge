from __future__ import annotations

import uuid
from typing import Any, Protocol

from langbridge.packages.contracts.connectors import ConnectorResponse, SecretReference
from langbridge.packages.contracts.datasets import (
    DatasetColumnResponse,
    DatasetPolicyResponse,
    DatasetResponse,
)
from langbridge.packages.contracts.semantic import SemanticModelRecordResponse


class DatasetMetadataProvider(Protocol):
    async def get_dataset(self, *, workspace_id: uuid.UUID, dataset_id: uuid.UUID) -> DatasetResponse: ...

    async def get_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_ids: list[uuid.UUID],
    ) -> list[DatasetResponse]: ...

    async def get_dataset_columns(self, *, dataset_id: uuid.UUID) -> list[DatasetColumnResponse]: ...

    async def get_dataset_policy(self, *, dataset_id: uuid.UUID) -> DatasetPolicyResponse | None: ...


class SemanticModelMetadataProvider(Protocol):
    async def get_semantic_model(
        self,
        *,
        organization_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
    ) -> SemanticModelRecordResponse | None: ...


class ConnectorMetadataProvider(Protocol):
    async def get_connector(self, connector_id: uuid.UUID) -> Any | None: ...


class SyncStateProvider(Protocol):
    async def get_or_create_state(self, **kwargs: Any) -> Any: ...

    async def mark_failed(self, **kwargs: Any) -> None: ...


class CredentialProvider(Protocol):
    def resolve_secret(self, reference: SecretReference) -> str: ...
