from __future__ import annotations

import uuid
from typing import Any, Protocol

from langbridge.packages.common.langbridge_common.contracts.connectors import SecretReference
from langbridge.packages.common.langbridge_common.contracts.datasets import DatasetResponse, DatasetColumnResponse, DatasetPolicyResponse
from langbridge.packages.common.langbridge_common.contracts.semantic import SemanticModelRecordResponse
from langbridge.packages.common.langbridge_common.contracts.connectors import ConnectorResponse


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