from __future__ import annotations

from types import SimpleNamespace
from typing import Any

import httpx

from langbridge.packages.runtime.providers.protocols import (
    ConnectorMetadataProvider,
    DatasetMetadataProvider,
    SemanticModelMetadataProvider,
    SyncStateProvider,
)


def _to_namespace(value: Any) -> Any:
    if isinstance(value, dict):
        return SimpleNamespace(**{key: _to_namespace(item) for key, item in value.items()})
    if isinstance(value, list):
        return [_to_namespace(item) for item in value]
    return value


class ControlPlaneApiClient:
    def __init__(self, *, base_url: str, service_token: str, timeout: float = 30.0) -> None:
        self._base_url = base_url.rstrip("/")
        self._service_token = service_token
        self._timeout = timeout

    async def request(self, method: str, path: str, **kwargs: Any) -> Any:
        headers = dict(kwargs.pop("headers", {}) or {})
        headers["x-langbridge-service-token"] = self._service_token
        async with httpx.AsyncClient(timeout=self._timeout) as client:
            response = await client.request(method, f"{self._base_url}{path}", headers=headers, **kwargs)
            response.raise_for_status()
            if not response.content:
                return None
            return response.json()


class ControlPlaneApiDatasetProvider(DatasetMetadataProvider):
    def __init__(self, *, client: ControlPlaneApiClient) -> None:
        self._client = client

    async def get_dataset(self, *, workspace_id, dataset_id) -> Any:
        payload = await self._client.request(
            "GET",
            f"/api/v1/runtime-metadata/workspaces/{workspace_id}/datasets/{dataset_id}",
        )
        return _to_namespace(payload) if payload is not None else None

    async def get_datasets(self, *, workspace_id, dataset_ids) -> list[Any]:
        payload = await self._client.request(
            "POST",
            "/api/v1/runtime-metadata/datasets/batch",
            json={
                "workspace_id": str(workspace_id),
                "dataset_ids": [str(dataset_id) for dataset_id in dataset_ids],
            },
        )
        return [_to_namespace(item) for item in (payload or [])]

    async def get_dataset_columns(self, *, dataset_id) -> list[Any]:
        payload = await self._client.request(
            "GET",
            f"/api/v1/runtime-metadata/datasets/{dataset_id}/columns",
        )
        return [_to_namespace(item) for item in (payload or [])]

    async def get_dataset_policy(self, *, dataset_id) -> Any | None:
        payload = await self._client.request(
            "GET",
            f"/api/v1/runtime-metadata/datasets/{dataset_id}/policy",
        )
        return _to_namespace(payload) if payload is not None else None


class ControlPlaneApiConnectorProvider(ConnectorMetadataProvider):
    def __init__(self, *, client: ControlPlaneApiClient) -> None:
        self._client = client

    async def get_connector(self, connector_id) -> Any | None:
        payload = await self._client.request(
            "GET",
            f"/api/v1/runtime-metadata/connectors/{connector_id}",
        )
        return _to_namespace(payload) if payload is not None else None


class ControlPlaneApiSemanticModelProvider(SemanticModelMetadataProvider):
    def __init__(self, *, client: ControlPlaneApiClient) -> None:
        self._client = client

    async def get_semantic_model(self, *, organization_id, semantic_model_id) -> Any | None:
        payload = await self._client.request(
            "GET",
            f"/api/v1/runtime-metadata/organizations/{organization_id}/semantic-models/{semantic_model_id}",
        )
        return _to_namespace(payload) if payload is not None else None


class ControlPlaneApiSyncStateProvider(SyncStateProvider):
    def __init__(self, *, client: ControlPlaneApiClient) -> None:
        self._client = client

    async def get_or_create_state(self, **kwargs: Any) -> Any:
        payload = await self._client.request(
            "POST",
            "/api/v1/runtime-metadata/sync-states/upsert",
            json={
                "workspace_id": str(kwargs["workspace_id"]),
                "connection_id": str(kwargs["connection_id"]),
                "connector_type": str(kwargs["connector_type"]),
                "resource_name": str(kwargs["resource_name"]),
                "sync_mode": str(kwargs.get("sync_mode") or "INCREMENTAL"),
            },
        )
        return _to_namespace(payload) if payload is not None else None

    async def mark_failed(self, **kwargs: Any) -> None:
        await self._client.request(
            "POST",
            "/api/v1/runtime-metadata/sync-states/fail",
            json={
                "workspace_id": str(kwargs["state"].workspace_id),
                "connection_id": str(kwargs["state"].connection_id),
                "resource_name": str(kwargs["state"].resource_name),
                "error_message": str(kwargs.get("error_message") or ""),
                "status": str(kwargs.get("status") or "failed"),
            },
        )
