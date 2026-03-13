import uuid

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, HTTPException, status
from pydantic import BaseModel

from langbridge.apps.api.langbridge_api.ioc.container import Container
from langbridge.apps.api.langbridge_api.services.runtime_metadata_service import (
    RuntimeMetadataService,
)

router = APIRouter(prefix="/runtime-metadata", tags=["runtime-metadata"])


class DatasetBatchRequest(BaseModel):
    workspace_id: uuid.UUID
    dataset_ids: list[uuid.UUID]


class SyncStateUpsertRequest(BaseModel):
    workspace_id: uuid.UUID
    connection_id: uuid.UUID
    connector_type: str
    resource_name: str
    sync_mode: str = "INCREMENTAL"


class SyncStateFailureRequest(BaseModel):
    workspace_id: uuid.UUID
    connection_id: uuid.UUID
    resource_name: str
    error_message: str
    status: str = "failed"


@router.get("/workspaces/{workspace_id}/datasets/{dataset_id}")
@inject
async def get_dataset(
    workspace_id: uuid.UUID,
    dataset_id: uuid.UUID,
    runtime_metadata_service: RuntimeMetadataService = Depends(
        Provide[Container.runtime_metadata_service]
    ),
):
    payload = await runtime_metadata_service.get_dataset(
        workspace_id=workspace_id,
        dataset_id=dataset_id,
    )
    if payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Dataset not found.")
    return payload


@router.post("/datasets/batch")
@inject
async def get_datasets(
    request: DatasetBatchRequest,
    runtime_metadata_service: RuntimeMetadataService = Depends(
        Provide[Container.runtime_metadata_service]
    ),
):
    return await runtime_metadata_service.get_datasets(
        workspace_id=request.workspace_id,
        dataset_ids=request.dataset_ids,
    )


@router.get("/datasets/{dataset_id}/columns")
@inject
async def get_dataset_columns(
    dataset_id: uuid.UUID,
    runtime_metadata_service: RuntimeMetadataService = Depends(
        Provide[Container.runtime_metadata_service]
    ),
):
    return await runtime_metadata_service.get_dataset_columns(dataset_id=dataset_id)


@router.get("/datasets/{dataset_id}/policy")
@inject
async def get_dataset_policy(
    dataset_id: uuid.UUID,
    runtime_metadata_service: RuntimeMetadataService = Depends(
        Provide[Container.runtime_metadata_service]
    ),
):
    return await runtime_metadata_service.get_dataset_policy(dataset_id=dataset_id)


@router.get("/connectors/{connector_id}")
@inject
async def get_connector(
    connector_id: uuid.UUID,
    runtime_metadata_service: RuntimeMetadataService = Depends(
        Provide[Container.runtime_metadata_service]
    ),
):
    payload = await runtime_metadata_service.get_connector(connector_id=connector_id)
    if payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Connector not found.")
    return payload


@router.get("/organizations/{organization_id}/semantic-models/{semantic_model_id}")
@inject
async def get_semantic_model(
    organization_id: uuid.UUID,
    semantic_model_id: uuid.UUID,
    runtime_metadata_service: RuntimeMetadataService = Depends(
        Provide[Container.runtime_metadata_service]
    ),
):
    payload = await runtime_metadata_service.get_semantic_model(
        organization_id=organization_id,
        semantic_model_id=semantic_model_id,
    )
    if payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Semantic model not found.")
    return payload


@router.get("/workspaces/{workspace_id}/connectors/{connection_id}/sync-state/{resource_name}")
@inject
async def get_sync_state(
    workspace_id: uuid.UUID,
    connection_id: uuid.UUID,
    resource_name: str,
    runtime_metadata_service: RuntimeMetadataService = Depends(
        Provide[Container.runtime_metadata_service]
    ),
):
    return await runtime_metadata_service.get_sync_state(
        workspace_id=workspace_id,
        connection_id=connection_id,
        resource_name=resource_name,
    )


@router.post("/sync-states/upsert")
@inject
async def upsert_sync_state(
    request: SyncStateUpsertRequest,
    runtime_metadata_service: RuntimeMetadataService = Depends(
        Provide[Container.runtime_metadata_service]
    ),
):
    return await runtime_metadata_service.upsert_sync_state(
        workspace_id=request.workspace_id,
        connection_id=request.connection_id,
        connector_type=request.connector_type,
        resource_name=request.resource_name,
        sync_mode=request.sync_mode,
    )


@router.post("/sync-states/fail")
@inject
async def mark_sync_state_failed(
    request: SyncStateFailureRequest,
    runtime_metadata_service: RuntimeMetadataService = Depends(
        Provide[Container.runtime_metadata_service]
    ),
):
    payload = await runtime_metadata_service.mark_sync_state_failed(
        workspace_id=request.workspace_id,
        connection_id=request.connection_id,
        resource_name=request.resource_name,
        error_message=request.error_message,
        status=request.status,
    )
    if payload is None:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail="Sync state not found.")
    return payload
