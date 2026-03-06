from uuid import UUID

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, File, Form, HTTPException, Query, UploadFile, status

from langbridge.apps.api.langbridge_api.auth.dependencies import get_current_user
from langbridge.apps.api.langbridge_api.ioc import Container
from langbridge.apps.api.langbridge_api.services.dataset_service import DatasetService
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.datasets import (
    DatasetBulkCreateRequest,
    DatasetBulkCreateStartResponse,
    DatasetCatalogResponse,
    DatasetCsvIngestResponse,
    DatasetCreateRequest,
    DatasetEnsureRequest,
    DatasetEnsureResponse,
    DatasetListResponse,
    DatasetLineageResponse,
    DatasetPreviewRequest,
    DatasetPreviewResponse,
    DatasetProfileRequest,
    DatasetProfileResponse,
    DatasetResponse,
    DatasetRestoreRequest,
    DatasetImpactResponse,
    DatasetUpdateRequest,
    DatasetUsageResponse,
    DatasetVersionDiffResponse,
    DatasetVersionListResponse,
    DatasetVersionResponse,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
    PermissionDeniedBusinessValidationError,
    ResourceNotFound,
)


router = APIRouter(prefix="/datasets", tags=["datasets"])


@router.get("", response_model=DatasetListResponse, status_code=status.HTTP_200_OK)
@inject
async def list_datasets(
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    project_id: UUID | None = Query(default=None),
    search: str | None = Query(default=None),
    tags: list[str] = Query(default_factory=list),
    dataset_types: list[str] = Query(default_factory=list),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetListResponse:
    return await service.list_datasets(
        workspace_id=workspace_id,
        project_id=project_id,
        search=search,
        tags=tags,
        dataset_types=dataset_types,
        current_user=current_user,
    )

@router.post("", response_model=DatasetResponse, status_code=status.HTTP_201_CREATED)
@inject
async def create_dataset(
    request: DatasetCreateRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetResponse:
    return await service.create_dataset(request=request, current_user=current_user)


@router.post("/upload-csv", response_model=DatasetCsvIngestResponse, status_code=status.HTTP_202_ACCEPTED)
@inject
async def upload_csv_dataset(
    workspace_id: UUID = Form(...),
    name: str = Form(...),
    file: UploadFile = File(...),
    project_id: UUID | None = Form(default=None),
    description: str | None = Form(default=None),
    tags: str | None = Form(default=None),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetCsvIngestResponse:
    return await service.upload_csv_dataset(
        workspace_id=workspace_id,
        project_id=project_id,
        name=name,
        filename=file.filename or "upload.csv",
        content=await file.read(),
        description=description,
        tags=[item.strip() for item in (tags or "").split(",") if item.strip()],
        current_user=current_user,
    )


@router.post("/bulk-create", response_model=DatasetBulkCreateStartResponse, status_code=status.HTTP_202_ACCEPTED)
@inject
async def bulk_create_datasets(
    request: DatasetBulkCreateRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetBulkCreateStartResponse:
    return await service.start_bulk_create(request=request, current_user=current_user)


@router.post("/ensure", response_model=DatasetEnsureResponse, status_code=status.HTTP_200_OK)
@inject
async def ensure_dataset(
    request: DatasetEnsureRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetEnsureResponse:
    return await service.ensure_dataset(request=request, current_user=current_user)


@router.get("/catalog", response_model=DatasetCatalogResponse, status_code=status.HTTP_200_OK)
@inject
async def get_dataset_catalog(
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    project_id: UUID | None = Query(default=None),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetCatalogResponse:
    return await service.get_catalog(
        workspace_id=workspace_id,
        project_id=project_id,
        current_user=current_user,
    )

@router.get("/{dataset_id}", response_model=DatasetResponse, status_code=status.HTTP_200_OK)
@inject
async def get_dataset(
    dataset_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetResponse:
    return await service.get_dataset(
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        current_user=current_user,
    )

@router.get("/{dataset_id}/versions", response_model=DatasetVersionListResponse, status_code=status.HTTP_200_OK)
@inject
async def list_dataset_versions(
    dataset_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetVersionListResponse:
    return await service.list_dataset_versions(
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        current_user=current_user,
    )

@router.get(
    "/{dataset_id}/versions/{revision_id}",
    response_model=DatasetVersionResponse,
    status_code=status.HTTP_200_OK,
)
@inject
async def get_dataset_version(
    dataset_id: UUID,
    revision_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetVersionResponse:
    return await service.get_dataset_version(
        dataset_id=dataset_id,
        revision_id=revision_id,
        workspace_id=workspace_id,
        current_user=current_user,
    )

@router.get("/{dataset_id}/diff", response_model=DatasetVersionDiffResponse, status_code=status.HTTP_200_OK)
@inject
async def diff_dataset_versions(
    dataset_id: UUID,
    from_revision: UUID = Query(...),
    to_revision: UUID = Query(...),
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetVersionDiffResponse:
    return await service.diff_dataset_versions(
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        from_revision_id=from_revision,
        to_revision_id=to_revision,
        current_user=current_user,
    )

@router.post("/{dataset_id}/restore", response_model=DatasetResponse, status_code=status.HTTP_200_OK)
@inject
async def restore_dataset(
    dataset_id: UUID,
    request: DatasetRestoreRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetResponse:
    return await service.restore_dataset(
        dataset_id=dataset_id,
        request=request,
        current_user=current_user,
    )

@router.get("/{dataset_id}/lineage", response_model=DatasetLineageResponse, status_code=status.HTTP_200_OK)
@inject
async def get_dataset_lineage(
    dataset_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetLineageResponse:
    return await service.get_lineage(
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        current_user=current_user,
    )

@router.get("/{dataset_id}/impact", response_model=DatasetImpactResponse, status_code=status.HTTP_200_OK)
@inject
async def get_dataset_impact(
    dataset_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetImpactResponse:
    return await service.get_impact(
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        current_user=current_user,
    )

@router.put("/{dataset_id}", response_model=DatasetResponse, status_code=status.HTTP_200_OK)
@inject
async def update_dataset(
    dataset_id: UUID,
    request: DatasetUpdateRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetResponse:
    return await service.update_dataset(
        dataset_id=dataset_id,
        request=request,
        current_user=current_user,
    )

@router.delete("/{dataset_id}", status_code=status.HTTP_204_NO_CONTENT)
@inject
async def delete_dataset(
    dataset_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> None:
    await service.delete_dataset(
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        current_user=current_user,
    )


@router.post("/{dataset_id}/preview", response_model=DatasetPreviewResponse, status_code=status.HTTP_200_OK)
@inject
async def preview_dataset(
    dataset_id: UUID,
    request: DatasetPreviewRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetPreviewResponse:
    return await service.preview_dataset(
        dataset_id=dataset_id,
        request=request,
        current_user=current_user,
    )

@router.get("/{dataset_id}/preview/jobs/{job_id}", response_model=DatasetPreviewResponse, status_code=status.HTTP_200_OK)
@inject
async def get_preview_dataset_job(
    dataset_id: UUID,
    job_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetPreviewResponse:
    return await service.get_preview_job_result(
        dataset_id=dataset_id,
        job_id=job_id,
        workspace_id=workspace_id,
        current_user=current_user,
    )

@router.post("/{dataset_id}/profile", response_model=DatasetProfileResponse, status_code=status.HTTP_200_OK)
@inject
async def profile_dataset(
    dataset_id: UUID,
    request: DatasetProfileRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetProfileResponse:
    return await service.profile_dataset(
        dataset_id=dataset_id,
        request=request,
        current_user=current_user,
    )

@router.get("/{dataset_id}/profile/jobs/{job_id}", response_model=DatasetProfileResponse, status_code=status.HTTP_200_OK)
@inject
async def get_profile_dataset_job(
    dataset_id: UUID,
    job_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetProfileResponse:
    return await service.get_profile_job_result(
            dataset_id=dataset_id,
            job_id=job_id,
            workspace_id=workspace_id,
            current_user=current_user,
        )

@router.get("/{dataset_id}/used-by", response_model=DatasetUsageResponse, status_code=status.HTTP_200_OK)
@inject
async def get_dataset_usage(
    dataset_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: DatasetService = Depends(Provide[Container.dataset_service]),
) -> DatasetUsageResponse:
    return await service.get_usage(
        dataset_id=dataset_id,
        workspace_id=workspace_id,
        current_user=current_user,
    )
