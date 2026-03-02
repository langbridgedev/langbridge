from uuid import UUID

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Body, Depends, HTTPException, Query, status
from fastapi.responses import Response

from langbridge.apps.api.langbridge_api.auth.dependencies import get_current_user
from langbridge.apps.api.langbridge_api.ioc import Container
from langbridge.apps.api.langbridge_api.services.sql_service import SqlService
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.sql import (
    SqlAssistRequest,
    SqlAssistResponse,
    SqlCancelRequest,
    SqlCancelResponse,
    SqlExecuteRequest,
    SqlExecuteResponse,
    SqlHistoryResponse,
    SqlJobResponse,
    SqlJobResultsResponse,
    SqlSavedQueryCreateRequest,
    SqlSavedQueryListResponse,
    SqlSavedQueryResponse,
    SqlSavedQueryUpdateRequest,
    SqlWorkspacePolicyResponse,
    SqlWorkspacePolicyUpdateRequest,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
    PermissionDeniedBusinessValidationError,
    ResourceNotFound,
)


router = APIRouter(prefix="/sql", tags=["sql"])


def _map_sql_service_exception(exc: Exception) -> HTTPException:
    if isinstance(exc, PermissionDeniedBusinessValidationError):
        return HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=exc.message)
    if isinstance(exc, ResourceNotFound):
        return HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc))
    if isinstance(exc, BusinessValidationError):
        return HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=exc.message)
    return HTTPException(status_code=status.HTTP_500_INTERNAL_SERVER_ERROR, detail="SQL request failed. Error: " + str(exc))


@router.post("/execute", response_model=SqlExecuteResponse, status_code=status.HTTP_202_ACCEPTED)
@inject
async def execute_sql(
    request: SqlExecuteRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlExecuteResponse:
    try:
        return await service.execute_sql(request=request, current_user=current_user)
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.post("/cancel", response_model=SqlCancelResponse, status_code=status.HTTP_200_OK)
@inject
async def cancel_sql(
    request: SqlCancelRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlCancelResponse:
    try:
        return await service.cancel_sql_job(request=request, current_user=current_user)
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.get("/jobs/{sql_job_id}", response_model=SqlJobResponse, status_code=status.HTTP_200_OK)
@inject
async def get_sql_job(
    sql_job_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlJobResponse:
    try:
        return await service.get_sql_job(
            sql_job_id=sql_job_id,
            workspace_id=workspace_id,
            current_user=current_user,
        )
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.get(
    "/jobs/{sql_job_id}/results",
    response_model=SqlJobResultsResponse,
    status_code=status.HTTP_200_OK,
)
@inject
async def get_sql_job_results(
    sql_job_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    cursor: str | None = Query(default=None),
    page_size: int = Query(default=100, ge=1, le=5000),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlJobResultsResponse:
    try:
        return await service.get_sql_job_results(
            sql_job_id=sql_job_id,
            workspace_id=workspace_id,
            current_user=current_user,
            cursor=cursor,
            page_size=page_size,
        )
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.get("/jobs/{sql_job_id}/results/download", status_code=status.HTTP_200_OK)
@inject
async def download_sql_job_results(
    sql_job_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    export_format: str = Query(..., alias="format", pattern="^(csv|parquet)$"),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> Response:
    try:
        content, mime_type, file_name = await service.download_sql_job_results(
            sql_job_id=sql_job_id,
            workspace_id=workspace_id,
            current_user=current_user,
            export_format=export_format,
        )
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc

    return Response(
        content=content,
        media_type=mime_type,
        headers={
            "Content-Disposition": f'attachment; filename="{file_name}"',
        },
    )


@router.get("/history", response_model=SqlHistoryResponse, status_code=status.HTTP_200_OK)
@inject
async def list_sql_history(
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    scope: str = Query(default="user", pattern="^(user|workspace)$"),
    limit: int = Query(default=100, ge=1, le=500),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlHistoryResponse:
    try:
        return await service.list_history(
            workspace_id=workspace_id,
            current_user=current_user,
            scope=scope,
            limit=limit,
        )
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.post("/saved", response_model=SqlSavedQueryResponse, status_code=status.HTTP_201_CREATED)
@inject
async def create_saved_sql_query(
    request: SqlSavedQueryCreateRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlSavedQueryResponse:
    try:
        return await service.create_saved_query(request=request, current_user=current_user)
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.get("/saved", response_model=SqlSavedQueryListResponse, status_code=status.HTTP_200_OK)
@inject
async def list_saved_sql_queries(
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlSavedQueryListResponse:
    try:
        return await service.list_saved_queries(
            workspace_id=workspace_id,
            current_user=current_user,
        )
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.get("/saved/{saved_query_id}", response_model=SqlSavedQueryResponse, status_code=status.HTTP_200_OK)
@inject
async def get_saved_sql_query(
    saved_query_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlSavedQueryResponse:
    try:
        return await service.get_saved_query(
            saved_query_id=saved_query_id,
            workspace_id=workspace_id,
            current_user=current_user,
        )
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.put("/saved/{saved_query_id}", response_model=SqlSavedQueryResponse, status_code=status.HTTP_200_OK)
@inject
async def update_saved_sql_query(
    saved_query_id: UUID,
    request: SqlSavedQueryUpdateRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlSavedQueryResponse:
    try:
        return await service.update_saved_query(
            saved_query_id=saved_query_id,
            request=request,
            current_user=current_user,
        )
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.delete("/saved/{saved_query_id}", status_code=status.HTTP_204_NO_CONTENT)
@inject
async def delete_saved_sql_query(
    saved_query_id: UUID,
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> None:
    try:
        await service.delete_saved_query(
            saved_query_id=saved_query_id,
            workspace_id=workspace_id,
            current_user=current_user,
        )
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc
    return None


@router.get("/policies", response_model=SqlWorkspacePolicyResponse, status_code=status.HTTP_200_OK)
@inject
async def get_sql_workspace_policy(
    workspace_id: UUID = Query(..., description="Workspace (organization) scope id."),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlWorkspacePolicyResponse:
    try:
        return await service.get_workspace_policy(
            workspace_id=workspace_id,
            current_user=current_user,
        )
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.put("/policies", response_model=SqlWorkspacePolicyResponse, status_code=status.HTTP_200_OK)
@inject
async def update_sql_workspace_policy(
    request: SqlWorkspacePolicyUpdateRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlWorkspacePolicyResponse:
    try:
        return await service.update_workspace_policy(request=request, current_user=current_user)
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc


@router.post("/assist", response_model=SqlAssistResponse, status_code=status.HTTP_200_OK)
@inject
async def assist_sql(
    request: SqlAssistRequest = Body(...),
    current_user: UserResponse = Depends(get_current_user),
    service: SqlService = Depends(Provide[Container.sql_service]),
) -> SqlAssistResponse:
    try:
        return await service.assist_sql(request=request, current_user=current_user)
    except Exception as exc:
        raise _map_sql_service_exception(exc) from exc
