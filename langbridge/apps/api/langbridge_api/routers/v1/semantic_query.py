from uuid import UUID

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, HTTPException, status

from langbridge.apps.api.langbridge_api.auth.dependencies import (
    get_current_user,
    get_organization,
)
from langbridge.apps.api.langbridge_api.ioc import Container
from langbridge.apps.api.langbridge_api.services.jobs.semantic_query_job_request_service import (
    SemanticQueryJobRequestService,
)
from langbridge.apps.api.langbridge_api.services.semantic import SemanticQueryService
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.jobs.semantic_query_job import (
    CreateSemanticQueryJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticQueryJobResponse,
    SemanticQueryMetaResponse,
    SemanticQueryRequest,
    SemanticQueryResponse,
    UnifiedSemanticQueryMetaRequest,
    UnifiedSemanticQueryMetaResponse,
    UnifiedSemanticQueryRequest,
    UnifiedSemanticQueryResponse,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)

router = APIRouter(prefix="/semantic-query/{organization_id}", tags=["semantic-query"])


@router.post(
    "/unified/q",
    response_model=UnifiedSemanticQueryResponse,
    status_code=status.HTTP_201_CREATED,
)
@inject
async def unified_semantic_query(
    request: UnifiedSemanticQueryRequest,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    service: SemanticQueryService = Depends(Provide[Container.semantic_query_service]),
) -> UnifiedSemanticQueryResponse:
    if request.organization_id != organization_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="organization_id in path and body must match.",
        )

    if request.project_id and current_user.available_projects is not None:
        allowed = {str(project_id) for project_id in current_user.available_projects}
        if str(request.project_id) not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    try:
        return await service.query_unified_request(request)
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post(
    "/unified/q/jobs",
    response_model=SemanticQueryJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@inject
async def unified_semantic_query_enqueue(
    request: UnifiedSemanticQueryRequest,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    service: SemanticQueryJobRequestService = Depends(
        Provide[Container.semantic_query_job_request_service]
    ),
) -> SemanticQueryJobResponse:
    if request.organization_id != organization_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="organization_id in path and body must match.",
        )

    if request.project_id and current_user.available_projects is not None:
        allowed = {str(project_id) for project_id in current_user.available_projects}
        if str(request.project_id) not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    try:
        job = await service.create_semantic_query_job_request(
            request=CreateSemanticQueryJobRequest(
                organisation_id=organization_id,
                project_id=request.project_id,
                user_id=current_user.id,
                query_scope="unified",
                connector_id=request.connector_id,
                semantic_model_ids=request.semantic_model_ids,
                source_models=request.source_models,
                relationships=request.relationships,
                metrics=request.metrics,
                query=request.query,
            ),
        )
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    return SemanticQueryJobResponse(
        job_id=job.id,
        job_status=(job.status.value if job.status is not None else "queued"),
    )


@router.post(
    "/unified/meta",
    response_model=UnifiedSemanticQueryMetaResponse,
    status_code=status.HTTP_200_OK,
)
@inject
async def unified_semantic_query_meta(
    request: UnifiedSemanticQueryMetaRequest,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    service: SemanticQueryService = Depends(Provide[Container.semantic_query_service]),
) -> UnifiedSemanticQueryMetaResponse:
    if request.organization_id != organization_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="organization_id in path and body must match.",
        )

    if request.project_id and current_user.available_projects is not None:
        allowed = {str(project_id) for project_id in current_user.available_projects}
        if str(request.project_id) not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    try:
        return await service.get_unified_meta(request)
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post(
    "/{semantic_model_id}/q",
    response_model=SemanticQueryResponse,
    status_code=status.HTTP_201_CREATED,
)
@inject
async def semantic_query(
    request: SemanticQueryRequest,
    semantic_model_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    service: SemanticQueryService = Depends(Provide[Container.semantic_query_service]),
) -> SemanticQueryResponse:
    if request.organization_id != organization_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="organization_id in path and body must match.",
        )
    if request.semantic_model_id != semantic_model_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="semantic_model_id in path and body must match.",
        )

    try:
        return await service.query_request(request)
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc


@router.post(
    "/{semantic_model_id}/q/jobs",
    response_model=SemanticQueryJobResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@inject
async def semantic_query_enqueue(
    request: SemanticQueryRequest,
    semantic_model_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    service: SemanticQueryJobRequestService = Depends(
        Provide[Container.semantic_query_job_request_service]
    ),
) -> SemanticQueryJobResponse:
    if request.organization_id != organization_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="organization_id in path and body must match.",
        )
    if request.semantic_model_id != semantic_model_id:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="semantic_model_id in path and body must match.",
        )

    if request.project_id and current_user.available_projects is not None:
        allowed = {str(project_id) for project_id in current_user.available_projects}
        if str(request.project_id) not in allowed:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    try:
        job = await service.create_semantic_query_job_request(
            request=CreateSemanticQueryJobRequest(
                organisation_id=organization_id,
                project_id=request.project_id,
                user_id=current_user.id,
                query_scope="semantic_model",
                semantic_model_id=semantic_model_id,
                query=request.query,
            ),
        )
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    return SemanticQueryJobResponse(
        job_id=job.id,
        job_status=(job.status.value if job.status is not None else "queued"),
    )


@router.get(
    "/{semantic_model_id}/meta",
    response_model=SemanticQueryMetaResponse,
    status_code=status.HTTP_200_OK,
)
@inject
async def semantic_query_meta(
    semantic_model_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    service: SemanticQueryService = Depends(Provide[Container.semantic_query_service]),
) -> SemanticQueryMetaResponse:
    try:
        return await service.get_meta(
            semantic_model_id=semantic_model_id,
            organization_id=organization_id,
        )
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc

