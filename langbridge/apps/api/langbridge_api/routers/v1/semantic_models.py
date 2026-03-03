from typing import Literal, Optional
from uuid import UUID

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, HTTPException, status
from fastapi.responses import PlainTextResponse

from langbridge.apps.api.langbridge_api.auth.dependencies import get_current_user, get_organization, get_project
from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.apps.api.langbridge_api.ioc import Container
from langbridge.apps.api.langbridge_api.services.jobs.agentic_semantic_model_job_request_service import (
    AgenticSemanticModelJobRequestService,
)
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.jobs.agentic_semantic_model_job import (
    CreateAgenticSemanticModelJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticModelAgenticJobCreateRequest,
    SemanticModelAgenticJobCreateResponse,
    SemanticModelCatalogResponse,
    SemanticModelSelectionGenerateRequest,
    SemanticModelSelectionGenerateResponse,
    SemanticModelRecordResponse,
    SemanticModelCreateRequest,
    SemanticModelUpdateRequest,
)
from langbridge.apps.api.langbridge_api.services.semantic import SemanticModelService

router = APIRouter(prefix="/semantic-model/{organization_id}", tags=["semantic-model"])


@router.get("/generate/yaml")
@inject
async def preview_semantic_model_yaml(
    organization_id: UUID,
    connector_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    service: SemanticModelService = Depends(Provide[Container.semantic_model_service]),
) -> PlainTextResponse:
    try:
        yaml_text = await service.generate_model_yaml(connector_id)
        return PlainTextResponse(yaml_text, media_type="text/yaml")
    except Exception as exc:  # pragma: no cover - defensive
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post(
    "/generate/yaml",
    response_model=SemanticModelSelectionGenerateResponse,
    status_code=status.HTTP_200_OK,
)
@inject
async def generate_semantic_model_yaml_from_selection(
    request: SemanticModelSelectionGenerateRequest,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    service: SemanticModelService = Depends(Provide[Container.semantic_model_service]),
) -> SemanticModelSelectionGenerateResponse:
    try:
        return await service.generate_model_yaml_from_selection(
            connector_id=request.connector_id,
            selected_tables=request.selected_tables,
            selected_columns=request.selected_columns,
            include_sample_values=request.include_sample_values,
            description=request.description,
        )
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.get(
    "/catalog",
    response_model=SemanticModelCatalogResponse,
    status_code=status.HTTP_200_OK,
)
@inject
async def get_semantic_model_catalog(
    organization_id: UUID,
    connector_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    service: SemanticModelService = Depends(Provide[Container.semantic_model_service]),
) -> SemanticModelCatalogResponse:
    try:
        return await service.get_connector_catalog(connector_id=connector_id)
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc


@router.post(
    "/agentic/jobs",
    response_model=SemanticModelAgenticJobCreateResponse,
    status_code=status.HTTP_202_ACCEPTED,
)
@inject
async def create_agentic_semantic_model_job(
    request: SemanticModelAgenticJobCreateRequest,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    _proj=Depends(get_project),
    service: SemanticModelService = Depends(Provide[Container.semantic_model_service]),
    job_service: AgenticSemanticModelJobRequestService = Depends(
        Provide[Container.agentic_semantic_model_job_request_service]
    ),
) -> SemanticModelAgenticJobCreateResponse:
    if request.project_id and current_user.available_projects is not None:
        allowed_projects = {str(project_id) for project_id in current_user.available_projects}
        if str(request.project_id) not in allowed_projects:
            raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail="Forbidden")

    try:
        generated = await service.generate_model_yaml_from_selection(
            connector_id=request.connector_id,
            selected_tables=request.selected_tables,
            selected_columns=request.selected_columns,
            include_sample_values=request.include_sample_values,
            description=request.description,
        )
        draft_model = await service.create_model(
            SemanticModelCreateRequest(
                connector_id=request.connector_id,
                organization_id=organization_id,
                project_id=request.project_id,
                name=request.name,
                description=request.description,
                model_yaml=generated.yaml_text,
                auto_generate=False,
            )
        )
        job = await job_service.create_agentic_semantic_model_job_request(
            CreateAgenticSemanticModelJobRequest(
                organisation_id=organization_id,
                project_id=request.project_id,
                user_id=current_user.id,
                semantic_model_id=draft_model.id,
                connector_id=request.connector_id,
                selected_tables=request.selected_tables,
                selected_columns=request.selected_columns,
                question_prompts=request.question_prompts,
                include_sample_values=request.include_sample_values,
            )
        )
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=str(exc),
        ) from exc

    return SemanticModelAgenticJobCreateResponse(
        job_id=job.id,
        job_status=(job.status.value if job.status is not None else "queued"),
        semantic_model_id=draft_model.id,
    )


@router.post(
    "/",
    response_model=SemanticModelRecordResponse,
    status_code=status.HTTP_201_CREATED,
)
@inject
async def create_semantic_model(
    request: SemanticModelCreateRequest,
    organization_id: UUID,
    project_id: Optional[UUID] = None,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    _proj = Depends(get_project),
    service: SemanticModelService = Depends(Provide[Container.semantic_model_service]),
) -> SemanticModelRecordResponse:
    try:
        return await service.create_model(request)
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc


@router.get("/", response_model=list[SemanticModelRecordResponse])
@inject
async def list_semantic_models(
    organization_id: UUID,
    project_id: Optional[UUID] = None,
    model_kind: Literal["all", "standard", "unified"] = "all",
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    _proj = Depends(get_project),
    service: SemanticModelService = Depends(Provide[Container.semantic_model_service]),
) -> list[SemanticModelRecordResponse]:
    models = await service.list_models(
        organization_id=organization_id,
        project_id=project_id,
        model_kind=model_kind,
    )
    return models


@router.get("/{model_id}", response_model=SemanticModelRecordResponse)
@inject
async def get_semantic_model(
    model_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    service: SemanticModelService = Depends(Provide[Container.semantic_model_service]),
) -> SemanticModelRecordResponse:
    try:
        return await service.get_model(model_id=model_id, organization_id=organization_id)
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc


@router.get("/{model_id}/yaml")
@inject
async def get_semantic_model_yaml(
    model_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    service: SemanticModelService = Depends(Provide[Container.semantic_model_service]),
) -> PlainTextResponse:
    try:
        model = await service.get_model(model_id=model_id, organization_id=organization_id)
        return PlainTextResponse(model.content_yaml, media_type="text/yaml")
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc


@router.put("/{model_id}", response_model=SemanticModelRecordResponse)
@inject
async def update_semantic_model(
    model_id: UUID,
    organization_id: UUID,
    request: SemanticModelUpdateRequest,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    service: SemanticModelService = Depends(Provide[Container.semantic_model_service]),
) -> SemanticModelRecordResponse:
    try:
        return await service.update_model(
            model_id=model_id,
            organization_id=organization_id,
            request=request,
        )
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc


@router.delete("/{model_id}", status_code=status.HTTP_204_NO_CONTENT)
@inject
async def delete_semantic_model(
    model_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    service: SemanticModelService = Depends(Provide[Container.semantic_model_service]),
) -> None:
    try:
        await service.delete_model(model_id=model_id, organization_id=organization_id)
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)
        ) from exc
    return None
