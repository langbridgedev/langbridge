import uuid

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, HTTPException, Query, status

from langbridge.apps.api.langbridge_api.auth.dependencies import get_current_user, get_organization
from langbridge.apps.api.langbridge_api.ioc import Container
from langbridge.apps.api.langbridge_api.services.jobs.job_service import JobService
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.jobs.agent_job import (
    AgentJobCancelResponse,
    AgentJobStateResponse,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
    PermissionDeniedBusinessValidationError,
    ResourceNotFound,
)


router = APIRouter(prefix="/jobs/{organization_id}", tags=["jobs"])


@router.get("/{job_id}", response_model=AgentJobStateResponse)
@inject
async def get_job_state(
    job_id: uuid.UUID,
    organization_id: uuid.UUID,
    include_internal: bool = Query(
        False,
        description="Include internal reasoning/audit events and thinking breakdown.",
    ),
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    job_service: JobService = Depends(Provide[Container.job_service]),
) -> AgentJobStateResponse:
    try:
        return await job_service.get_agent_job_state(
            job_id=job_id,
            organization_id=organization_id,
            current_user=current_user,
            include_internal=include_internal,
        )
    except ResourceNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except PermissionDeniedBusinessValidationError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc


@router.post("/{job_id}/cancel", response_model=AgentJobCancelResponse)
@inject
async def cancel_job(
    job_id: uuid.UUID,
    organization_id: uuid.UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org=Depends(get_organization),
    job_service: JobService = Depends(Provide[Container.job_service]),
) -> AgentJobCancelResponse:
    try:
        return await job_service.cancel_agent_job(
            job_id=job_id,
            organization_id=organization_id,
            current_user=current_user,
        )
    except ResourceNotFound as exc:
        raise HTTPException(status_code=status.HTTP_404_NOT_FOUND, detail=str(exc)) from exc
    except PermissionDeniedBusinessValidationError as exc:
        raise HTTPException(status_code=status.HTTP_403_FORBIDDEN, detail=str(exc)) from exc
    except BusinessValidationError as exc:
        raise HTTPException(status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)) from exc
