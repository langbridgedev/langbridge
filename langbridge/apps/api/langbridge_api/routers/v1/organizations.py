import uuid
from typing import List

from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, status

from langbridge.apps.api.langbridge_api.auth.dependencies import get_current_user, has_organization_access
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.apps.api.langbridge_api.ioc import Container
from langbridge.packages.common.langbridge_common.contracts.organizations import (
    InviteUserRequest,
    OrganizationCreateRequest,
    OrganizationInviteResponse,
    OrganizationResponse,
    ProjectCreateRequest,
    ProjectInviteResponse,
    ProjectResponse,
    OrganizationEnvironmentSetting,
    OrganizationEnvironmentSettingCatalogEntry,
)
from langbridge.apps.api.langbridge_api.services.organization_service import OrganizationService

router = APIRouter(prefix="/organizations", tags=["organizations"])


@router.get("", response_model=List[OrganizationResponse])
@inject
async def list_organizations(
    current_user: UserResponse = Depends(get_current_user),
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> List[OrganizationResponse]:
    organizations = await organization_service.list_user_organizations(current_user)
    return organizations


@router.post("", response_model=OrganizationResponse, status_code=status.HTTP_201_CREATED)
@inject
async def create_organization(
    payload: OrganizationCreateRequest,
    current_user: UserResponse = Depends(get_current_user),
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> OrganizationResponse:
    organization = await organization_service.create_organization(
        current_user,
        payload.name,
    )
    return organization


@router.get("/{organization_id}/projects", response_model=List[ProjectResponse])
@inject
async def list_projects(
    organization_id: uuid.UUID,
    current_user: UserResponse = Depends(get_current_user),
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> List[ProjectResponse]:
    projects = await organization_service.list_projects_for_organization(
        organization_id,
        current_user,
    )
    return projects


@router.post(
    "/{organization_id}/projects",
    response_model=ProjectResponse,
    status_code=status.HTTP_201_CREATED,
)
@inject
async def create_project(
    organization_id: uuid.UUID,
    payload: ProjectCreateRequest,
    current_user: UserResponse = Depends(get_current_user),
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> ProjectResponse:
    project = await organization_service.create_project(
        organization_id,
        current_user,
        payload.name,
    )
    return project


@router.post(
    "/{organization_id}/invites",
    response_model=OrganizationInviteResponse,
    status_code=status.HTTP_201_CREATED,
)
@inject
async def invite_to_organization(
    organization_id: uuid.UUID,
    payload: InviteUserRequest,
    current_user: UserResponse = Depends(get_current_user),
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> OrganizationInviteResponse:
    invite = await organization_service.invite_user_to_organization(
        organization_id,
        current_user,
        payload.username,
    )
    return invite


@router.post(
    "/{organization_id}/projects/{project_id}/invites",
    response_model=ProjectInviteResponse,
    status_code=status.HTTP_201_CREATED,
)
@inject
async def invite_to_project(
    organization_id: uuid.UUID,
    project_id: uuid.UUID,
    payload: InviteUserRequest,
    current_user: UserResponse = Depends(get_current_user),
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> ProjectInviteResponse:
    invite = await organization_service.invite_user_to_project(
        organization_id,
        project_id,
        current_user,
        payload.username,
    )
    return invite

@router.get(
    "/environment/keys",
    response_model=List[str],
    status_code=status.HTTP_200_OK,
)
@inject
async def get_environment_setting_keys(
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> List[str]:
    keys = organization_service.get_available_keys()
    return keys


@router.get(
    "/environment/catalog",
    response_model=List[OrganizationEnvironmentSettingCatalogEntry],
    status_code=status.HTTP_200_OK,
)
@inject
async def get_environment_settings_catalog(
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> List[OrganizationEnvironmentSettingCatalogEntry]:
    return organization_service.get_environment_settings_catalog()


@router.get(
    "/{organization_id}/environment",
    response_model=List[OrganizationEnvironmentSetting],
    status_code=status.HTTP_200_OK,
)
@inject
async def get_organization_environment_settings(
    organization_id: uuid.UUID,
    current_user: UserResponse = Depends(has_organization_access),
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> List[OrganizationEnvironmentSetting]:
    settings = await organization_service.get_organization_environment_settings(
        organization_id,
    )
    return settings

@router.post(
    "/{organization_id}/environment/{setting_key}",
    response_model=OrganizationEnvironmentSetting,
    status_code=status.HTTP_201_CREATED,
)
@inject
async def set_organization_environment_setting(
    organization_id: uuid.UUID,
    setting_key: str,
    payload: OrganizationEnvironmentSetting,
    current_user: UserResponse = Depends(has_organization_access),
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> OrganizationEnvironmentSetting:
    setting = await organization_service.set_organization_environment_setting(
        organization_id,
        setting_key,
        payload.setting_value,
        updated_by=current_user.username,
    )
    return setting

@router.delete(
    "/{organization_id}/environment/{setting_key}",
    status_code=status.HTTP_204_NO_CONTENT,
)
@inject
async def delete_organization_environment_setting(
    organization_id: uuid.UUID,
    setting_key: str,
    current_user: UserResponse = Depends(has_organization_access),
    organization_service: OrganizationService = Depends(
        Provide[Container.organization_service]
    ),
) -> None:
    await organization_service.delete_organization_environment_setting(
        organization_id,
        setting_key,
    )
    return None
