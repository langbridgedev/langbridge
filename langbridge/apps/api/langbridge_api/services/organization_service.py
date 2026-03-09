from typing import Any, List
import uuid
from datetime import datetime, timezone

from langbridge.packages.common.langbridge_common.db.auth import (
    InviteStatus,
    Organization,
    OrganizationInvite,
    OrganizationRole,
    Project,
    ProjectInvite,
    ProjectRole,
    User,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
    PermissionDeniedBusinessValidationError,
    ResourceNotFound,
)
from langbridge.apps.api.langbridge_api.services.environment_service import EnvironmentService
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.organizations import (
    OrganizationInviteResponse,
    OrganizationResponse,
    ProjectInviteResponse,
    ProjectResponse,
    OrganizationEnvironmentSetting,
    OrganizationEnvironmentSettingCatalogEntry,
)
from langbridge.packages.common.langbridge_common.repositories.organization_repository import (
    OrganizationInviteRepository,
    OrganizationRepository,
    ProjectInviteRepository,
    ProjectRepository,
)
from langbridge.packages.common.langbridge_common.repositories.user_repository import UserRepository


class OrganizationService:
    """Domain logic for managing organizations, projects, and invitations."""

    def __init__(
        self,
        organization_repository: OrganizationRepository,
        project_repository: ProjectRepository,
        organization_invite_repository: OrganizationInviteRepository,
        project_invite_repository: ProjectInviteRepository,
        user_repository: UserRepository,
        environment_service:EnvironmentService
    ) -> None:
        self._organization_repository = organization_repository
        self._project_repository = project_repository
        self._organization_invite_repository = organization_invite_repository
        self._project_invite_repository = project_invite_repository
        self._user_repository = user_repository
        self._environment_service = environment_service

    async def list_user_organizations(self, user: UserResponse) -> list[OrganizationResponse]:
        db_user = await self._resolve_user(user)
        organizations = await self._organization_repository.list_for_user(db_user)
        return [self._serialize_organization(org) for org in organizations]

    async def ensure_default_workspace_for_user(
        self,
        user: User | UserResponse,
    ) -> tuple[Organization, Project]:
        db_user = await self._resolve_user(user)
        default_name = getattr(user, "username", db_user.username).strip()
        if not default_name:
            raise BusinessValidationError(
                "User username cannot be empty for workspace creation"
            )

        default_name = default_name + "'s Workspace"

        organization = await self._organization_repository.get_by_name(default_name)
        project: Project | None = None

        if organization is None:
            organization = Organization(name=default_name)
            self._organization_repository.add(organization)

        if not await self._organization_repository.is_member(organization, db_user):
            await self._organization_repository.add_member(
                organization, db_user, OrganizationRole.OWNER
            )

        project = await self._project_repository.get_by_name_within_org(
            organization.id, default_name
        )
        if project is None:
            project = Project(name=default_name, organization=organization)
            self._project_repository.add(project)

        if not await self._project_repository.is_member(project, db_user):
            await self._project_repository.add_member(
                project, db_user, ProjectRole.OWNER
            )
        return organization, project

    async def create_organization(self, owner: UserResponse, name: str) -> OrganizationResponse:
        db_owner = await self._resolve_user(owner)
        normalized_name = name.strip()
        if not normalized_name:
            raise BusinessValidationError("Organization name is required")

        existing = await self._organization_repository.get_by_name(normalized_name)
        if existing is not None:
            raise BusinessValidationError(
                "An organization with this name already exists"
            )

        organization = Organization(id=uuid.uuid4(), name=normalized_name)
        self._organization_repository.add(organization)
        await self._organization_repository.add_member(organization, db_owner)
        return self._serialize_organization(organization)

    async def create_project(
        self,
        organization_id: uuid.UUID,
        requester: UserResponse,
        name: str,
    ) -> ProjectResponse:
        db_requester = await self._resolve_user(requester)
        organization = await self._organization_repository.get_by_id(organization_id)
        if organization is None:
            raise ResourceNotFound("Organization not found")

        if not await self._organization_repository.is_member(organization, db_requester):
            raise PermissionDeniedBusinessValidationError(
                "You are not a member of this organization"
            )

        normalized_name = name.strip()
        if not normalized_name:
            raise BusinessValidationError("Project name is required")

        if await self._project_repository.get_by_name_within_org(
            organization_id, normalized_name
        ):
            raise BusinessValidationError(
                "A project with this name already exists in the organization"
            )

        project_id = uuid.uuid4()

        project = Project(id=project_id, organization_id=organization_id, name=normalized_name, organization=organization)
        self._project_repository.add(project)
        await self._project_repository.add_member(project, db_requester)
        return self._serialize_project(project)
    
    async def get_organization(self, organization_id: uuid.UUID) -> OrganizationResponse:
        organization = await self._organization_repository.get_by_id(organization_id)
        if organization is None:
            raise ResourceNotFound("Organization not found")
        return self._serialize_organization(organization)
    
    async def get_project(self, project_id: uuid.UUID) -> ProjectResponse:
        project = await self._project_repository.get_by_id(project_id)
        if project is None:
            raise ResourceNotFound("Project not found")
        return self._serialize_project(project)

    async def invite_user_to_organization(
        self,
        organization_id: uuid.UUID,
        inviter: UserResponse,
        invitee_username: str,
    ) -> OrganizationInviteResponse:
        db_inviter = await self._resolve_user(inviter)
        organization = await self._organization_repository.get_by_id(organization_id)
        if organization is None:
            raise ResourceNotFound("Organization not found")

        if not await self._organization_repository.is_member(organization, db_inviter):
            raise PermissionDeniedBusinessValidationError(
                "You are not a member of this organization"
            )

        normalized_username = invitee_username.strip()
        if not normalized_username:
            raise BusinessValidationError("Invitee username is required")

        invitee = await self._user_repository.get_by_username(normalized_username)
        if invitee is None:
            raise ResourceNotFound("No user exists with that username")

        if await self._organization_repository.is_member(organization, invitee):
            raise BusinessValidationError(
                "That user is already a member of this organization"
            )

        existing = await self._organization_invite_repository.get_by_invitee(
            organization.id, normalized_username
        )
        timestamp = datetime.now(timezone.utc)

        if existing:
            existing.status = InviteStatus.ACCEPTED
            existing.responded_at = timestamp
            await self._organization_repository.add_member(organization, invitee)
            return OrganizationInviteResponse.model_validate(existing)

        invite = OrganizationInvite(
            organization=organization,
            inviter=db_inviter,
            invitee_username=normalized_username,
            status=InviteStatus.ACCEPTED,
            responded_at=timestamp,
        )
        self._organization_invite_repository.add(invite)
        await self._organization_repository.add_member(organization, invitee)
        return OrganizationInviteResponse.model_validate(invite)

    async def invite_user_to_project(
        self,
        organization_id: uuid.UUID,
        project_id: uuid.UUID,
        inviter: UserResponse,
        invitee_username: str,
    ) -> ProjectInviteResponse:
        db_inviter = await self._resolve_user(inviter)
        organization = await self._organization_repository.get_by_id(organization_id)
        if organization is None:
            raise ResourceNotFound("Organization not found")

        project = await self._project_repository.get_by_id(project_id)
        if project is None or project.organization_id != organization.id:
            raise ResourceNotFound("Project not found in this organization")

        if not await self._organization_repository.is_member(organization, db_inviter):
            raise PermissionDeniedBusinessValidationError(
                "You are not a member of this organization"
            )

        normalized_username = invitee_username.strip()
        if not normalized_username:
            raise BusinessValidationError("Invitee username is required")

        invitee = await self._user_repository.get_by_username(normalized_username)
        if invitee is None:
            raise ResourceNotFound("No user exists with that username")

        if not await self._organization_repository.is_member(organization, invitee):
            raise BusinessValidationError(
                "User must join the organization before being added to a project"
            )

        if await self._project_repository.is_member(project, invitee):
            raise BusinessValidationError(
                "That user is already a member of this project"
            )

        existing = await self._project_invite_repository.get_by_invitee(
            project.id, invitee.id
        )
        timestamp = datetime.now(timezone.utc)

        if existing:
            existing.status = InviteStatus.ACCEPTED
            existing.responded_at = timestamp
            await self._project_repository.add_member(project, invitee)
            return ProjectInviteResponse.model_validate(existing)

        invite = ProjectInvite(
            project=project,
            inviter=db_inviter,
            invitee=invitee,
            status=InviteStatus.ACCEPTED,
            responded_at=timestamp,
        )
        self._project_invite_repository.add(invite)
        await self._project_repository.add_member(project, invitee)
        return ProjectInviteResponse.model_validate(invite)

    async def list_projects_for_organization(
        self,
        organization_id: uuid.UUID,
        user: UserResponse,
    ) -> list[ProjectResponse]:
        db_user = await self._resolve_user(user)
        organization = await self._organization_repository.get_by_id(organization_id)
        if organization is None:
            raise ResourceNotFound("Organization not found")

        if not await self._organization_repository.is_member(organization, db_user):
            raise PermissionDeniedBusinessValidationError(
                "You are not a member of this organization"
            )

        projects = await self._project_repository.list_for_organization(organization_id)
        return [self._serialize_project(project) for project in projects]

    def get_available_keys(self) -> list[str]:
        """Return a list of all available setting keys."""
        return self._environment_service.get_available_keys()

    def get_environment_settings_catalog(self) -> list[OrganizationEnvironmentSettingCatalogEntry]:
        catalog = self._environment_service.get_catalog()
        return [OrganizationEnvironmentSettingCatalogEntry(**entry) for entry in catalog]
    
    async def get_organization_environment_settings(
        self,
        organization_id: uuid.UUID,
    ) -> List[OrganizationEnvironmentSetting]:
        settings_dict = await self._environment_service.list_settings_with_metadata(organization_id)
        settings: list[OrganizationEnvironmentSetting] = []
        for key, payload in settings_dict.items():
            catalog_entry = self._environment_service.get_catalog_entry(key) or {}
            settings.append(
                OrganizationEnvironmentSetting(
                    setting_key=key,
                    setting_value=payload.get("setting_value", ""),
                    category=catalog_entry.get("category"),
                    display_name=catalog_entry.get("display_name"),
                    description=catalog_entry.get("description"),
                    scope=catalog_entry.get("scope", "organization"),
                    is_locked=bool(catalog_entry.get("is_locked", False)),
                    is_inherited=bool(catalog_entry.get("is_inherited", False)),
                    last_updated_by=payload.get("last_updated_by"),
                    last_updated_at=payload.get("last_updated_at"),
                    data_type=catalog_entry.get("data_type"),
                    options=catalog_entry.get("options"),
                    placeholder=catalog_entry.get("placeholder"),
                    multiline=catalog_entry.get("multiline"),
                    default_value=catalog_entry.get("default_value"),
                    helper_text=catalog_entry.get("helper_text"),
                    is_advanced=bool(catalog_entry.get("is_advanced", False)),
                )
            )
        settings.sort(key=lambda item: item.setting_key)
        return settings
    
    async def set_organization_environment_setting(
        self,
        organization_id: uuid.UUID,
        setting_key: str,
        setting_value: Any,
        *,
        updated_by: str | None = None,
    ) -> OrganizationEnvironmentSetting:
        await self._environment_service.set_setting(
            organization_id,
            setting_key,
            setting_value,
            updated_by=updated_by,
        )
        catalog_entry = self._environment_service.get_catalog_entry(setting_key) or {}
        now_iso = datetime.now(timezone.utc).isoformat()
        return OrganizationEnvironmentSetting(
            setting_key=setting_key,
            setting_value=str(setting_value),
            category=catalog_entry.get("category"),
            display_name=catalog_entry.get("display_name"),
            description=catalog_entry.get("description"),
            scope=catalog_entry.get("scope", "organization"),
            is_locked=bool(catalog_entry.get("is_locked", False)),
            is_inherited=bool(catalog_entry.get("is_inherited", False)),
            last_updated_by=updated_by,
            last_updated_at=now_iso,
            data_type=catalog_entry.get("data_type"),
            options=catalog_entry.get("options"),
            placeholder=catalog_entry.get("placeholder"),
            multiline=catalog_entry.get("multiline"),
            default_value=catalog_entry.get("default_value"),
            helper_text=catalog_entry.get("helper_text"),
            is_advanced=bool(catalog_entry.get("is_advanced", False)),
        )
        
    async def delete_organization_environment_setting(
        self,
        organization_id: uuid.UUID,
        setting_key: str,
    ) -> None:
        await self._environment_service.delete_setting(
            organization_id,
            setting_key,
        )
    
    async def _resolve_user(self, user: User | UserResponse) -> User:
        if isinstance(user, User):
            return user
        resolved = await self._user_repository.get_by_username(user.username)
        if not resolved:
            raise BusinessValidationError("User not found")
        return resolved

    def _serialize_project(self, project: Project) -> ProjectResponse:
        return ProjectResponse.model_validate(project)

    def _serialize_organization(self, organization: Organization) -> OrganizationResponse:
        projects = [self._serialize_project(project) for project in organization.projects]
        member_links = list(organization.user_links or [])
        return OrganizationResponse(
            id=organization.id,
            name=organization.name,
            member_count=len(member_links),
            projects=projects,
        )
