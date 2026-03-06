from datetime import datetime, timezone
from typing import TYPE_CHECKING
from uuid import UUID
import uuid

from langbridge.packages.common.langbridge_common.contracts.dashboards import (
    DashboardDataFormat,
    DashboardCreateRequest,
    DashboardRefreshMode,
    DashboardResponse,
    DashboardSnapshotResponse,
    DashboardSnapshotUpsertRequest,
    DashboardUpdateRequest,
)
from langbridge.packages.common.langbridge_common.db.bi import BIDashboard
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.interfaces.dashboard_snapshot_storage import (
    IDashboardSnapshotStorage,
)
from langbridge.packages.common.langbridge_common.repositories.dashboard_repository import (
    DashboardRepository,
)
from langbridge.packages.common.langbridge_common.repositories.organization_repository import (
    OrganizationRepository,
    ProjectRepository,
)
from langbridge.packages.common.langbridge_common.utils.lineage import LineageNodeType

if TYPE_CHECKING:
    from langbridge.apps.api.langbridge_api.services.lineage_service import LineageService
    from langbridge.apps.api.langbridge_api.services.semantic.semantic_model_service import (
        SemanticModelService,
    )


class DashboardService:
    def __init__(
        self,
        repository: DashboardRepository,
        organization_repository: OrganizationRepository,
        project_repository: ProjectRepository,
        semantic_model_service: "SemanticModelService",
        snapshot_storage: IDashboardSnapshotStorage,
        lineage_service: "LineageService | None" = None,
    ) -> None:
        self._repository = repository
        self._organization_repository = organization_repository
        self._project_repository = project_repository
        self._semantic_model_service = semantic_model_service
        self._snapshot_storage = snapshot_storage
        self._lineage_service = lineage_service

    async def list_dashboards(
        self,
        organization_id: UUID,
        project_id: UUID | None = None,
    ) -> list[DashboardResponse]:
        dashboards = await self._repository.list_for_scope(
            organization_id=organization_id,
            project_id=project_id,
        )
        return [DashboardResponse.model_validate(entry) for entry in dashboards]

    async def get_dashboard(
        self,
        dashboard_id: UUID,
        organization_id: UUID,
    ) -> DashboardResponse:
        dashboard = await self._get_dashboard_entity(
            dashboard_id=dashboard_id,
            organization_id=organization_id,
        )
        return DashboardResponse.model_validate(dashboard)

    async def create_dashboard(
        self,
        organization_id: UUID,
        created_by: UUID,
        request: DashboardCreateRequest,
    ) -> DashboardResponse:
        await self._validate_organization_scope(organization_id=organization_id)
        await self._validate_project_scope(
            organization_id=organization_id,
            project_id=request.project_id,
        )
        await self._validate_semantic_model_scope(
            organization_id=organization_id,
            project_id=request.project_id,
            semantic_model_id=request.semantic_model_id,
        )

        now = datetime.now(timezone.utc)
        bi_dashboard_id = uuid.uuid4()
        entry = BIDashboard(
            id=bi_dashboard_id,
            organization_id=organization_id,
            project_id=request.project_id,
            semantic_model_id=request.semantic_model_id,
            name=request.name.strip(),
            description=request.description.strip() if request.description else None,
            refresh_mode=request.refresh_mode.value,
            data_snapshot_format=DashboardDataFormat.json.value,
            data_snapshot_reference=None,
            last_refreshed_at=None,
            global_filters=request.global_filters,
            widgets=request.widgets,
            created_by=created_by,
            created_at=now,
            updated_at=now,
        )

        self._repository.add(entry)
        if self._lineage_service is not None:
            await self._lineage_service.register_dashboard_lineage(dashboard=entry)
        return DashboardResponse.model_validate(entry)

    async def update_dashboard(
        self,
        dashboard_id: UUID,
        organization_id: UUID,
        request: DashboardUpdateRequest,
    ) -> DashboardResponse:
        dashboard = await self._get_dashboard_entity(
            dashboard_id=dashboard_id,
            organization_id=organization_id,
        )
        await self._validate_organization_scope(organization_id=organization_id)

        if "project_id" in request.model_fields_set:
            await self._validate_project_scope(
                organization_id=organization_id,
                project_id=request.project_id,
            )
            dashboard.project_id = request.project_id

        if "semantic_model_id" in request.model_fields_set:
            if request.semantic_model_id is None:
                raise BusinessValidationError("semantic_model_id is required.")
            dashboard.semantic_model_id = request.semantic_model_id

        await self._validate_semantic_model_scope(
            organization_id=organization_id,
            project_id=dashboard.project_id,
            semantic_model_id=dashboard.semantic_model_id,
        )

        if "name" in request.model_fields_set:
            if request.name is None:
                raise BusinessValidationError("Dashboard name is required.")
            dashboard.name = request.name.strip()

        if "description" in request.model_fields_set:
            dashboard.description = request.description.strip() if request.description else None

        if "refresh_mode" in request.model_fields_set:
            if request.refresh_mode is None:
                dashboard.refresh_mode = DashboardRefreshMode.manual.value
            else:
                dashboard.refresh_mode = request.refresh_mode.value

        if "global_filters" in request.model_fields_set:
            dashboard.global_filters = request.global_filters or []

        if "widgets" in request.model_fields_set:
            dashboard.widgets = request.widgets or []

        dashboard.updated_at = datetime.now(timezone.utc)
        if self._lineage_service is not None:
            await self._lineage_service.register_dashboard_lineage(dashboard=dashboard)
        return DashboardResponse.model_validate(dashboard)

    async def get_dashboard_snapshot(
        self,
        *,
        dashboard_id: UUID,
        organization_id: UUID,
    ) -> DashboardSnapshotResponse | None:
        dashboard = await self._get_dashboard_entity(
            dashboard_id=dashboard_id,
            organization_id=organization_id,
        )
        if not dashboard.data_snapshot_reference:
            return None

        snapshot_data = await self._snapshot_storage.read_snapshot(
            organization_id=organization_id,
            dashboard_id=dashboard.id,
            snapshot_reference=dashboard.data_snapshot_reference,
        )
        if snapshot_data is None:
            return None

        captured_at = dashboard.last_refreshed_at or dashboard.updated_at
        return DashboardSnapshotResponse(
            dashboard_id=dashboard.id,
            snapshot_format=DashboardDataFormat(dashboard.data_snapshot_format),
            captured_at=captured_at,
            data=snapshot_data,
        )

    async def upsert_dashboard_snapshot(
        self,
        *,
        dashboard_id: UUID,
        organization_id: UUID,
        request: DashboardSnapshotUpsertRequest,
    ) -> DashboardSnapshotResponse:
        dashboard = await self._get_dashboard_entity(
            dashboard_id=dashboard_id,
            organization_id=organization_id,
        )
        if not request.data:
            raise BusinessValidationError("Snapshot data is required.")

        reference = await self._snapshot_storage.write_snapshot(
            organization_id=organization_id,
            dashboard_id=dashboard.id,
            data=request.data,
        )
        captured_at = request.captured_at or datetime.now(timezone.utc)

        dashboard.data_snapshot_reference = reference
        dashboard.data_snapshot_format = DashboardDataFormat.json.value
        dashboard.last_refreshed_at = captured_at
        dashboard.updated_at = datetime.now(timezone.utc)

        return DashboardSnapshotResponse(
            dashboard_id=dashboard.id,
            snapshot_format=DashboardDataFormat(dashboard.data_snapshot_format),
            captured_at=captured_at,
            data=request.data,
        )

    async def delete_dashboard(
        self,
        dashboard_id: UUID,
        organization_id: UUID,
    ) -> None:
        dashboard = await self._get_dashboard_entity(
            dashboard_id=dashboard_id,
            organization_id=organization_id,
        )
        if dashboard.data_snapshot_reference:
            await self._snapshot_storage.delete_snapshot(
                organization_id=organization_id,
                dashboard_id=dashboard.id,
                snapshot_reference=dashboard.data_snapshot_reference,
            )
        if self._lineage_service is not None:
            await self._lineage_service.delete_node_lineage(
                workspace_id=organization_id,
                node_type=LineageNodeType.DASHBOARD,
                node_id=str(dashboard.id),
            )
        await self._repository.delete(dashboard)

    async def _get_dashboard_entity(
        self,
        dashboard_id: UUID,
        organization_id: UUID,
    ) -> BIDashboard:
        dashboard = await self._repository.get_for_scope(
            dashboard_id=dashboard_id,
            organization_id=organization_id,
        )
        if not dashboard:
            raise BusinessValidationError("Dashboard not found.")
        return dashboard

    async def _validate_organization_scope(self, organization_id: UUID) -> None:
        organization = await self._organization_repository.get_by_id(organization_id)
        if not organization:
            raise BusinessValidationError("Organization not found.")

    async def _validate_project_scope(
        self,
        organization_id: UUID,
        project_id: UUID | None,
    ) -> None:
        if project_id is None:
            return

        project = await self._project_repository.get_by_id(project_id)
        if not project:
            raise BusinessValidationError("Project not found.")
        if project.organization_id != organization_id:
            raise BusinessValidationError(
                "Project does not belong to the specified organization.",
            )

    async def _validate_semantic_model_scope(
        self,
        organization_id: UUID,
        project_id: UUID | None,
        semantic_model_id: UUID,
    ) -> None:
        semantic_model = await self._semantic_model_service.get_model(
            model_id=semantic_model_id,
            organization_id=organization_id,
        )
        if semantic_model.project_id is None:
            return
        if project_id is None:
            raise BusinessValidationError(
                "Project-scoped semantic models require a project-scoped dashboard.",
            )
        if semantic_model.project_id != project_id:
            raise BusinessValidationError(
                "Dashboard project_id must match semantic_model project_id.",
            )
