from __future__ import annotations

import hashlib
import json
import uuid
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

import yaml

from langbridge.apps.api.langbridge_api.services.connector_service import ConnectorService
from langbridge.apps.api.langbridge_api.services.jobs.dataset_job_request_service import (
    DatasetJobRequestService,
)
from langbridge.apps.api.langbridge_api.services.request_context_provider import (
    RequestContextProvider,
)
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.datasets import (
    DatasetBulkCreateRequest,
    DatasetBulkCreateStartResponse,
    DatasetCatalogItem,
    DatasetCatalogResponse,
    DatasetColumnRequest,
    DatasetColumnResponse,
    DatasetCsvIngestResponse,
    DatasetCreateRequest,
    DatasetEnsureRequest,
    DatasetEnsureResponse,
    DatasetListResponse,
    DatasetLineageEdgeResponse,
    DatasetLineageNodeResponse,
    DatasetLineageResponse,
    DatasetLineageNodeType,
    DatasetLineageEdgeType,
    DatasetSelectionColumnRequest,
    DatasetPolicyDefaultsRequest,
    DatasetPolicyRequest,
    DatasetPolicyResponse,
    DatasetPreviewColumn,
    DatasetPreviewRequest,
    DatasetPreviewResponse,
    DatasetProfileRequest,
    DatasetProfileResponse,
    DatasetResponse,
    DatasetRestoreRequest,
    DatasetStatsResponse,
    DatasetStatus,
    DatasetType,
    DatasetUpdateRequest,
    DatasetImpactItemResponse,
    DatasetImpactResponse,
    DatasetUsageResponse,
    DatasetVersionDiffResponse,
    DatasetVersionFieldDiff,
    DatasetVersionListResponse,
    DatasetVersionResponse,
    DatasetVersionSummaryResponse,
    DatasetSchemaColumnDiff,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.dataset_job import (
    CreateDatasetBulkCreateJobRequest,
    CreateDatasetCsvIngestJobRequest,
    CreateDatasetPreviewJobRequest,
    CreateDatasetProfileJobRequest,
)
from langbridge.packages.common.langbridge_common.contracts.jobs.type import JobType
from langbridge.packages.common.langbridge_common.db.dataset import (
    DatasetColumnRecord,
    DatasetPolicyRecord,
    DatasetRecord,
    DatasetRevisionRecord,
)
from langbridge.packages.common.langbridge_common.db.job import JobRecord, JobStatus
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
    PermissionDeniedBusinessValidationError,
    ResourceNotFound,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetColumnRepository,
    DatasetPolicyRepository,
    DatasetRepository,
    DatasetRevisionRepository,
)
from langbridge.packages.common.langbridge_common.repositories.job_repository import JobRepository
from langbridge.packages.common.langbridge_common.repositories.organization_repository import (
    OrganizationRepository,
)
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import (
    SemanticModelRepository,
)
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlWorkspacePolicyRepository,
)
from langbridge.packages.common.langbridge_common.repositories.user_repository import (
    UserRepository,
)
from langbridge.packages.common.langbridge_common.utils.lineage import (
    LineageNodeType,
    stable_payload_hash,
)
from langbridge.packages.common.langbridge_common.utils.sql import (
    enforce_preview_limit,
    enforce_read_only_sql,
)
from langbridge.packages.common.langbridge_common.utils.storage_uri import (
    path_to_storage_uri,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorRuntimeType
from langbridge.apps.api.langbridge_api.services.lineage_service import LineageService

_FORBIDDEN_SECRET_KEYS = {
    "password",
    "passwd",
    "pwd",
    "secret",
    "token",
    "api_key",
    "apikey",
    "private_key",
    "client_secret",
}
_DATASET_AUTO_GENERATED_TAG = "auto-generated"


class DatasetService:
    def __init__(
        self,
        *,
        dataset_repository: DatasetRepository,
        dataset_column_repository: DatasetColumnRepository,
        dataset_policy_repository: DatasetPolicyRepository,
        dataset_revision_repository: DatasetRevisionRepository,
        connector_repository: ConnectorRepository,
        semantic_model_repository: SemanticModelRepository,
        sql_workspace_policy_repository: SqlWorkspacePolicyRepository,
        organization_repository: OrganizationRepository,
        user_repository: UserRepository,
        connector_service: ConnectorService,
        dataset_job_request_service: DatasetJobRequestService,
        job_repository: JobRepository,
        request_context_provider: RequestContextProvider,
        lineage_service: LineageService | None = None,
    ) -> None:
        self._dataset_repository = dataset_repository
        self._dataset_column_repository = dataset_column_repository
        self._dataset_policy_repository = dataset_policy_repository
        self._dataset_revision_repository = dataset_revision_repository
        self._connector_repository = connector_repository
        self._semantic_model_repository = semantic_model_repository
        self._sql_workspace_policy_repository = sql_workspace_policy_repository
        self._organization_repository = organization_repository
        self._user_repository = user_repository
        self._connector_service = connector_service
        self._dataset_job_request_service = dataset_job_request_service
        self._job_repository = job_repository
        self._request_context_provider = request_context_provider
        self._lineage_service = lineage_service
        self._policy_cache_attr = "_dataset_policy_cache"

    async def list_datasets(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        search: str | None,
        tags: list[str] | None,
        dataset_types: list[str] | None,
        current_user: UserResponse,
    ) -> DatasetListResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        items = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
            project_id=project_id,
            search=search,
            tags=tags or [],
            dataset_types=dataset_types or [],
        )
        results: list[DatasetResponse] = []
        for dataset in items:
            results.append(await self._to_dataset_response(dataset))
        return DatasetListResponse(items=results, total=len(results))

    async def create_dataset(
        self,
        *,
        request: DatasetCreateRequest,
        current_user: UserResponse,
    ) -> DatasetResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        await self._assert_workspace_admin(request.workspace_id, current_user)
        self._assert_no_secret_payload(request.federated_plan, context="federated_plan")
        self._assert_no_secret_payload(request.file_config, context="file_config")
        await self._validate_dataset_feature_flags(request.dataset_type)
        await self._validate_connection_scope(
            workspace_id=request.workspace_id,
            connection_id=request.connection_id,
        )

        now = datetime.now(timezone.utc)
        dataset_id = uuid.uuid4()
        dataset = DatasetRecord(
            id=dataset_id,
            workspace_id=request.workspace_id,
            project_id=request.project_id,
            connection_id=request.connection_id,
            created_by=current_user.id,
            updated_by=current_user.id,
            name=request.name.strip(),
            description=(request.description.strip() if request.description else None),
            tags_json=[tag.strip() for tag in request.tags if tag and tag.strip()],
            dataset_type=request.dataset_type.value,
            dialect=(request.dialect.strip().lower() if request.dialect else None),
            storage_uri=(request.storage_uri.strip() if request.storage_uri else None),
            catalog_name=request.catalog_name,
            schema_name=request.schema_name,
            table_name=request.table_name,
            sql_text=request.sql_text,
            referenced_dataset_ids_json=[str(dataset_id) for dataset_id in request.referenced_dataset_ids],
            federated_plan_json=request.federated_plan,
            file_config_json=request.file_config,
            status=request.status.value,
            revision_id=None,
            row_count_estimate=None,
            bytes_estimate=None,
            last_profiled_at=None,
            created_at=now,
            updated_at=now,
        )
        self._dataset_repository.add(dataset)

        inferred_columns = await self._resolve_create_columns(dataset=dataset, request=request)
        await self._replace_columns(
            dataset=dataset,
            workspace_id=request.workspace_id,
            columns=inferred_columns,
        )
        policy = await self._upsert_policy(dataset=dataset, policy=request.policy)
        revision_id = await self._create_revision(
            dataset=dataset,
            policy=policy,
            created_by=current_user.id,
            change_summary=request.change_summary or "Initial dataset revision.",
        )
        dataset.revision_id = revision_id
        dataset.updated_at = datetime.now(timezone.utc)
        await self._register_dataset_lineage(dataset)
        return await self._to_dataset_response(dataset)

    async def upload_csv_dataset(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        name: str,
        filename: str,
        content: bytes,
        description: str | None,
        tags: list[str] | None,
        current_user: UserResponse,
    ) -> DatasetCsvIngestResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        await self._assert_workspace_admin(workspace_id, current_user)
        await self._validate_dataset_feature_flags(DatasetType.FILE)

        safe_filename = Path(filename or "upload.csv").name or "upload.csv"
        upload_root = Path(settings.DATASET_FILE_LOCAL_DIR) / "uploads" / str(workspace_id)
        upload_root.mkdir(parents=True, exist_ok=True)
        file_token = uuid.uuid4()
        upload_path = upload_root / f"{file_token}_{safe_filename}"
        upload_path.write_bytes(content)
        storage_uri = path_to_storage_uri(upload_path)

        dataset = await self.create_dataset(
            request=DatasetCreateRequest(
                workspace_id=workspace_id,
                project_id=project_id,
                name=name.strip() or safe_filename,
                description=description,
                tags=list(tags or []),
                dataset_type=DatasetType.FILE,
                dialect="duckdb",
                storage_uri=storage_uri,
                file_config={
                    "format": "csv",
                    "filename": safe_filename,
                    "source_storage_uri": storage_uri,
                },
                status=DatasetStatus.DRAFT,
            ),
            current_user=current_user,
        )

        job = await self._dataset_job_request_service.create_csv_ingest_job(
            CreateDatasetCsvIngestJobRequest(
                dataset_id=dataset.id,
                workspace_id=workspace_id,
                project_id=project_id,
                user_id=current_user.id,
                storage_uri=storage_uri,
                correlation_id=self._request_context_provider.correlation_id,
            )
        )
        return DatasetCsvIngestResponse(
            dataset_id=dataset.id,
            job_id=job.id,
            job_status=job.status.value,
            storage_uri=storage_uri,
        )

    async def ensure_dataset(
        self,
        *,
        request: DatasetEnsureRequest,
        current_user: UserResponse,
    ) -> DatasetEnsureResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        await self._validate_connection_scope(
            workspace_id=request.workspace_id,
            connection_id=request.connection_id,
        )
        columns = self._normalize_selection_columns(request.columns)
        existing = await self._find_existing_table_dataset(
            workspace_id=request.workspace_id,
            project_id=request.project_id,
            connection_id=request.connection_id,
            schema_name=request.schema,
            table_name=request.table,
            selected_columns=columns,
        )
        if existing is not None:
            return DatasetEnsureResponse(
                dataset_id=existing.id,
                created=False,
                name=existing.name,
            )

        await self._assert_workspace_admin(request.workspace_id, current_user)
        created = await self._create_dataset_from_table_selection(
            workspace_id=request.workspace_id,
            project_id=request.project_id,
            connection_id=request.connection_id,
            schema_name=request.schema,
            table_name=request.table,
            columns=columns,
            naming_template=request.naming_template,
            requested_name=request.name,
            policy_defaults=request.policy_defaults,
            tags=request.tags,
            current_user=current_user,
        )
        return DatasetEnsureResponse(
            dataset_id=created.id,
            created=True,
            name=created.name,
        )

    async def start_bulk_create(
        self,
        *,
        request: DatasetBulkCreateRequest,
        current_user: UserResponse,
    ) -> DatasetBulkCreateStartResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        await self._assert_workspace_admin(request.workspace_id, current_user)
        await self._validate_connection_scope(
            workspace_id=request.workspace_id,
            connection_id=request.connection_id,
        )

        normalized_selections = [
            selection.model_copy(update={"columns": self._normalize_selection_columns(selection.columns)})
            for selection in request.selections
        ]
        dedupe_key_set: set[str] = set()
        deduped_selections = []
        for selection in normalized_selections:
            key = self._selection_signature(
                schema_name=selection.schema,
                table_name=selection.table,
                selected_columns=[column.name for column in selection.columns],
            )
            if key in dedupe_key_set:
                continue
            dedupe_key_set.add(key)
            deduped_selections.append(selection)

        if len(deduped_selections) == 0:
            raise BusinessValidationError("No unique selections remain after de-duplication.")

        job = await self._dataset_job_request_service.create_bulk_create_job(
            CreateDatasetBulkCreateJobRequest(
                workspace_id=request.workspace_id,
                project_id=request.project_id,
                user_id=current_user.id,
                connection_id=request.connection_id,
                selections=deduped_selections,
                naming_template=request.naming_template,
                policy_defaults=request.policy_defaults,
                tags=list(request.tags or []),
                profile_after_create=request.profile_after_create,
                correlation_id=self._request_context_provider.correlation_id,
            )
        )
        return DatasetBulkCreateStartResponse(
            job_id=job.id,
            job_status=job.status.value,
        )

    async def get_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> DatasetResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=workspace_id)
        return await self._to_dataset_response(dataset)

    async def list_dataset_versions(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> DatasetVersionListResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=workspace_id)
        revisions = await self._dataset_revision_repository.list_for_dataset(dataset_id=dataset.id, limit=200)
        return DatasetVersionListResponse(
            items=[self._to_dataset_version_summary(dataset, revision) for revision in revisions]
        )

    async def get_dataset_version(
        self,
        *,
        dataset_id: uuid.UUID,
        revision_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> DatasetVersionResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=workspace_id)
        revision = await self._get_dataset_revision(dataset_id=dataset.id, revision_id=revision_id)
        return self._to_dataset_version_response(dataset, revision)

    async def diff_dataset_versions(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        from_revision_id: uuid.UUID,
        to_revision_id: uuid.UUID,
        current_user: UserResponse,
    ) -> DatasetVersionDiffResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=workspace_id)
        from_revision = await self._get_dataset_revision(dataset_id=dataset.id, revision_id=from_revision_id)
        to_revision = await self._get_dataset_revision(dataset_id=dataset.id, revision_id=to_revision_id)
        return self._diff_dataset_revisions(dataset, from_revision, to_revision)

    async def restore_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        request: DatasetRestoreRequest,
        current_user: UserResponse,
    ) -> DatasetResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        await self._assert_workspace_admin(request.workspace_id, current_user)
        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=request.workspace_id)
        revision = await self._get_dataset_revision(dataset_id=dataset.id, revision_id=request.revision_id)
        snapshot = self._build_revision_snapshot_payload(revision)

        definition = dict(snapshot["definition"])
        schema_snapshot = list(snapshot["schema"])
        policy_snapshot = dict(snapshot["policy"])

        dataset.project_id = self._parse_optional_uuid(definition.get("project_id"))
        dataset.connection_id = self._parse_optional_uuid(definition.get("connection_id"))
        dataset.name = str(definition.get("name") or dataset.name)
        dataset.description = definition.get("description")
        dataset.tags_json = list(definition.get("tags") or [])
        dataset.dialect = definition.get("dialect")
        dataset.catalog_name = definition.get("catalog_name")
        dataset.schema_name = definition.get("schema_name")
        dataset.table_name = definition.get("table_name")
        dataset.storage_uri = definition.get("storage_uri")
        dataset.sql_text = definition.get("sql_text")
        dataset.referenced_dataset_ids_json = list(definition.get("referenced_dataset_ids") or [])
        dataset.federated_plan_json = definition.get("federated_plan")
        dataset.file_config_json = definition.get("file_config")
        if definition.get("status"):
            dataset.status = str(definition["status"])

        await self._replace_columns(
            dataset=dataset,
            workspace_id=request.workspace_id,
            columns=[self._column_request_from_snapshot(item) for item in schema_snapshot],
        )
        policy = await self._upsert_policy(
            dataset=dataset,
            policy=DatasetPolicyRequest(
                max_rows_preview=policy_snapshot.get("max_rows_preview"),
                max_export_rows=policy_snapshot.get("max_export_rows"),
                redaction_rules=policy_snapshot.get("redaction_rules"),
                row_filters=policy_snapshot.get("row_filters"),
                allow_dml=policy_snapshot.get("allow_dml"),
            ),
        )
        revision_id = await self._create_revision(
            dataset=dataset,
            policy=policy,
            created_by=current_user.id,
            change_summary=(
                request.change_summary
                or f"Restored dataset from revision {revision.revision_number}."
            ),
        )
        dataset.revision_id = revision_id
        dataset.updated_by = current_user.id
        dataset.updated_at = datetime.now(timezone.utc)
        await self._register_dataset_lineage(dataset)
        return await self._to_dataset_response(dataset)

    async def update_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        request: DatasetUpdateRequest,
        current_user: UserResponse,
    ) -> DatasetResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        await self._assert_workspace_admin(request.workspace_id, current_user)
        self._assert_no_secret_payload(request.federated_plan, context="federated_plan")
        self._assert_no_secret_payload(request.file_config, context="file_config")
        await self._validate_connection_scope(
            workspace_id=request.workspace_id,
            connection_id=request.connection_id,
        )

        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=request.workspace_id)
        should_refresh_columns = False

        if request.name is not None:
            dataset.name = request.name.strip()
        if request.description is not None:
            dataset.description = request.description.strip() or None
        if request.tags is not None:
            dataset.tags_json = [tag.strip() for tag in request.tags if tag and tag.strip()]
        if request.project_id is not None or "project_id" in request.model_fields_set:
            dataset.project_id = request.project_id
        if request.connection_id is not None or "connection_id" in request.model_fields_set:
            dataset.connection_id = request.connection_id
            should_refresh_columns = True
        if request.dialect is not None:
            dataset.dialect = request.dialect.strip().lower() if request.dialect.strip() else None
        if request.storage_uri is not None or "storage_uri" in request.model_fields_set:
            dataset.storage_uri = request.storage_uri.strip() if request.storage_uri else None
            should_refresh_columns = True
        if request.catalog_name is not None:
            dataset.catalog_name = request.catalog_name
            should_refresh_columns = True
        if request.schema_name is not None:
            dataset.schema_name = request.schema_name
            should_refresh_columns = True
        if request.table_name is not None:
            dataset.table_name = request.table_name
            should_refresh_columns = True
        if request.sql_text is not None:
            dataset.sql_text = request.sql_text
            should_refresh_columns = True
        if request.referenced_dataset_ids is not None:
            dataset.referenced_dataset_ids_json = [str(item) for item in request.referenced_dataset_ids]
            should_refresh_columns = True
        if request.federated_plan is not None or "federated_plan" in request.model_fields_set:
            dataset.federated_plan_json = request.federated_plan
            should_refresh_columns = True
        if request.file_config is not None or "file_config" in request.model_fields_set:
            dataset.file_config_json = request.file_config
            should_refresh_columns = True
        if request.status is not None:
            dataset.status = request.status.value

        if request.columns is not None:
            await self._replace_columns(
                dataset=dataset,
                workspace_id=request.workspace_id,
                columns=request.columns,
            )
        elif should_refresh_columns:
            refreshed_columns = await self._resolve_columns_for_dataset(dataset)
            if refreshed_columns:
                await self._replace_columns(
                    dataset=dataset,
                    workspace_id=request.workspace_id,
                    columns=refreshed_columns,
                )

        policy = await self._upsert_policy(dataset=dataset, policy=request.policy)
        revision_id = await self._create_revision(
            dataset=dataset,
            policy=policy,
            created_by=current_user.id,
            change_summary=request.change_summary or "Dataset updated.",
        )
        dataset.revision_id = revision_id
        dataset.updated_by = current_user.id
        dataset.updated_at = datetime.now(timezone.utc)
        await self._register_dataset_lineage(dataset)
        return await self._to_dataset_response(dataset)

    async def delete_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> None:
        await self._assert_workspace_access(workspace_id, current_user)
        await self._assert_workspace_admin(workspace_id, current_user)
        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=workspace_id)
        if self._lineage_service is not None:
            await self._lineage_service.delete_node_lineage(
                workspace_id=workspace_id,
                node_type=LineageNodeType.DATASET,
                node_id=str(dataset.id),
            )
        await self._dataset_repository.delete(dataset)

    async def get_catalog(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        current_user: UserResponse,
    ) -> DatasetCatalogResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        datasets = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
            project_id=project_id,
            limit=500,
        )
        items: list[DatasetCatalogItem] = []
        for dataset in datasets:
            columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
            items.append(
                DatasetCatalogItem(
                    id=dataset.id,
                    name=dataset.name,
                    dataset_type=DatasetType(dataset.dataset_type.upper()),
                    tags=list(dataset.tags_json or []),
                    columns=[self._to_column_response(column) for column in columns],
                    updated_at=dataset.updated_at,
                )
            )
        return DatasetCatalogResponse(workspace_id=workspace_id, items=items)

    async def get_usage(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> DatasetUsageResponse:
        impact = await self.get_impact(
            dataset_id=dataset_id,
            workspace_id=workspace_id,
            current_user=current_user,
        )
        return DatasetUsageResponse(
            semantic_models=[self._impact_item_to_legacy_dict(item) for item in impact.semantic_models],
            unified_semantic_models=[
                self._impact_item_to_legacy_dict(item) for item in impact.unified_semantic_models
            ],
            dependent_datasets=[
                self._impact_item_to_legacy_dict(item) for item in impact.dependent_datasets
            ],
            dashboards=[self._impact_item_to_legacy_dict(item) for item in impact.dashboards],
            saved_queries=[self._impact_item_to_legacy_dict(item) for item in impact.saved_queries],
        )

    async def get_lineage(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> DatasetLineageResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        await self._get_dataset(dataset_id=dataset_id, workspace_id=workspace_id)
        if self._lineage_service is None:
            return DatasetLineageResponse(dataset_id=dataset_id)
        nodes, edges, upstream_count, downstream_count = await self._lineage_service.build_dataset_lineage_graph(
            workspace_id=workspace_id,
            dataset_id=dataset_id,
        )
        return DatasetLineageResponse(
            dataset_id=dataset_id,
            nodes=[
                DatasetLineageNodeResponse(
                    node_type=DatasetLineageNodeType(node["node_type"]),
                    node_id=str(node["node_id"]),
                    label=str(node["label"]),
                    direction=str(node["direction"]),
                    metadata=dict(node.get("metadata") or {}),
                )
                for node in nodes
            ],
            edges=[
                DatasetLineageEdgeResponse(
                    source_type=DatasetLineageNodeType(edge.source_type),
                    source_id=edge.source_id,
                    target_type=DatasetLineageNodeType(edge.target_type),
                    target_id=edge.target_id,
                    edge_type=DatasetLineageEdgeType(edge.edge_type),
                    metadata=dict(edge.metadata_json or {}),
                )
                for edge in edges
            ],
            upstream_count=upstream_count,
            downstream_count=downstream_count,
        )

    async def get_impact(
        self,
        *,
        dataset_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> DatasetImpactResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        await self._get_dataset(dataset_id=dataset_id, workspace_id=workspace_id)
        if self._lineage_service is None:
            return DatasetImpactResponse(dataset_id=dataset_id)
        impact = await self._lineage_service.build_dataset_impact(
            workspace_id=workspace_id,
            dataset_id=dataset_id,
        )
        return DatasetImpactResponse(
            dataset_id=dataset_id,
            total_downstream_assets=int(impact.get("total_downstream_assets") or 0),
            direct_dependents=self._impact_items_from_payload(impact.get("direct_dependents")),
            dependent_datasets=self._impact_items_from_payload(impact.get("dependent_datasets")),
            semantic_models=self._impact_items_from_payload(impact.get("semantic_models")),
            unified_semantic_models=self._impact_items_from_payload(
                impact.get("unified_semantic_models")
            ),
            saved_queries=self._impact_items_from_payload(impact.get("saved_queries")),
            dashboards=self._impact_items_from_payload(impact.get("dashboards")),
        )

    async def preview_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        request: DatasetPreviewRequest,
        current_user: UserResponse,
    ) -> DatasetPreviewResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=request.workspace_id)
        policy = await self._get_or_create_policy(dataset)

        workspace_cap = await self._get_workspace_preview_cap(workspace_id=request.workspace_id)
        requested_limit = request.limit or policy.max_rows_preview
        effective_limit = min(requested_limit, policy.max_rows_preview, workspace_cap)

        job = await self._dataset_job_request_service.create_preview_job(
            CreateDatasetPreviewJobRequest(
                dataset_id=dataset.id,
                workspace_id=request.workspace_id,
                project_id=request.project_id,
                user_id=current_user.id,
                requested_limit=request.limit,
                enforced_limit=effective_limit,
                filters=request.filters,
                sort=[item.model_dump(mode="json") for item in request.sort],
                user_context=request.user_context,
                correlation_id=self._request_context_provider.correlation_id,
            )
        )
        return DatasetPreviewResponse(
            job_id=job.id,
            status=job.status.value,
            dataset_id=dataset.id,
            effective_limit=effective_limit,
        )

    async def profile_dataset(
        self,
        *,
        dataset_id: uuid.UUID,
        request: DatasetProfileRequest,
        current_user: UserResponse,
    ) -> DatasetProfileResponse:
        await self._assert_workspace_access(request.workspace_id, current_user)
        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=request.workspace_id)

        job = await self._dataset_job_request_service.create_profile_job(
            CreateDatasetProfileJobRequest(
                dataset_id=dataset.id,
                workspace_id=request.workspace_id,
                project_id=request.project_id,
                user_id=current_user.id,
                user_context=request.user_context,
                correlation_id=self._request_context_provider.correlation_id,
            )
        )
        return DatasetProfileResponse(
            job_id=job.id,
            status=job.status.value,
            dataset_id=dataset.id,
        )

    async def get_preview_job_result(
        self,
        *,
        dataset_id: uuid.UUID,
        job_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> DatasetPreviewResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=workspace_id)
        policy = await self._get_or_create_policy(dataset)
        workspace_cap = await self._get_workspace_preview_cap(workspace_id=workspace_id)
        latest_job = await self._get_dataset_job(
            dataset=dataset,
            job_id=job_id,
            workspace_id=workspace_id,
            expected_job_type=JobType.DATASET_PREVIEW,
            current_user=current_user,
        )
        return self._build_preview_response_from_job(
            dataset_id=dataset.id,
            policy_max_preview_rows=policy.max_rows_preview,
            workspace_preview_cap=workspace_cap,
            latest_job=latest_job,
        )

    async def get_profile_job_result(
        self,
        *,
        dataset_id: uuid.UUID,
        job_id: uuid.UUID,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> DatasetProfileResponse:
        await self._assert_workspace_access(workspace_id, current_user)
        dataset = await self._get_dataset(dataset_id=dataset_id, workspace_id=workspace_id)
        latest_job = await self._get_dataset_job(
            dataset=dataset,
            job_id=job_id,
            workspace_id=workspace_id,
            expected_job_type=JobType.DATASET_PROFILE,
            current_user=current_user,
        )
        return self._build_profile_response_from_job(
            dataset_id=dataset.id,
            latest_job=latest_job,
        )

    async def _create_dataset_from_table_selection(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        connection_id: uuid.UUID,
        schema_name: str,
        table_name: str,
        columns: list[DatasetSelectionColumnRequest],
        naming_template: str | None,
        requested_name: str | None,
        policy_defaults: DatasetPolicyDefaultsRequest | None,
        tags: list[str],
        current_user: UserResponse,
    ) -> DatasetRecord:
        selected_columns = [
            DatasetColumnRequest(
                name=column.name.strip(),
                data_type=(column.data_type or "unknown"),
                nullable=column.nullable if column.nullable is not None else True,
                is_allowed=True,
                is_computed=False,
                ordinal_position=index,
            )
            for index, column in enumerate(columns)
            if column.name.strip()
        ]
        if not selected_columns:
            selected_columns = await self._infer_columns_from_table(
                connection_id=connection_id,
                schema_name=schema_name,
                table_name=table_name,
            )

        connector = await self._connector_repository.get_by_id(connection_id)
        connection_name = getattr(connector, "name", None) or str(connection_id)
        inferred_base_name = self._render_dataset_name_template(
            connection_name=connection_name,
            schema_name=schema_name,
            table_name=table_name,
            naming_template=naming_template or "{schema}.{table}",
        )
        base_name = (requested_name or inferred_base_name).strip() or f"{schema_name}.{table_name}"
        suffix_seed = self._selection_signature(
            schema_name=schema_name,
            table_name=table_name,
            selected_columns=[column.name for column in selected_columns],
        )
        final_name = await self._ensure_unique_dataset_name(
            workspace_id=workspace_id,
            project_id=project_id,
            base_name=base_name,
            suffix_seed=suffix_seed,
        )

        now = datetime.now(timezone.utc)
        dataset = DatasetRecord(
            id=uuid.uuid4(),
            workspace_id=workspace_id,
            project_id=project_id,
            connection_id=connection_id,
            created_by=current_user.id,
            updated_by=current_user.id,
            name=final_name,
            description=None,
            tags_json=self._normalize_dataset_tags(tags),
            dataset_type=DatasetType.TABLE.value,
            dialect=None,
            catalog_name=None,
            schema_name=schema_name.strip() or None,
            table_name=table_name.strip() or None,
            sql_text=None,
            referenced_dataset_ids_json=[],
            federated_plan_json=None,
            file_config_json=None,
            status=DatasetStatus.PUBLISHED.value,
            revision_id=None,
            row_count_estimate=None,
            bytes_estimate=None,
            last_profiled_at=None,
            created_at=now,
            updated_at=now,
        )
        self._dataset_repository.add(dataset)
        await self._replace_columns(
            dataset=dataset,
            workspace_id=workspace_id,
            columns=selected_columns,
        )
        policy = await self._upsert_policy(
            dataset=dataset,
            policy=self._resolve_policy_defaults(policy_defaults),
        )
        revision_id = await self._create_revision(
            dataset=dataset,
            policy=policy,
            created_by=current_user.id,
            change_summary="Auto-generated dataset created.",
        )
        dataset.revision_id = revision_id
        dataset.updated_at = datetime.now(timezone.utc)
        await self._register_dataset_lineage(dataset)
        return dataset

    async def _find_existing_table_dataset(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        connection_id: uuid.UUID,
        schema_name: str,
        table_name: str,
        selected_columns: list[DatasetSelectionColumnRequest],
    ) -> DatasetRecord | None:
        candidates = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
            project_id=project_id,
            dataset_types=[DatasetType.TABLE.value],
            limit=5000,
        )
        requested_names = sorted(
            {column.name.strip().lower() for column in selected_columns if column.name.strip()}
        )
        requested_signature = ",".join(requested_names) if requested_names else "*"
        for dataset in candidates:
            if dataset.connection_id != connection_id:
                continue
            if (dataset.schema_name or "").strip().lower() != schema_name.strip().lower():
                continue
            if (dataset.table_name or "").strip().lower() != table_name.strip().lower():
                continue
            dataset_columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
            existing_names = sorted(
                {
                    column.name.strip().lower()
                    for column in dataset_columns
                    if column.is_allowed and column.name.strip()
                }
            )
            existing_signature = ",".join(existing_names) if existing_names else "*"
            if existing_signature == requested_signature:
                return dataset
        return None

    async def _ensure_unique_dataset_name(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        base_name: str,
        suffix_seed: str,
    ) -> str:
        rows = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
            project_id=project_id,
            limit=5000,
        )
        taken_names = {row.name.strip().lower() for row in rows if row.name}
        normalized_base = base_name.strip()
        if normalized_base.lower() not in taken_names:
            return normalized_base

        base_with_signature = f"{normalized_base}_{suffix_seed[:8]}"
        if base_with_signature.lower() not in taken_names:
            return base_with_signature

        counter = 2
        while True:
            candidate = f"{base_with_signature}_{counter}"
            if candidate.lower() not in taken_names:
                return candidate
            counter += 1

    def _render_dataset_name_template(
        self,
        *,
        connection_name: str,
        schema_name: str,
        table_name: str,
        naming_template: str,
    ) -> str:
        safe_schema = schema_name.strip() or "schema"
        safe_table = table_name.strip() or "table"
        safe_connection = connection_name.strip().replace(" ", "_").replace("-", "_")
        template = naming_template.strip() or "{schema}.{table}"
        return (
            template.replace("{schema}", safe_schema)
            .replace("{table}", safe_table)
            .replace("{connection}", safe_connection)
        )

    @staticmethod
    def _selection_signature(
        *,
        schema_name: str,
        table_name: str,
        selected_columns: list[str],
    ) -> str:
        column_part = ",".join(sorted({value.strip().lower() for value in selected_columns if value.strip()})) or "*"
        payload = f"{schema_name.strip().lower()}|{table_name.strip().lower()}|{column_part}"
        return hashlib.sha1(payload.encode("utf-8")).hexdigest()

    @staticmethod
    def _normalize_selection_columns(
        columns: list[DatasetSelectionColumnRequest],
    ) -> list[DatasetSelectionColumnRequest]:
        seen: set[str] = set()
        normalized: list[DatasetSelectionColumnRequest] = []
        for column in columns:
            key = column.name.strip().lower()
            if not key or key in seen:
                continue
            seen.add(key)
            normalized.append(
                DatasetSelectionColumnRequest(
                    name=column.name.strip(),
                    data_type=column.data_type,
                    nullable=column.nullable,
                )
            )
        return normalized

    @staticmethod
    def _normalize_dataset_tags(tags: list[str]) -> list[str]:
        normalized = [tag.strip() for tag in (tags or []) if tag and tag.strip()]
        lowered = {tag.lower() for tag in normalized}
        if _DATASET_AUTO_GENERATED_TAG not in lowered:
            normalized.append(_DATASET_AUTO_GENERATED_TAG)
        return normalized

    def _resolve_policy_defaults(
        self,
        policy_defaults: DatasetPolicyDefaultsRequest | None,
    ) -> DatasetPolicyRequest:
        max_preview_rows = (
            policy_defaults.max_preview_rows
            if policy_defaults and policy_defaults.max_preview_rows is not None
            else settings.SQL_DEFAULT_MAX_PREVIEW_ROWS
        )
        max_export_rows = (
            policy_defaults.max_export_rows
            if policy_defaults and policy_defaults.max_export_rows is not None
            else settings.SQL_DEFAULT_MAX_EXPORT_ROWS
        )
        max_preview_rows = max(
            1,
            min(int(max_preview_rows), int(settings.SQL_POLICY_MAX_PREVIEW_ROWS_UPPER_BOUND)),
        )
        max_export_rows = max(
            1,
            min(int(max_export_rows), int(settings.SQL_POLICY_MAX_EXPORT_ROWS_UPPER_BOUND)),
        )
        return DatasetPolicyRequest(
            max_rows_preview=max_preview_rows,
            max_export_rows=max_export_rows,
            allow_dml=bool(policy_defaults.allow_dml) if policy_defaults else False,
            redaction_rules=(dict(policy_defaults.redaction_rules) if policy_defaults else {}),
        )

    async def _resolve_create_columns(
        self,
        *,
        dataset: DatasetRecord,
        request: DatasetCreateRequest,
    ) -> list[DatasetColumnRequest]:
        if request.columns:
            return request.columns

        if request.dataset_type == DatasetType.TABLE:
            inferred = await self._infer_columns_from_table(
                connection_id=request.connection_id,
                schema_name=request.schema_name or "",
                table_name=request.table_name or "",
            )
            if inferred:
                return inferred
        elif request.dataset_type == DatasetType.SQL:
            inferred = await self._infer_columns_from_sql(
                connection_id=request.connection_id,
                dataset_dialect=request.dialect or "tsql",
                sql_text=request.sql_text or "",
            )
            if inferred:
                return inferred
        elif request.dataset_type == DatasetType.FILE:
            inferred = await self._infer_columns_from_file(
                storage_uri=request.storage_uri,
                file_config=request.file_config,
            )
            if inferred:
                return inferred

        return []

    async def _resolve_columns_for_dataset(
        self,
        dataset: DatasetRecord,
    ) -> list[DatasetColumnRequest]:
        dataset_type = DatasetType(dataset.dataset_type.upper())
        if dataset_type == DatasetType.TABLE:
            return await self._infer_columns_from_table(
                connection_id=dataset.connection_id,
                schema_name=dataset.schema_name or "",
                table_name=dataset.table_name or "",
            )
        if dataset_type == DatasetType.SQL:
            return await self._infer_columns_from_sql(
                connection_id=dataset.connection_id,
                dataset_dialect=dataset.dialect or "tsql",
                sql_text=dataset.sql_text or "",
            )
        if dataset_type == DatasetType.FILE:
            return await self._infer_columns_from_file(
                storage_uri=dataset.storage_uri,
                file_config=dataset.file_config_json,
            )
        return []

    async def _infer_columns_from_table(
        self,
        *,
        connection_id: uuid.UUID | None,
        schema_name: str,
        table_name: str,
    ) -> list[DatasetColumnRequest]:
        if connection_id is None or not table_name:
            return []
        connector = await self._connector_repository.get_by_id(connection_id)
        if connector is None:
            return []
        connector_type = getattr(connector, "connector_type", None)
        if not connector_type:
            return []

        connector_response = await self._connector_service.get_connector(connection_id)
        runtime_type = ConnectorRuntimeType(connector_type.upper())
        try:
            sql_connector = await self._connector_service.async_create_sql_connector(
                runtime_type,
                connector_response.config or {},
            )
            columns = await sql_connector.fetch_columns(schema_name, table_name)
        except Exception:
            return []

        inferred: list[DatasetColumnRequest] = []
        for index, column in enumerate(columns):
            inferred.append(
                DatasetColumnRequest(
                    name=column.name,
                    data_type=getattr(column, "data_type", "string"),
                    nullable=bool(getattr(column, "is_nullable", True)),
                    is_allowed=True,
                    ordinal_position=index,
                )
            )
        return inferred

    async def _infer_columns_from_sql(
        self,
        *,
        connection_id: uuid.UUID | None,
        dataset_dialect: str,
        sql_text: str,
    ) -> list[DatasetColumnRequest]:
        if connection_id is None or not sql_text.strip():
            return []
        connector = await self._connector_repository.get_by_id(connection_id)
        if connector is None:
            return []
        connector_type = getattr(connector, "connector_type", None)
        if not connector_type:
            return []

        connector_response = await self._connector_service.get_connector(connection_id)
        runtime_type = ConnectorRuntimeType(connector_type.upper())
        try:
            enforce_read_only_sql(sql_text, allow_dml=False, dialect=dataset_dialect)
            limited_sql, _ = enforce_preview_limit(
                sql_text,
                max_rows=1,
                dialect=dataset_dialect,
            )
            sql_connector = await self._connector_service.async_create_sql_connector(
                runtime_type,
                connector_response.config or {},
            )
            result = await sql_connector.execute(limited_sql, params={}, max_rows=1, timeout_s=10)
        except Exception:
            return []

        inferred: list[DatasetColumnRequest] = []
        for index, column_name in enumerate(result.columns):
            inferred.append(
                DatasetColumnRequest(
                    name=str(column_name),
                    data_type="unknown",
                    nullable=True,
                    is_allowed=True,
                    ordinal_position=index,
                )
            )
        return inferred

    async def _infer_columns_from_file(
        self,
        *,
        storage_uri: str | None,
        file_config: dict[str, Any] | None,
    ) -> list[DatasetColumnRequest]:
        config_payload = dict(file_config or {})
        resolved_uri = (storage_uri or "").strip()
        if not resolved_uri:
            resolved_uri = str(
                config_payload.get("storage_uri")
                or config_payload.get("uri")
                or config_payload.get("path")
                or ""
            ).strip()
        if not resolved_uri:
            return []

        normalized_uri = self._normalize_local_storage_uri(resolved_uri)
        file_format = self._infer_file_dataset_format(normalized_uri, config_payload)
        if file_format is None:
            return []

        scan_sql = self._build_duckdb_file_scan_sql(
            storage_uri=normalized_uri,
            file_format=file_format,
            file_config=config_payload,
        )
        if not scan_sql:
            return []

        try:
            import duckdb
        except Exception:
            return []

        connection = None
        try:
            connection = duckdb.connect(database=":memory:")
            rows = connection.execute(f"DESCRIBE SELECT * FROM {scan_sql}").fetchall()
        except Exception:
            return []
        finally:
            if connection is not None:
                try:
                    connection.close()
                except Exception:
                    pass

        inferred: list[DatasetColumnRequest] = []
        for index, row in enumerate(rows):
            if not row:
                continue
            name = str(row[0] or "").strip()
            if not name:
                continue
            data_type = str(row[1] or "string").strip() or "string"
            nullable_flag = str(row[2] or "").strip().upper()
            inferred.append(
                DatasetColumnRequest(
                    name=name,
                    data_type=data_type.lower(),
                    nullable=nullable_flag != "NO",
                    is_allowed=True,
                    ordinal_position=index,
                )
            )
        return inferred

    async def _replace_columns(
        self,
        *,
        dataset: DatasetRecord,
        workspace_id: uuid.UUID,
        columns: list[DatasetColumnRequest],
    ) -> None:
        await self._dataset_column_repository.delete_for_dataset(dataset_id=dataset.id)
        for index, column in enumerate(columns):
            column_record = DatasetColumnRecord(
                id=uuid.uuid4(),
                dataset_id=dataset.id,
                workspace_id=workspace_id,
                name=column.name,
                data_type=column.data_type,
                nullable=column.nullable,
                ordinal_position=column.ordinal_position if column.ordinal_position is not None else index,
                description=column.description,
                is_allowed=column.is_allowed,
                is_computed=column.is_computed,
                expression=column.expression,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            self._dataset_column_repository.add(column_record)

    async def _upsert_policy(
        self,
        *,
        dataset: DatasetRecord,
        policy: DatasetPolicyRequest | None,
    ) -> DatasetPolicyRecord:
        existing = await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
        if existing is None:
            existing = DatasetPolicyRecord(
                id=uuid.uuid4(),
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                max_rows_preview=settings.SQL_DEFAULT_MAX_PREVIEW_ROWS,
                max_export_rows=settings.SQL_DEFAULT_MAX_EXPORT_ROWS,
                redaction_rules_json={},
                row_filters_json=[],
                allow_dml=False,
                created_at=datetime.now(timezone.utc),
                updated_at=datetime.now(timezone.utc),
            )
            self._dataset_policy_repository.add(existing)
        self._set_cached_policy(dataset, existing)

        if policy is not None:
            if policy.max_rows_preview is not None:
                existing.max_rows_preview = policy.max_rows_preview
            if policy.max_export_rows is not None:
                existing.max_export_rows = policy.max_export_rows
            if policy.redaction_rules is not None:
                existing.redaction_rules_json = dict(policy.redaction_rules)
            if policy.row_filters is not None:
                existing.row_filters_json = list(policy.row_filters)
            if policy.allow_dml is not None:
                existing.allow_dml = bool(policy.allow_dml)
            existing.updated_at = datetime.now(timezone.utc)

        return existing

    async def _create_revision(
        self,
        *,
        dataset: DatasetRecord,
        policy: DatasetPolicyRecord,
        created_by: uuid.UUID,
        change_summary: str,
    ) -> uuid.UUID:
        columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
        next_revision = await self._dataset_revision_repository.next_revision_number(dataset_id=dataset.id)
        definition = self._build_dataset_definition_snapshot(dataset)
        schema_snapshot = [self._to_column_response(column).model_dump(mode="json") for column in columns]
        policy_snapshot = self._to_policy_response(policy).model_dump(mode="json")
        source_bindings = await self._build_dataset_source_bindings(dataset)
        execution_characteristics = {
            "row_count_estimate": dataset.row_count_estimate,
            "bytes_estimate": dataset.bytes_estimate,
            "last_profiled_at": dataset.last_profiled_at.isoformat() if dataset.last_profiled_at else None,
        }
        snapshot = {
            "dataset": definition,
            "columns": schema_snapshot,
            "policy": policy_snapshot,
            "source_bindings": source_bindings,
            "execution_characteristics": execution_characteristics,
        }
        revision_hash = stable_payload_hash(snapshot)
        revision_id = uuid.uuid4()
        self._dataset_revision_repository.add(
            DatasetRevisionRecord(
                id=revision_id,
                dataset_id=dataset.id,
                workspace_id=dataset.workspace_id,
                revision_number=next_revision,
                revision_hash=revision_hash,
                change_summary=change_summary,
                definition_json=definition,
                schema_json=schema_snapshot,
                policy_json=policy_snapshot,
                source_bindings_json=source_bindings,
                execution_characteristics_json=execution_characteristics,
                status=dataset.status,
                snapshot_json=snapshot,
                note=change_summary,
                created_by=created_by,
                created_at=datetime.now(timezone.utc),
            )
        )
        return revision_id

    def _build_dataset_definition_snapshot(self, dataset: DatasetRecord) -> dict[str, Any]:
        return {
            "id": str(dataset.id),
            "workspace_id": str(dataset.workspace_id),
            "project_id": str(dataset.project_id) if dataset.project_id else None,
            "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
            "name": dataset.name,
            "description": dataset.description,
            "tags": list(dataset.tags_json or []),
            "dataset_type": dataset.dataset_type,
            "dialect": dataset.dialect,
            "storage_uri": dataset.storage_uri,
            "catalog_name": dataset.catalog_name,
            "schema_name": dataset.schema_name,
            "table_name": dataset.table_name,
            "sql_text": dataset.sql_text,
            "referenced_dataset_ids": list(dataset.referenced_dataset_ids_json or []),
            "federated_plan": dataset.federated_plan_json,
            "file_config": dataset.file_config_json,
            "status": dataset.status,
        }

    async def _build_dataset_source_bindings(self, dataset: DatasetRecord) -> list[dict[str, Any]]:
        dataset_type = DatasetType(dataset.dataset_type.upper())
        if dataset_type == DatasetType.TABLE:
            return [
                {
                    "source_type": "connection",
                    "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                },
                {
                    "source_type": "source_table",
                    "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                    "catalog_name": dataset.catalog_name,
                    "schema_name": dataset.schema_name,
                    "table_name": dataset.table_name,
                },
            ]
        if dataset_type == DatasetType.SQL:
            bindings: list[dict[str, Any]] = []
            if dataset.connection_id:
                bindings.append(
                    {
                        "source_type": "connection",
                        "connection_id": str(dataset.connection_id),
                    }
                )
            if self._lineage_service is not None and (dataset.sql_text or "").strip():
                dataset_refs, source_refs = await self._lineage_service._resolve_sql_references(
                    workspace_id=dataset.workspace_id,
                    project_id=dataset.project_id,
                    connection_id=dataset.connection_id,
                    query_text=dataset.sql_text or "",
                    default_catalog=dataset.catalog_name,
                )
                bindings.extend(
                    {
                        "source_type": "dataset",
                        "dataset_id": str(ref),
                    }
                    for ref in dataset_refs
                    if ref != dataset.id
                )
                bindings.extend(
                    {
                        "source_type": "source_table",
                        **dict(ref["metadata"]),
                    }
                    for ref in source_refs
                )
            return bindings
        if dataset_type == DatasetType.FILE:
            sync_meta = (
                (dataset.file_config_json or {}).get("connector_sync")
                if isinstance(dataset.file_config_json, dict)
                else None
            )
            storage_uri = (
                str((dataset.file_config_json or {}).get("source_storage_uri") or "").strip()
                or str((dataset.file_config_json or {}).get("storage_uri") or "").strip()
                or dataset.storage_uri
            )
            bindings = [
                {
                    "source_type": "file_resource",
                    "storage_uri": storage_uri,
                    "file_config": dict(dataset.file_config_json or {}),
                }
            ]
            if isinstance(sync_meta, dict):
                bindings.insert(
                    0,
                    {
                        "source_type": "api_resource",
                        "connection_id": str(dataset.connection_id) if dataset.connection_id else None,
                        "connector_type": sync_meta.get("connector_type"),
                        "resource_name": sync_meta.get("resource_name"),
                        "root_resource_name": sync_meta.get("root_resource_name"),
                        "parent_resource_name": sync_meta.get("parent_resource_name"),
                    },
                )
                if dataset.connection_id is not None:
                    bindings.insert(
                        0,
                        {
                            "source_type": "connection",
                            "connection_id": str(dataset.connection_id),
                        },
                    )
            return bindings
        if dataset_type == DatasetType.FEDERATED:
            return [
                {
                    "source_type": "dataset",
                    "dataset_id": value,
                }
                for value in self._extract_federated_source_ids(dataset)
            ]
        return []

    def _build_revision_snapshot_payload(self, revision: DatasetRevisionRecord) -> dict[str, Any]:
        definition = dict(revision.definition_json or {})
        schema_snapshot = list(revision.schema_json or [])
        policy_snapshot = dict(revision.policy_json or {})
        source_bindings = list(revision.source_bindings_json or [])
        execution_characteristics = (
            dict(revision.execution_characteristics_json or {})
            if revision.execution_characteristics_json is not None
            else None
        )
        legacy_snapshot = dict(revision.snapshot_json or {})

        if not definition:
            definition = dict(legacy_snapshot.get("dataset") or {})
        if not schema_snapshot:
            schema_snapshot = list(legacy_snapshot.get("columns") or [])
        if not policy_snapshot:
            policy_snapshot = dict(legacy_snapshot.get("policy") or {})
        if not source_bindings:
            source_bindings = list(legacy_snapshot.get("source_bindings") or [])
        if execution_characteristics is None and isinstance(
            legacy_snapshot.get("execution_characteristics"), dict
        ):
            execution_characteristics = dict(legacy_snapshot["execution_characteristics"])

        return {
            "definition": definition,
            "schema": schema_snapshot,
            "policy": policy_snapshot,
            "source_bindings": source_bindings,
            "execution_characteristics": execution_characteristics,
            "legacy_snapshot": legacy_snapshot or None,
        }

    def _to_dataset_version_summary(
        self,
        dataset: DatasetRecord,
        revision: DatasetRevisionRecord,
    ) -> DatasetVersionSummaryResponse:
        status_value = revision.status or self._build_revision_snapshot_payload(revision)["definition"].get("status")
        return DatasetVersionSummaryResponse(
            id=revision.id,
            dataset_id=revision.dataset_id,
            revision_number=revision.revision_number,
            revision_hash=revision.revision_hash or stable_payload_hash(revision.snapshot_json or {}),
            created_at=revision.created_at,
            created_by=revision.created_by,
            change_summary=revision.change_summary or revision.note,
            status=(DatasetStatus(status_value) if status_value in DatasetStatus._value2member_map_ else None),
            is_current=dataset.revision_id == revision.id,
        )

    def _to_dataset_version_response(
        self,
        dataset: DatasetRecord,
        revision: DatasetRevisionRecord,
    ) -> DatasetVersionResponse:
        payload = self._build_revision_snapshot_payload(revision)
        summary = self._to_dataset_version_summary(dataset, revision)
        return DatasetVersionResponse(
            id=summary.id,
            dataset_id=summary.dataset_id,
            revision_number=summary.revision_number,
            revision_hash=summary.revision_hash,
            created_at=summary.created_at,
            created_by=summary.created_by,
            change_summary=summary.change_summary,
            status=summary.status,
            is_current=summary.is_current,
            definition_snapshot=payload["definition"],
            schema_snapshot=payload["schema"],
            policy_snapshot=payload["policy"],
            source_bindings_snapshot=payload["source_bindings"],
            execution_characteristics_snapshot=payload["execution_characteristics"],
            legacy_snapshot=payload["legacy_snapshot"],
        )

    async def _get_dataset_revision(
        self,
        *,
        dataset_id: uuid.UUID,
        revision_id: uuid.UUID,
    ) -> DatasetRevisionRecord:
        revision = await self._dataset_revision_repository.get_for_dataset(
            dataset_id=dataset_id,
            revision_id=revision_id,
        )
        if revision is None:
            raise ResourceNotFound("Dataset revision not found.")
        return revision

    def _diff_dataset_revisions(
        self,
        dataset: DatasetRecord,
        from_revision: DatasetRevisionRecord,
        to_revision: DatasetRevisionRecord,
    ) -> DatasetVersionDiffResponse:
        from_payload = self._build_revision_snapshot_payload(from_revision)
        to_payload = self._build_revision_snapshot_payload(to_revision)
        definition_changes = self._diff_object_fields(from_payload["definition"], to_payload["definition"])
        policy_changes = self._diff_object_fields(from_payload["policy"], to_payload["policy"])
        source_binding_changes = self._diff_list_payloads(
            from_payload["source_bindings"],
            to_payload["source_bindings"],
            field_name="source_bindings",
        )
        execution_changes = self._diff_object_fields(
            from_payload["execution_characteristics"] or {},
            to_payload["execution_characteristics"] or {},
        )
        schema_changes = self._diff_schema_payloads(from_payload["schema"], to_payload["schema"])

        summary: list[str] = []
        if schema_changes:
            summary.append(f"{len(schema_changes)} schema change(s)")
        if definition_changes:
            summary.append(f"{len(definition_changes)} definition change(s)")
        if policy_changes:
            summary.append(f"{len(policy_changes)} policy change(s)")
        if source_binding_changes:
            summary.append(f"{len(source_binding_changes)} source binding change(s)")
        if execution_changes:
            summary.append(f"{len(execution_changes)} execution metadata change(s)")

        return DatasetVersionDiffResponse(
            dataset_id=dataset.id,
            from_revision_id=from_revision.id,
            to_revision_id=to_revision.id,
            from_revision_number=from_revision.revision_number,
            to_revision_number=to_revision.revision_number,
            summary=summary,
            definition_changes=definition_changes,
            policy_changes=policy_changes,
            source_binding_changes=source_binding_changes,
            execution_changes=execution_changes,
            schema_changes=schema_changes,
        )

    def _diff_object_fields(
        self,
        before: dict[str, Any],
        after: dict[str, Any],
    ) -> list[DatasetVersionFieldDiff]:
        changes: list[DatasetVersionFieldDiff] = []
        keys = sorted(set(before.keys()) | set(after.keys()))
        for key in keys:
            if before.get(key) == after.get(key):
                continue
            change_type = "changed"
            if key not in before:
                change_type = "added"
            elif key not in after:
                change_type = "removed"
            changes.append(
                DatasetVersionFieldDiff(
                    field=key,
                    change_type=change_type,
                    before=before.get(key),
                    after=after.get(key),
                )
            )
        return changes

    def _diff_list_payloads(
        self,
        before: list[dict[str, Any]],
        after: list[dict[str, Any]],
        *,
        field_name: str,
    ) -> list[DatasetVersionFieldDiff]:
        if before == after:
            return []
        return [
            DatasetVersionFieldDiff(
                field=field_name,
                change_type="changed",
                before=before,
                after=after,
            )
        ]

    def _diff_schema_payloads(
        self,
        before: list[dict[str, Any]],
        after: list[dict[str, Any]],
    ) -> list[DatasetSchemaColumnDiff]:
        before_map = {str(item.get("name") or ""): item for item in before if item.get("name")}
        after_map = {str(item.get("name") or ""): item for item in after if item.get("name")}
        changes: list[DatasetSchemaColumnDiff] = []
        for column_name in sorted(set(before_map.keys()) | set(after_map.keys())):
            if column_name not in before_map:
                changes.append(
                    DatasetSchemaColumnDiff(
                        column_name=column_name,
                        change_type="added",
                        before=None,
                        after=after_map[column_name],
                    )
                )
            elif column_name not in after_map:
                changes.append(
                    DatasetSchemaColumnDiff(
                        column_name=column_name,
                        change_type="removed",
                        before=before_map[column_name],
                        after=None,
                    )
                )
            elif before_map[column_name] != after_map[column_name]:
                changes.append(
                    DatasetSchemaColumnDiff(
                        column_name=column_name,
                        change_type="changed",
                        before=before_map[column_name],
                        after=after_map[column_name],
                    )
                )
        return changes

    async def _register_dataset_lineage(self, dataset: DatasetRecord) -> None:
        if self._lineage_service is None:
            return
        await self._lineage_service.register_dataset_lineage(dataset=dataset)

    @staticmethod
    def _parse_optional_uuid(value: Any) -> uuid.UUID | None:
        if value in {None, ""}:
            return None
        try:
            return uuid.UUID(str(value))
        except (TypeError, ValueError):
            return None

    def _column_request_from_snapshot(self, value: dict[str, Any]) -> DatasetColumnRequest:
        return DatasetColumnRequest(
            name=str(value.get("name") or ""),
            data_type=str(value.get("data_type") or value.get("dataType") or "unknown"),
            nullable=bool(value.get("nullable", True)),
            description=value.get("description"),
            is_allowed=bool(value.get("is_allowed", value.get("isAllowed", True))),
            is_computed=bool(value.get("is_computed", value.get("isComputed", False))),
            expression=value.get("expression"),
            ordinal_position=value.get("ordinal_position", value.get("ordinalPosition")),
        )

    @staticmethod
    def _extract_federated_source_ids(dataset: DatasetRecord) -> list[str]:
        ids: list[str] = []
        for raw_id in dataset.referenced_dataset_ids_json or []:
            if raw_id and raw_id not in ids:
                ids.append(str(raw_id))
        plan = dataset.federated_plan_json if isinstance(dataset.federated_plan_json, dict) else {}
        tables_payload = plan.get("tables")
        iterable = tables_payload.values() if isinstance(tables_payload, dict) else tables_payload or []
        for item in iterable:
            if not isinstance(item, dict):
                continue
            raw_id = item.get("dataset_id") or item.get("datasetId")
            if raw_id and str(raw_id) not in ids:
                ids.append(str(raw_id))
        return ids

    def _impact_items_from_payload(self, payload: Any) -> list[DatasetImpactItemResponse]:
        items: list[DatasetImpactItemResponse] = []
        if not isinstance(payload, list):
            return items
        for item in payload:
            if not isinstance(item, dict):
                continue
            node_type = str(item.get("node_type") or "")
            if node_type not in DatasetLineageNodeType._value2member_map_:
                continue
            items.append(
                DatasetImpactItemResponse(
                    node_type=DatasetLineageNodeType(node_type),
                    node_id=str(item.get("node_id") or ""),
                    label=str(item.get("label") or item.get("node_id") or ""),
                    direct=bool(item.get("direct")),
                    metadata=dict(item.get("metadata") or {}),
                )
            )
        return items

    def _impact_item_to_legacy_dict(self, item: DatasetImpactItemResponse) -> dict[str, Any]:
        return {
            "id": item.node_id,
            "name": item.label,
            "node_type": item.node_type.value,
            "direct": item.direct,
            **dict(item.metadata or {}),
        }

    async def _to_dataset_response(self, dataset: DatasetRecord) -> DatasetResponse:
        columns = await self._dataset_column_repository.list_for_dataset(dataset_id=dataset.id)
        policy = await self._get_or_create_policy(dataset)
        return DatasetResponse(
            id=dataset.id,
            workspace_id=dataset.workspace_id,
            project_id=dataset.project_id,
            connection_id=dataset.connection_id,
            owner_id=dataset.created_by,
            name=dataset.name,
            description=dataset.description,
            tags=list(dataset.tags_json or []),
            dataset_type=DatasetType(dataset.dataset_type.upper()),
            dialect=dataset.dialect,
            storage_uri=dataset.storage_uri,
            catalog_name=dataset.catalog_name,
            schema_name=dataset.schema_name,
            table_name=dataset.table_name,
            sql_text=dataset.sql_text,
            referenced_dataset_ids=[
                uuid.UUID(value) for value in (dataset.referenced_dataset_ids_json or []) if value
            ],
            federated_plan=dataset.federated_plan_json,
            file_config=dataset.file_config_json,
            status=DatasetStatus(dataset.status),
            revision_id=dataset.revision_id,
            columns=[self._to_column_response(column) for column in columns],
            policy=self._to_policy_response(policy),
            stats=DatasetStatsResponse(
                row_count_estimate=dataset.row_count_estimate,
                bytes_estimate=dataset.bytes_estimate,
                last_profiled_at=dataset.last_profiled_at,
            ),
            created_at=dataset.created_at,
            updated_at=dataset.updated_at,
        )

    @staticmethod
    def _normalize_local_storage_uri(storage_uri: str) -> str:
        normalized = (storage_uri or "").strip()
        if normalized.startswith("file://"):
            return normalized[7:]
        return normalized

    @staticmethod
    def _infer_file_dataset_format(storage_uri: str, file_config: dict[str, Any]) -> str | None:
        configured = str(file_config.get("format") or file_config.get("file_format") or "").strip().lower()
        if configured in {"csv", "parquet"}:
            return configured
        lowered_uri = storage_uri.lower()
        if lowered_uri.endswith(".parquet"):
            return "parquet"
        if lowered_uri.endswith(".csv"):
            return "csv"
        return None

    @staticmethod
    def _build_duckdb_file_scan_sql(
        *,
        storage_uri: str,
        file_format: str,
        file_config: dict[str, Any],
    ) -> str | None:
        escaped_uri = storage_uri.replace("'", "''")
        if file_format == "parquet":
            return f"read_parquet('{escaped_uri}')"
        if file_format == "csv":
            header = "true" if bool(file_config.get("header", True)) else "false"
            delimiter = str(file_config.get("delimiter") or ",").replace("'", "''")
            quote = str(file_config.get("quote") or '\"').replace("'", "''")
            return (
                "read_csv_auto("
                f"'{escaped_uri}', "
                f"header={header}, "
                f"delim='{delimiter}', "
                f"quote='{quote}'"
                ")"
            )
        return None

    async def _get_dataset(self, *, dataset_id: uuid.UUID, workspace_id: uuid.UUID) -> DatasetRecord:
        dataset = await self._dataset_repository.get_for_workspace(
            dataset_id=dataset_id,
            workspace_id=workspace_id,
        )
        if dataset is None:
            raise ResourceNotFound("Dataset not found.")
        return dataset

    async def _get_or_create_policy(self, dataset: DatasetRecord) -> DatasetPolicyRecord:
        cached_policy = self._get_cached_policy(dataset)
        if cached_policy is not None:
            return cached_policy
        policy = await self._dataset_policy_repository.get_for_dataset(dataset_id=dataset.id)
        if policy is not None:
            self._set_cached_policy(dataset, policy)
            return policy
        policy = DatasetPolicyRecord(
            id=uuid.uuid4(),
            dataset_id=dataset.id,
            workspace_id=dataset.workspace_id,
            max_rows_preview=settings.SQL_DEFAULT_MAX_PREVIEW_ROWS,
            max_export_rows=settings.SQL_DEFAULT_MAX_EXPORT_ROWS,
            redaction_rules_json={},
            row_filters_json=[],
            allow_dml=False,
            created_at=datetime.now(timezone.utc),
            updated_at=datetime.now(timezone.utc),
        )
        self._set_cached_policy(dataset, policy)
        self._dataset_policy_repository.add(policy)
        return policy

    def _get_cached_policy(self, dataset: DatasetRecord) -> DatasetPolicyRecord | None:
        cached = getattr(dataset, self._policy_cache_attr, None)
        if isinstance(cached, DatasetPolicyRecord):
            return cached
        return None

    def _set_cached_policy(self, dataset: DatasetRecord, policy: DatasetPolicyRecord) -> None:
        setattr(dataset, self._policy_cache_attr, policy)

    def _to_column_response(self, column: DatasetColumnRecord) -> DatasetColumnResponse:
        return DatasetColumnResponse(
            id=column.id,
            dataset_id=column.dataset_id,
            name=column.name,
            data_type=column.data_type,
            nullable=column.nullable,
            description=column.description,
            is_allowed=column.is_allowed,
            is_computed=column.is_computed,
            expression=column.expression,
            ordinal_position=column.ordinal_position,
        )

    @staticmethod
    def _to_policy_response(policy: DatasetPolicyRecord) -> DatasetPolicyResponse:
        return DatasetPolicyResponse(
            max_rows_preview=policy.max_rows_preview,
            max_export_rows=policy.max_export_rows,
            redaction_rules=dict(policy.redaction_rules_json or {}),
            row_filters=list(policy.row_filters_json or []),
            allow_dml=policy.allow_dml,
        )

    async def _get_workspace_preview_cap(self, *, workspace_id: uuid.UUID) -> int:
        sql_policy = await self._sql_workspace_policy_repository.get_by_workspace_id(
            workspace_id=workspace_id
        )
        if sql_policy is not None:
            return sql_policy.max_preview_rows
        return settings.SQL_DEFAULT_MAX_PREVIEW_ROWS

    async def _get_dataset_job(
        self,
        *,
        dataset: DatasetRecord,
        job_id: uuid.UUID,
        workspace_id: uuid.UUID,
        expected_job_type: JobType,
        current_user: UserResponse,
    ) -> JobRecord:
        job = await self._job_repository.get_by_id(job_id)
        if job is None:
            raise ResourceNotFound("Dataset execution job not found.")
        if str(job.organisation_id) != str(workspace_id):
            raise PermissionDeniedBusinessValidationError("Dataset execution job is outside this workspace.")
        if str(job.job_type) != expected_job_type.value:
            raise BusinessValidationError("Dataset execution job type does not match this endpoint.")

        payload = job.payload if isinstance(job.payload, dict) else {}
        payload_dataset_id = str(payload.get("dataset_id") or "").strip()
        if payload_dataset_id != str(dataset.id):
            raise PermissionDeniedBusinessValidationError("Dataset execution job does not belong to this dataset.")

        if not self._is_internal_user(current_user):
            payload_user_id = str(payload.get("user_id") or "").strip()
            if payload_user_id and payload_user_id != str(current_user.id):
                raise PermissionDeniedBusinessValidationError("You do not have access to this dataset execution job.")

        return job

    def _build_preview_response_from_job(
        self,
        *,
        dataset_id: uuid.UUID,
        policy_max_preview_rows: int,
        workspace_preview_cap: int,
        latest_job: JobRecord,
    ) -> DatasetPreviewResponse:
        payload = latest_job.result if isinstance(latest_job.result, dict) else {}
        result = payload.get("result") if isinstance(payload.get("result"), dict) else payload
        requested_limit_raw = (
            latest_job.payload.get("requested_limit")
            if isinstance(latest_job.payload, dict)
            else None
        )
        requested_limit = int(requested_limit_raw) if requested_limit_raw is not None else policy_max_preview_rows
        effective_limit = min(requested_limit, policy_max_preview_rows, workspace_preview_cap)

        if latest_job.status != JobStatus.succeeded:
            error = None
            if isinstance(latest_job.error, dict):
                error = str(latest_job.error.get("message") or "")
            return DatasetPreviewResponse(
                job_id=latest_job.id,
                status=latest_job.status.value,
                dataset_id=dataset_id,
                effective_limit=effective_limit,
                error=error or "Dataset preview job did not complete successfully.",
            )

        if result is None:
            return DatasetPreviewResponse(
                job_id=latest_job.id,
                status=latest_job.status.value,
                dataset_id=dataset_id,
                effective_limit=effective_limit,
                error="Dataset preview job did not return a result.",
            )

        columns = [
            DatasetPreviewColumn(
                name=str(column.get("name") or ""),
                data_type=(str(column.get("type")) if column.get("type") is not None else None),
            )
            for column in (result.get("columns") or [])
            if isinstance(column, dict) and str(column.get("name") or "").strip()
        ]
        rows = [row for row in (result.get("rows") or []) if isinstance(row, dict)]
        result_effective_limit = (
            int(result["effective_limit"]) if result.get("effective_limit") is not None else effective_limit
        )
        return DatasetPreviewResponse(
            job_id=latest_job.id,
            status=latest_job.status.value,
            dataset_id=dataset_id,
            columns=columns,
            rows=rows,
            row_count_preview=int(result.get("row_count_preview") or len(rows)),
            effective_limit=result_effective_limit,
            redaction_applied=bool(result.get("redaction_applied")),
            duration_ms=(int(result["duration_ms"]) if result.get("duration_ms") is not None else None),
            bytes_scanned=(int(result["bytes_scanned"]) if result.get("bytes_scanned") is not None else None),
        )

    @staticmethod
    def _build_profile_response_from_job(
        *,
        dataset_id: uuid.UUID,
        latest_job: JobRecord,
    ) -> DatasetProfileResponse:
        payload = latest_job.result if isinstance(latest_job.result, dict) else {}
        result = payload.get("result") if isinstance(payload.get("result"), dict) else payload

        if latest_job.status != JobStatus.succeeded:
            error = None
            if isinstance(latest_job.error, dict):
                error = str(latest_job.error.get("message") or "")
            return DatasetProfileResponse(
                job_id=latest_job.id,
                status=latest_job.status.value,
                dataset_id=dataset_id,
                error=error or "Dataset profile job did not complete successfully.",
            )

        if result is None:
            return DatasetProfileResponse(
                job_id=latest_job.id,
                status=latest_job.status.value,
                dataset_id=dataset_id,
                error="Dataset profile job did not return a result.",
            )

        profiled_at = None
        if isinstance(result.get("profiled_at"), str):
            try:
                profiled_at = datetime.fromisoformat(str(result["profiled_at"]))
            except ValueError:
                profiled_at = datetime.now(timezone.utc)
        return DatasetProfileResponse(
            job_id=latest_job.id,
            status=latest_job.status.value,
            dataset_id=dataset_id,
            row_count_estimate=(
                int(result["row_count_estimate"]) if result.get("row_count_estimate") is not None else None
            ),
            bytes_estimate=(int(result["bytes_estimate"]) if result.get("bytes_estimate") is not None else None),
            distinct_counts={
                str(key): int(value)
                for key, value in (result.get("distinct_counts") or {}).items()
                if value is not None
            },
            null_rates={
                str(key): float(value)
                for key, value in (result.get("null_rates") or {}).items()
                if value is not None
            },
            profiled_at=profiled_at,
        )

    async def _assert_workspace_access(
        self,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> None:
        if self._is_internal_user(current_user):
            return
        allowed = {str(item) for item in (current_user.available_organizations or [])}
        if str(workspace_id) not in allowed:
            raise PermissionDeniedBusinessValidationError("Forbidden")

    async def _assert_workspace_admin(
        self,
        workspace_id: uuid.UUID,
        current_user: UserResponse,
    ) -> None:
        if self._is_internal_user(current_user):
            return
        organization = await self._organization_repository.get_by_id(workspace_id)
        if organization is None:
            raise ResourceNotFound("Workspace not found.")
        user = await self._user_repository.get_by_id(current_user.id)
        if user is None:
            raise ResourceNotFound("User not found.")
        role = await self._organization_repository.get_member_role(organization, user)
        if role not in {"owner", "admin"}:
            raise PermissionDeniedBusinessValidationError(
                "Dataset create/update/delete requires workspace admin permissions."
            )

    async def _validate_connection_scope(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID | None,
    ) -> None:
        if connection_id is None:
            return
        connector = await self._connector_repository.get_by_id(connection_id)
        if connector is None:
            raise ResourceNotFound("Connection not found.")
        if not self._connector_is_in_workspace(connector, workspace_id):
            raise PermissionDeniedBusinessValidationError("Connection does not belong to this workspace.")

    async def _validate_dataset_feature_flags(self, dataset_type: DatasetType) -> None:
        if dataset_type == DatasetType.FEDERATED and not settings.SQL_FEDERATION_ENABLED:
            raise BusinessValidationError("Federated datasets are disabled in this deployment.")
        if dataset_type == DatasetType.FILE and not getattr(settings, "DATASET_FILE_ENABLED", False):
            raise BusinessValidationError("File datasets are disabled in this deployment.")

    def _assert_no_secret_payload(self, payload: Any, *, context: str) -> None:
        if payload is None:
            return
        if isinstance(payload, dict):
            for key, value in payload.items():
                lowered = str(key).strip().lower()
                if lowered in _FORBIDDEN_SECRET_KEYS and value not in {None, ""}:
                    raise BusinessValidationError(
                        f"{context} must not include secret field '{key}'."
                    )
                self._assert_no_secret_payload(value, context=context)
            return
        if isinstance(payload, list):
            for item in payload:
                self._assert_no_secret_payload(item, context=context)

    @staticmethod
    def _connector_is_in_workspace(connector: Any, workspace_id: uuid.UUID) -> bool:
        organizations = getattr(connector, "organizations", None) or []
        return any(str(getattr(org, "id", "")) == str(workspace_id) for org in organizations)

    @staticmethod
    def _is_internal_user(user: UserResponse) -> bool:
        return user.id.int == 0

    @staticmethod
    def _parse_json_or_yaml(
        content_json: str | None,
        content_yaml: str | None,
    ) -> dict[str, Any] | None:
        if content_json:
            try:
                parsed = json.loads(content_json)
                if isinstance(parsed, dict):
                    return parsed
            except Exception:
                pass
        if content_yaml:
            try:
                parsed_yaml = yaml.safe_load(content_yaml)
                if isinstance(parsed_yaml, dict):
                    return parsed_yaml
            except Exception:
                return None
        return None
