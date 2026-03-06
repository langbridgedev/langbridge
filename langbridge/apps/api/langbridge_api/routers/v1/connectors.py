from typing import Optional
from uuid import UUID
from dependency_injector.wiring import Provide, inject
from fastapi import APIRouter, Depends, HTTPException, Query, status

from langbridge.apps.api.langbridge_api.auth.dependencies import get_current_user, get_organization, get_project
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorConfigSchema
from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.apps.api.langbridge_api.ioc import Container
from langbridge.packages.common.langbridge_common.contracts.auth import UserResponse
from langbridge.packages.common.langbridge_common.contracts.connectors import (
    ConnectorResponse,
    ConnectorCatalogResponse,
    ConnectorResourceListResponse,
    ConnectorSourceSchemaColumnResponse,
    ConnectorSourceSchemaResponse,
    ConnectorSourceSchemaTableResponse,
    ConnectorSourceSchemasResponse,
    ConnectorSyncHistoryResponse,
    ConnectorSyncRequest,
    ConnectorSyncStartResponse,
    ConnectorSyncStateListResponse,
    ConnectorTestResponse,
    CreateConnectorRequest,
    UpdateConnectorRequest,
)
from langbridge.apps.api.langbridge_api.services.connector_schema_service import ConnectorSchemaService
from langbridge.apps.api.langbridge_api.services.connector_sync_service import ConnectorSyncService
from langbridge.apps.api.langbridge_api.services.connector_service import ConnectorService

router = APIRouter(prefix="/connectors/{organization_id}", tags=["connectors"])


@router.post("/", response_model=ConnectorResponse)
@inject
async def create_connector(
    request: CreateConnectorRequest,
    organization_id: UUID,
    project_id: Optional[UUID] = None,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    _proj = Depends(get_project),
    connector_service: ConnectorService = Depends(Provide[Container.connector_service]),
    connector_schema_service: ConnectorSchemaService = Depends(
        Provide[Container.connector_schema_service]
    ),
) -> ConnectorResponse:
    connector = await connector_service.create_connector(request)
    try:
        connector.catalog_summary = await connector_schema_service.get_catalog_summary(
            connector_id=connector.id,
            include_system_schemas=False,
        )
    except Exception:
        # Keep connector creation resilient even when catalog introspection fails.
        connector.catalog_summary = None
    return connector


@router.get("/{connector_id}", response_model=ConnectorResponse)
@inject
async def get_connector(
    connector_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_service: ConnectorService = Depends(Provide[Container.connector_service]),
) -> ConnectorResponse:
    return await connector_service.get_connector(connector_id)


@router.post("/{connector_id}/test", response_model=ConnectorTestResponse)
@inject
async def test_connector(
    connector_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_sync_service: ConnectorSyncService = Depends(
        Provide[Container.connector_sync_service]
    ),
) -> ConnectorTestResponse:
    return await connector_sync_service.test_connection(
        organization_id=organization_id,
        connector_id=connector_id,
    )


@router.get("/{connector_id}/resources", response_model=ConnectorResourceListResponse)
@inject
async def list_connector_resources(
    connector_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_sync_service: ConnectorSyncService = Depends(
        Provide[Container.connector_sync_service]
    ),
) -> ConnectorResourceListResponse:
    return await connector_sync_service.list_resources(
        organization_id=organization_id,
        connector_id=connector_id,
    )


@router.post("/{connector_id}/sync", response_model=ConnectorSyncStartResponse)
@inject
async def sync_connector(
    connector_id: UUID,
    request: ConnectorSyncRequest,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_sync_service: ConnectorSyncService = Depends(
        Provide[Container.connector_sync_service]
    ),
) -> ConnectorSyncStartResponse:
    return await connector_sync_service.trigger_sync(
        organization_id=organization_id,
        connector_id=connector_id,
        user_id=current_user.id,
        request=request,
    )


@router.get("/{connector_id}/sync-state", response_model=ConnectorSyncStateListResponse)
@inject
async def get_connector_sync_state(
    connector_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_sync_service: ConnectorSyncService = Depends(
        Provide[Container.connector_sync_service]
    ),
) -> ConnectorSyncStateListResponse:
    return await connector_sync_service.list_sync_states(
        organization_id=organization_id,
        connector_id=connector_id,
    )


@router.get("/{connector_id}/sync-history", response_model=ConnectorSyncHistoryResponse)
@inject
async def get_connector_sync_history(
    connector_id: UUID,
    organization_id: UUID,
    limit: int = Query(default=20, ge=1, le=100),
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_sync_service: ConnectorSyncService = Depends(
        Provide[Container.connector_sync_service]
    ),
) -> ConnectorSyncHistoryResponse:
    return await connector_sync_service.list_sync_history(
        organization_id=organization_id,
        connector_id=connector_id,
        limit=limit,
    )


@router.get("/{connector_id}/source/schemas", response_model=ConnectorSourceSchemasResponse)
@inject
async def get_connector_schemas(
    connector_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_schema_service: ConnectorSchemaService = Depends(
        Provide[Container.connector_schema_service]
    ),
) -> ConnectorSourceSchemasResponse:
    schemas = await connector_schema_service.get_schemas(connector_id)
    return ConnectorSourceSchemasResponse(schemas=schemas)


@router.get(
    "/{connector_id}/source/schema/{schema}",
    response_model=ConnectorSourceSchemaResponse,
)
@inject
async def get_connector_tables(
    connector_id: UUID,
    schema: str,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_schema_service: ConnectorSchemaService = Depends(
        Provide[Container.connector_schema_service]
    ),
) -> ConnectorSourceSchemaResponse:
    tables = await connector_schema_service.get_tables(connector_id, schema)
    return ConnectorSourceSchemaResponse(schema=schema, tables=tables)


@router.get(
    "/{connector_id}/source/schema/{schema}/table/{table}/columns",
    response_model=ConnectorSourceSchemaTableResponse,
)
@inject
async def get_connector_table(
    connector_id: UUID,
    schema: str,
    table: str,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_schema_service: ConnectorSchemaService = Depends(
        Provide[Container.connector_schema_service]
    ),
) -> ConnectorSourceSchemaTableResponse:
    columns = await connector_schema_service.get_columns(connector_id, schema, table)
    return ConnectorSourceSchemaTableResponse(
        name=table,
        columns={
            column.name: ConnectorSourceSchemaColumnResponse(
                name=column.name,
                type=column.data_type,
                nullable=getattr(column, "nullable", None),
                primary_key=getattr(column, "primary_key", False),
            )
            for column in columns
        },
    )


@router.get(
    "/{connector_id}/catalog",
    response_model=ConnectorCatalogResponse,
)
@inject
async def get_connector_catalog(
    connector_id: UUID,
    organization_id: UUID,
    search: str | None = Query(default=None),
    include_schemas: list[str] = Query(default_factory=list),
    exclude_schemas: list[str] = Query(default_factory=list),
    include_system_schemas: bool = Query(default=False),
    include_columns: bool = Query(default=True),
    limit: int = Query(default=200, ge=1, le=1000),
    offset: int = Query(default=0, ge=0),
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_schema_service: ConnectorSchemaService = Depends(
        Provide[Container.connector_schema_service]
    ),
) -> ConnectorCatalogResponse:
    return await connector_schema_service.get_catalog(
        connector_id=connector_id,
        search=search,
        include_schemas=include_schemas,
        exclude_schemas=exclude_schemas,
        include_system_schemas=include_system_schemas,
        include_columns=include_columns,
        limit=limit,
        offset=offset,
    )


@router.get("/schemas/type", response_model=list[str])
@inject
async def list_connector_types(
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_service: ConnectorService = Depends(Provide[Container.connector_service]),
) -> list[str]:
    return connector_service.list_connector_types()


@router.get("/schema/{connector_type}", response_model=ConnectorConfigSchema)
@inject
async def get_connector_schema(
    connector_type: str,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_service: ConnectorService = Depends(Provide[Container.connector_service]),
) -> ConnectorConfigSchema:
    try:
        return connector_service.get_connector_config_schema(connector_type)
    except BusinessValidationError as exc:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST, detail=str(exc)
        ) from exc


@router.put("/{connector_id}", response_model=ConnectorResponse)
@inject
async def update_connector(
    connector_id: UUID,
    request: UpdateConnectorRequest,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_service: ConnectorService = Depends(Provide[Container.connector_service]),
) -> ConnectorResponse:
    return await connector_service.update_connector(connector_id, request)


@router.delete("/{connector_id}", status_code=status.HTTP_204_NO_CONTENT)
@inject
async def delete_connector(
    connector_id: UUID,
    organization_id: UUID,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    connector_service: ConnectorService = Depends(Provide[Container.connector_service]),
) -> None:
    await connector_service.delete_connector(connector_id)
    return None


@router.get("/", response_model=list[ConnectorResponse])
@inject
async def list_connectors(
    organization_id: UUID,
    project_id: Optional[UUID] = None,
    current_user: UserResponse = Depends(get_current_user),
    _org = Depends(get_organization),
    _proj = Depends(get_project),
    connector_service: ConnectorService = Depends(Provide[Container.connector_service]),
) -> list[ConnectorResponse]:
    if project_id:
        return await connector_service.list_project_connectors(project_id)
    return await connector_service.list_organization_connectors(organization_id)
