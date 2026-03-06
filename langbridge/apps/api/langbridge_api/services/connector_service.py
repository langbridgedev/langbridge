import json
import logging
import uuid
from copy import deepcopy
from typing import Any, Dict, Optional, Type

from langbridge.packages.connectors.langbridge_connectors.api import (
    ApiConnector,
    ApiConnectorFactory,
    BaseConnectorConfig,
    BaseConnectorConfigFactory,
    BaseConnectorConfigSchemaFactory,
    ConnectorConfigSchema,
    ConnectorFamily as RegistryConnectorFamily,
    ConnectorPluginMetadata as RegistryConnectorPluginMetadata,
    ConnectorRuntimeType,
    ConnectorRuntimeTypeSqlDialectMap,
    ConnectorRuntimeTypeVectorDBMap,
    SqlConnector,
    SqlConnectorFactory,
    VecotorDBConnector,
    VectorDBConnectorFactory,
    get_connector_plugin,
    get_connector_config_factory,
    get_connector_config_schema_factory,
)
from langbridge.packages.common.langbridge_common.db.auth import Organization
from langbridge.packages.common.langbridge_common.db.connector import (
    APIConnector as ApiConnectorRecord,
    DatabaseConnector,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import BusinessValidationError
from langbridge.packages.common.langbridge_common.contracts.connectors import (
    ConnectorAuthSchemaField,
    ConnectorFamily,
    ConnectorPluginMetadata,
    ConnectorResponse,
    ConnectorSyncStrategy,
    CreateConnectorRequest,
    UpdateConnectorRequest,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import ConnectorRepository
from langbridge.packages.common.langbridge_common.repositories.organization_repository import (
    OrganizationRepository,
    ProjectRepository,
)


class ConnectorService:
    """Domain logic for managing connectors."""

    def __init__(
        self,
        connector_repository: ConnectorRepository,
        organization_repository: OrganizationRepository,
        project_repository: ProjectRepository,
    ) -> None:
        self._connector_repository = connector_repository
        self._organization_repository = organization_repository
        self._project_repository = project_repository
        self._sql_connector_factory = SqlConnectorFactory()
        self._api_connector_factory = ApiConnectorFactory()
        self._vector_connector_factory = VectorDBConnectorFactory()
        self._logger = logging.getLogger(__name__)

    @staticmethod
    def _normalize_runtime_type(
        connector_type: str | ConnectorRuntimeType | None,
    ) -> ConnectorRuntimeType | None:
        if connector_type is None:
            return None
        if isinstance(connector_type, ConnectorRuntimeType):
            return connector_type
        try:
            return ConnectorRuntimeType(str(connector_type).upper())
        except ValueError:
            return None

    def _build_response_plugin_metadata(
        self,
        connector_type: str | ConnectorRuntimeType | None,
    ) -> ConnectorPluginMetadata | None:
        runtime_type = self._normalize_runtime_type(connector_type)
        if runtime_type is None:
            return None

        plugin = get_connector_plugin(runtime_type)
        if plugin is not None:
            return ConnectorPluginMetadata(
                connector_type=runtime_type.value,
                connector_family=ConnectorFamily(plugin.connector_family.value),
                supported_resources=list(plugin.supported_resources),
                auth_schema=[
                    ConnectorAuthSchemaField(
                        field=field.field,
                        label=field.label,
                        required=field.required,
                        description=field.description,
                        type=field.type,
                        secret=field.secret,
                        default=field.default,
                        value_list=list(field.value_list or []),
                    )
                    for field in plugin.auth_schema
                ],
                sync_strategy=(
                    ConnectorSyncStrategy(plugin.sync_strategy.value)
                    if plugin.sync_strategy is not None
                    else None
                ),
            )

        if runtime_type in ConnectorRuntimeTypeSqlDialectMap:
            return ConnectorPluginMetadata(
                connector_type=runtime_type.value,
                connector_family=ConnectorFamily.DATABASE,
            )
        if runtime_type in ConnectorRuntimeTypeVectorDBMap:
            return ConnectorPluginMetadata(
                connector_type=runtime_type.value,
                connector_family=ConnectorFamily.VECTOR_DB,
            )
        return None

    def _build_schema_plugin_metadata(
        self,
        connector_type: ConnectorRuntimeType,
    ) -> RegistryConnectorPluginMetadata | None:
        plugin = get_connector_plugin(connector_type)
        if plugin is not None:
            return RegistryConnectorPluginMetadata(
                connector_type=connector_type.value,
                connector_family=plugin.connector_family,
                supported_resources=list(plugin.supported_resources),
                auth_schema=list(plugin.auth_schema),
                sync_strategy=plugin.sync_strategy,
            )

        if connector_type in ConnectorRuntimeTypeSqlDialectMap:
            return RegistryConnectorPluginMetadata(
                connector_type=connector_type.value,
                connector_family=RegistryConnectorFamily.DATABASE,
            )
        if connector_type in ConnectorRuntimeTypeVectorDBMap:
            return RegistryConnectorPluginMetadata(
                connector_type=connector_type.value,
                connector_family=RegistryConnectorFamily.VECTOR_DB,
            )
        return None

    def _to_connector_response(
        self,
        connector: Any,
        *,
        organization_id: uuid.UUID | None = None,
        project_id: uuid.UUID | None = None,
    ) -> ConnectorResponse:
        return ConnectorResponse.from_connector(
            connector,
            organization_id=organization_id,
            project_id=project_id,
            plugin_metadata=self._build_response_plugin_metadata(
                getattr(connector, "connector_type", None)
            ),
        )

    def _connector_family(
        self,
        connector_type: ConnectorRuntimeType,
    ) -> RegistryConnectorFamily | None:
        plugin = get_connector_plugin(connector_type)
        if plugin is not None:
            return plugin.connector_family
        if connector_type in ConnectorRuntimeTypeSqlDialectMap:
            return RegistryConnectorFamily.DATABASE
        if connector_type in ConnectorRuntimeTypeVectorDBMap:
            return RegistryConnectorFamily.VECTOR_DB
        return None

    def list_connector_plugins(self) -> list[ConnectorPluginMetadata]:
        metadata: list[ConnectorPluginMetadata] = []
        for connector_type in ConnectorRuntimeType:
            plugin_metadata = self._build_response_plugin_metadata(connector_type)
            if plugin_metadata is not None:
                metadata.append(plugin_metadata)
        return metadata

    async def list_organization_connectors(
        self,
        organization_id: uuid.UUID,
    ) -> list[ConnectorResponse]:
        organization = await self._organization_repository.get_by_id(organization_id)
        if not organization:
            raise BusinessValidationError("Organization not found")
        return [
            self._to_connector_response(connector, organization_id=organization_id)
            for connector in organization.connectors
        ]

    async def list_all_connectors(self) -> list[ConnectorResponse]:
        connectors = await self._connector_repository.get_all()
        return [self._to_connector_response(connector) for connector in connectors]

    async def list_project_connectors(self, project_id: uuid.UUID) -> list[ConnectorResponse]:
        project = await self._project_repository.get_by_id(project_id)
        if not project:
            raise BusinessValidationError("Project not found")
        return [
            self._to_connector_response(
                connector,
                organization_id=project.organization_id,
                project_id=project_id,
            )
            for connector in project.connectors
        ]

    def list_connector_types(self) -> list[str]:
        return [ct.value for ct in ConnectorRuntimeType]

    def get_connector_config_schema(self, connector_type: str) -> ConnectorConfigSchema:
        try:
            connector_type_enum = ConnectorRuntimeType(connector_type.upper())
            factory: Type[BaseConnectorConfigSchemaFactory] = (
                get_connector_config_schema_factory(connector_type_enum)
            )
            schema = factory.create({})
            if schema.plugin_metadata is None:
                schema.plugin_metadata = self._build_schema_plugin_metadata(
                    connector_type_enum
                )
            return schema
        except ValueError as exc:
            raise BusinessValidationError(str(exc)) from exc

    @staticmethod
    def _build_config_instance(
        connector_type: ConnectorRuntimeType,
        connector_config: Dict[str, Any],
    ) -> BaseConnectorConfig:
        config_factory: Type[BaseConnectorConfigFactory] = get_connector_config_factory(
            connector_type
        )
        return config_factory.create(connector_config["config"])

    async def _validate_connector_config(
        self,
        connector_type: ConnectorRuntimeType,
        connector_config: Dict[str, Any],
    ) -> None:
        connector_family = self._connector_family(connector_type)
        if connector_family == RegistryConnectorFamily.API:
            await self.async_create_api_connector(connector_type, connector_config)
            return
        if connector_type in ConnectorRuntimeTypeSqlDialectMap:
            await self.async_create_sql_connector(connector_type, connector_config)
            return
        if connector_type in ConnectorRuntimeTypeVectorDBMap:
            await self.async_create_vector_connector(connector_type, connector_config)
            return
        raise BusinessValidationError(f"Unsupported connector type: {connector_type.value}")

    async def create_connector(self, create_request: CreateConnectorRequest) -> ConnectorResponse:
        connector_type = ConnectorRuntimeType(create_request.connector_type.upper())
        connector_family = self._connector_family(connector_type)

        if not getattr(create_request, "config", None):
            raise BusinessValidationError("Connector config must be provided")

        config_payload = deepcopy(create_request.config or {})
        if isinstance(config_payload.get("config"), dict):
            for secret_name in create_request.secret_references.keys():
                config_payload["config"].pop(secret_name, None)
        config_json = json.dumps(config_payload)

        # Hosted mode can still validate live credentials. Runtime secret-ref mode cannot.
        if not create_request.secret_references:
            try:
                await self._validate_connector_config(connector_type, create_request.config)
            except Exception as exc:
                raise BusinessValidationError(str(exc)) from exc

        connector_record_class: Type[DatabaseConnector] | Type[ApiConnectorRecord]
        connector_record_class = DatabaseConnector
        connector_record_type = "database_connector"
        if connector_family == RegistryConnectorFamily.API:
            connector_record_class = ApiConnectorRecord
            connector_record_type = "api_connector"

        connector = connector_record_class(
            id=uuid.uuid4(),
            name=create_request.name,
            type=connector_record_type,
            connector_type=connector_type.value,
            config_json=config_json,
            description=create_request.description,
            connection_metadata_json=(
                create_request.connection_metadata.model_dump(mode="json")
                if create_request.connection_metadata
                else None
            ),
            secret_references_json={
                key: value.model_dump(mode="json")
                for key, value in create_request.secret_references.items()
            }
            if create_request.secret_references
            else None,
            access_policy_json=(
                create_request.connection_policy.model_dump(mode="json")
                if create_request.connection_policy
                else None
            ),
        )

        self._connector_repository.add(connector)

        if create_request.organization_id is None and create_request.project_id is None:
            raise BusinessValidationError(
                "Either organization_id or project_id must be provided"
            )

        organization: Optional[Organization] = None
        if create_request.organization_id is not None:
            organization = await self._organization_repository.get_by_id(
                create_request.organization_id
            )
            if not organization:
                raise BusinessValidationError("Organization not found")
        organization.connectors.append(connector)

        if create_request.project_id:
            project = await self._project_repository.get_by_id(create_request.project_id)
            if not project:
                raise BusinessValidationError("Project not found")

            if (
                create_request.organization_id
                and project.organization_id != create_request.organization_id
            ):
                raise BusinessValidationError(
                    "Project does not belong to the specified organization"
                )

            project.connectors.append(connector)

            if organization is None:
                organization = await self._organization_repository.get_by_id(
                    project.organization_id
                )
                if not organization:
                    raise BusinessValidationError("Organization not found")
                organization.connectors.append(connector)
                # await self._organization_repository.commit()

        return self._to_connector_response(
            connector,
            organization_id=create_request.organization_id,
            project_id=create_request.project_id,
        )

    async def get_connector(self, connector_id: uuid.UUID) -> ConnectorResponse:
        connector = await self._connector_repository.get_by_id(connector_id)
        if not connector:
            raise BusinessValidationError("Connector not found")
        return self._to_connector_response(connector)

    async def update_connector(
        self,
        connector_id: str,
        update_request: UpdateConnectorRequest,
    ) -> ConnectorResponse:
        connector_entity = await self._connector_repository.get_by_id(connector_id)
        if not connector_entity:
            raise BusinessValidationError("Connector not found")
        if update_request.name is not None:
            connector_entity.name = update_request.name
        if update_request.description is not None:
            connector_entity.description = update_request.description
        if update_request.connector_type is not None:
            connector_entity.connector_type = update_request.connector_type
        if update_request.config is not None:
            connector_entity.config_json = json.dumps(update_request.config)
        if update_request.connection_metadata is not None:
            connector_entity.connection_metadata_json = update_request.connection_metadata.model_dump(
                mode="json"
            )
        if update_request.secret_references is not None:
            connector_entity.secret_references_json = {
                key: value.model_dump(mode="json")
                for key, value in update_request.secret_references.items()
            }
        if update_request.connection_policy is not None:
            connector_entity.access_policy_json = update_request.connection_policy.model_dump(
                mode="json"
            )
        return self._to_connector_response(
            connector_entity,
            organization_id=update_request.organization_id,
            project_id=update_request.project_id,
        )

    async def delete_connector(self, connector_id: uuid.UUID) -> None:
        connector = await self._connector_repository.get_by_id(connector_id)
        if not connector:
            raise BusinessValidationError("Connector not found")
        await self._connector_repository.delete(connector)

    async def create_sql_connector(
        self,
        connector_type: ConnectorRuntimeType,
        connector_config: Dict[str, Any],
    ) -> SqlConnector:
        dialect = ConnectorRuntimeTypeSqlDialectMap.get(connector_type)
        if dialect is None:
            raise BusinessValidationError(
                f"Connector type {connector_type.value} does not support SQL operations."
            )
        config_instance = self._build_config_instance(connector_type, connector_config)
        sql_connector = self._sql_connector_factory.create_sql_connector(
            dialect,
            config_instance,
            logger=self._logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    async def async_create_sql_connector(
        self,
        connector_type: ConnectorRuntimeType,
        connector_config: Dict[str, Any],
    ) -> SqlConnector:
        dialect = ConnectorRuntimeTypeSqlDialectMap.get(connector_type)
        if dialect is None:
            raise BusinessValidationError(
                f"Connector type {connector_type.value} does not support SQL operations."
            )
        config_instance = self._build_config_instance(connector_type, connector_config)
        sql_connector = self._sql_connector_factory.create_sql_connector(
            dialect,
            config_instance,
            logger=self._logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    async def create_api_connector(
        self,
        connector_type: ConnectorRuntimeType,
        connector_config: Dict[str, Any],
    ) -> ApiConnector:
        return await self.async_create_api_connector(connector_type, connector_config)

    async def async_create_api_connector(
        self,
        connector_type: ConnectorRuntimeType,
        connector_config: Dict[str, Any],
    ) -> ApiConnector:
        plugin = get_connector_plugin(connector_type)
        if plugin is None or plugin.connector_family != RegistryConnectorFamily.API:
            raise BusinessValidationError(
                f"Connector type {connector_type.value} is not configured as an API connector."
            )
        config_instance = self._build_config_instance(connector_type, connector_config)
        api_connector = self._api_connector_factory.create_api_connector(
            connector_type,
            config_instance,
            logger=self._logger,
        )
        await api_connector.test_connection()
        return api_connector

    async def create_vector_connector(
        self,
        connector_type: ConnectorRuntimeType,
        connector_config: Dict[str, Any],
    ) -> VecotorDBConnector:
        return await self.async_create_vector_connector(connector_type, connector_config)

    async def async_create_vector_connector(
        self,
        connector_type: ConnectorRuntimeType,
        connector_config: Dict[str, Any],
    ) -> VecotorDBConnector:
        vector_type = ConnectorRuntimeTypeVectorDBMap.get(connector_type)
        if vector_type is None:
            raise BusinessValidationError(
                f"Connector type {connector_type.value} is not configured as a vector database."
            )
        config_instance = self._build_config_instance(connector_type, connector_config)
        vector_connector = self._vector_connector_factory.create_vector_connector(
            vector_type,
            config_instance,
            logger=self._logger,
        )
        await vector_connector.test_connection()
        return vector_connector
