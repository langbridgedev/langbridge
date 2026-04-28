from typing import Any

from langbridge.connectors.base import SqlConnectorFactory, get_connector_config_factory
from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.runtime.models import ConnectorMetadata
from langbridge.runtime.providers import ConnectorMetadataProvider, CredentialProvider
from langbridge.runtime.services.errors import ExecutionValidationError


class SqlConnectorRuntimeFactory:
    """Resolves connector metadata, secrets, and SQL connector instances."""

    def __init__(
        self,
        *,
        connector_provider: ConnectorMetadataProvider,
        credential_provider: CredentialProvider,
        sql_connector_factory: SqlConnectorFactory | None = None,
        logger: Any = None,
    ) -> None:
        self._connector_provider = connector_provider
        self._credential_provider = credential_provider
        self._sql_connector_factory = sql_connector_factory or SqlConnectorFactory()
        self._logger = logger

    async def create_sql_connector(
        self,
        *,
        connector_type: ConnectorRuntimeType,
        connector_payload: dict[str, Any],
    ) -> Any:
        try:
            self._sql_connector_factory.get_sql_connector_class_reference(connector_type)
        except ValueError as exc:
            raise ExecutionValidationError(
                f"Connector type {connector_type.value} does not support SQL execution."
            ) from exc
        config_factory = get_connector_config_factory(connector_type)
        config_instance = config_factory.create(connector_payload.get("config", {}))
        sql_connector = self._sql_connector_factory.create_sql_connector(
            connector_type,
            config_instance,
            logger=self._logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    async def get_connector(
        self,
        *,
        connection_id,
        workspace_id,
    ) -> ConnectorMetadata | None:
        if self._connector_provider is not None:
            return await self._connector_provider.get_connector(
                workspace_id=workspace_id,
                connector_id=connection_id,
            )
        raise ExecutionValidationError("Connector metadata provider is required for SQL execution.")

    def resolve_connector_config(self, connector: ConnectorMetadata) -> dict[str, Any]:
        resolved_payload = dict(connector.config or {})
        runtime_config = dict(resolved_payload.get("config") or {})

        if connector.connection_metadata is not None:
            metadata = connector.connection_metadata.model_dump(exclude_none=True, by_alias=True)
            extra = metadata.pop("extra", {})
            for key, value in metadata.items():
                runtime_config.setdefault(key, value)
            if isinstance(extra, dict):
                for key, value in extra.items():
                    if value is not None:
                        runtime_config.setdefault(key, value)

        for secret_name, secret_ref in connector.secret_references.items():
            try:
                runtime_config[secret_name] = self._credential_provider.resolve_secret(secret_ref)
            except Exception as exc:  # pragma: no cover
                raise ExecutionValidationError(
                    f"Unable to resolve connector secret '{secret_name}'."
                ) from exc

        resolved_payload["config"] = runtime_config
        return resolved_payload

    def sqlglot_dialect_for_connector(self, connector_type: ConnectorRuntimeType) -> str:
        try:
            return SqlConnectorFactory.get_sqlglot_dialect(connector_type)
        except ValueError:
            return "tsql"
