import json
import uuid
from typing import Optional, cast

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession

from langbridge.runtime.models import ConnectorMetadata
from langbridge.runtime.persistence.mappers import from_connector_record
from langbridge.runtime.persistence.db.connector import Connector
from langbridge.runtime.ports import IConnectorStore

from .base import AsyncBaseRepository


class ConnectorRepository(AsyncBaseRepository[Connector]):
    """Data access helper for connector entities."""

    def __init__(self, session: AsyncSession):
        super().__init__(session, Connector)

    def _select_with_relationships(self):
        return select(Connector)

    async def get_by_name(self, name: str) -> Connector | None:
        stmt = self._select_with_relationships().filter(Connector.name == name)
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def get_by_id(self, id_: object) -> Connector | None:
        stmt = self._select_with_relationships().filter(Connector.id == id_)
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def get_by_id_for_workspace(
        self,
        *,
        connector_id: uuid.UUID,
        workspace_id: uuid.UUID,
    ) -> Connector | None:
        stmt = (
            self._select_with_relationships()
            .where(
                Connector.id == connector_id,
                Connector.workspace_id == workspace_id,
            )
        )
        result = await self._session.scalars(stmt)
        return result.one_or_none()

    async def get_all(self) -> list[Connector]:
        stmt = self._select_with_relationships()
        result = await self._session.scalars(stmt)
        return list(result.all())

    async def get_by_ids(self, connector_ids: list[uuid.UUID]) -> list[Connector]:
        stmt = self._select_with_relationships().filter(Connector.id.in_(connector_ids))
        result = await self._session.scalars(stmt)
        return list(result.all())


class ConnectorStore(IConnectorStore):
    def __init__(self, repository: ConnectorRepository):
        self._repository = repository

    @staticmethod
    def _to_metadata(connector: Connector) -> ConnectorMetadata:
        raw_config = connector.config_json
        config: Optional[dict[str, object]] = None
        if isinstance(raw_config, (str, bytes)):
            try:
                parsed = json.loads(raw_config)
            except Exception:
                parsed = None
            if isinstance(parsed, dict):
                config = cast(dict[str, object], parsed)
        elif isinstance(raw_config, dict):
            config = raw_config

        return from_connector_record(
            connector,
        ).model_copy(
            update={
                "config": config,
                "version": "",
                "label": connector.name,
                "icon": "",
            }
        )

    async def get_by_name(self, name: str) -> ConnectorMetadata | None:
        connector = await self._repository.get_by_name(name)
        if connector is None:
            return None
        return self._to_metadata(connector)

    async def get_by_id(self, connector_id: uuid.UUID) -> ConnectorMetadata | None:
        connector = await self._repository.get_by_id(connector_id)
        if connector is None:
            return None
        return self._to_metadata(connector)

    async def get_by_ids(self, connector_ids: list[uuid.UUID]) -> list[ConnectorMetadata]:
        connectors = await self._repository.get_by_ids(connector_ids)
        return [self._to_metadata(connector) for connector in connectors]

    async def get_all(self) -> list[ConnectorMetadata]:
        connectors = await self._repository.get_all()
        return [self._to_metadata(connector) for connector in connectors]
