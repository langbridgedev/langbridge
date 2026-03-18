
from typing import Generic, TypeVar

from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.orm import Session

from langbridge.packages.common.langbridge_common.db.base import Base


ModelT = TypeVar("ModelT", bound=Base)


FILTER_OPERATOR_MAPPING = {
    "=": lambda field, value: field == value,
    "!=": lambda field, value: field != value,
    "<": lambda field, value: field < value,
    "<=": lambda field, value: field <= value,
    ">": lambda field, value: field > value,
    ">=": lambda field, value: field >= value,
}

class ModelFilter:
    def __init__(self, field: str, operator: str, value: object):
        self.field = field
        self.operator = operator
        self.value = value

class BaseRepository(Generic[ModelT]):
    def __init__(self, session: Session, model: type[ModelT]):
        self._session = session
        self._model = model

    def add(self, instance: ModelT) -> ModelT:
        self._session.add(instance)
        return instance

    def delete(self, instance: ModelT) -> None:
        self._session.delete(instance)

    def get_by_id(self, id_: object) -> ModelT | None:
        return self._session.get(self._model, id_)

    def get_all(self) -> list[ModelT]:
        return list(self._session.scalars(select(self._model)).all())

    def save(self, instance: ModelT) -> ModelT:
        return self._session.merge(instance)


class AsyncBaseRepository(Generic[ModelT]):
    def __init__(self, session: AsyncSession, model: type[ModelT]):
        self._session = session
        self._model = model

    def add(self, instance: ModelT) -> ModelT:
        """Add instance to the session; caller manages flush/commit."""
        self._session.add(instance)
        return instance

    async def delete(self, instance: ModelT) -> None:
        await self._session.delete(instance)

    async def get_by_id(self, id_: object) -> ModelT | None:
        return await self._session.get(self._model, id_)

    async def get_all(self) -> list[ModelT]:
        result = await self._session.scalars(select(self._model))
        return list(result.all())

    async def save(self, instance: ModelT) -> ModelT:
        return await self._session.merge(instance)

    async def commit(self) -> None:
        await self._session.commit()

    async def flush(self) -> None:
        await self._session.flush()

    async def search(
        self,
        filters: list[ModelFilter],
        limit: int = 10,
        offset: int = 0,
    ) -> list[ModelT]:
        base_query = select(self._model)
        for filter in filters:
            if filter.operator not in FILTER_OPERATOR_MAPPING.keys():
                raise ValueError(f"Invalid operator: {filter.operator}")
            base_query = base_query.where(FILTER_OPERATOR_MAPPING[filter.operator](getattr(self._model, filter.field), filter.value))
        result = await self._session.scalars(base_query.limit(limit).offset(offset))
        return list(result.all())
