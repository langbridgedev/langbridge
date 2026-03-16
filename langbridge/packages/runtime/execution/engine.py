from __future__ import annotations

from dataclasses import dataclass
from typing import Any, Protocol


@dataclass(slots=True, frozen=True)
class ExecutionResult:
    columns: list[str]
    rows: list[tuple[Any, ...]]
    rowcount: int
    elapsed_ms: int | None = None
    sql: str | None = None


class ExecutionEngine(Protocol):
    async def execute(
        self,
        sql: str,
        *,
        params: dict[str, Any] | None = None,
    ) -> ExecutionResult: ...

    def open_connection(self) -> Any: ...
