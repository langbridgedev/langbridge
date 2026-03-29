
from collections import defaultdict

from langbridge.federation.models.virtual_dataset import TableStatistics


class StatsStore:
    """In-memory stats registry keyed by workspace/table for heuristic planning."""

    def __init__(self) -> None:
        self._stats: dict[str, dict[str, TableStatistics]] = defaultdict(dict)

    def get(self, *, workspace_id: str, table_key: str) -> TableStatistics | None:
        return self._stats.get(workspace_id, {}).get(table_key)

    def upsert(self, *, workspace_id: str, table_key: str, stats: TableStatistics) -> None:
        self._stats.setdefault(workspace_id, {})[table_key] = stats

    def apply_overrides(
        self,
        *,
        workspace_id: str,
        overrides: dict[str, TableStatistics],
    ) -> None:
        if not overrides:
            return
        table_stats = self._stats.setdefault(workspace_id, {})
        for table_key, stats in overrides.items():
            table_stats[table_key] = stats
