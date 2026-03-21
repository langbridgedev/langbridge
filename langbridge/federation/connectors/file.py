from __future__ import annotations

import logging
import time
from typing import Any

import duckdb
import pyarrow as pa

from langbridge.federation.utils import (
    resolve_local_storage_path,
)
from langbridge.federation.connectors.base import (
    RemoteExecutionResult,
    RemoteSource,
    SourceCapabilities,
)
from langbridge.federation.models.plans import SourceSubplan
from langbridge.federation.models.virtual_dataset import (
    TableStatistics,
    VirtualTableBinding,
)


class DuckDbFileRemoteSource(RemoteSource):
    def __init__(
        self,
        *,
        source_id: str,
        bindings: list[VirtualTableBinding],
        logger: logging.Logger | None = None,
    ) -> None:
        self.source_id = source_id
        self._bindings = {binding.table_key: binding for binding in bindings}
        self._logger = logger or logging.getLogger(__name__)

    def capabilities(self) -> SourceCapabilities:
        return SourceCapabilities(
            pushdown_filter=True,
            pushdown_projection=True,
            pushdown_aggregation=True,
            pushdown_limit=True,
            pushdown_join=False,
        )

    def dialect(self) -> str:
        return "duckdb"

    async def execute(self, subplan: SourceSubplan) -> RemoteExecutionResult:
        binding = self._require_binding(subplan.table_key)
        started = time.perf_counter()
        connection = duckdb.connect(database=":memory:")
        try:
            self._register_binding(connection=connection, binding=binding)
            result = connection.execute(subplan.sql)
            table = result.fetch_arrow_table()
            return RemoteExecutionResult(
                table=table if isinstance(table, pa.Table) else pa.table({}),
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )
        except Exception as e:
            self._logger.error("Error executing subplan on file source %s: %s", self.source_id, str(e), exc_info=True)
            raise
        finally:
            connection.close()

    async def estimate_table_stats(self, binding: VirtualTableBinding) -> TableStatistics:
        table_binding = self._require_binding(binding.table_key)
        if table_binding.stats is not None:
            return table_binding.stats

        connection = duckdb.connect(database=":memory:")
        try:
            self._register_binding(connection=connection, binding=table_binding)
            result = connection.execute(
                f"SELECT COUNT(*) AS row_count FROM {self._qualified_relation_name(table_binding)}"
            )
            rows = result.fetchall()
            row_count = float(rows[0][0]) if rows else None
            bytes_per_row = 128.0
            try:
                storage_uri = self._storage_uri_from_binding(table_binding)
                path = resolve_local_storage_path(storage_uri)
                if path.exists():
                    file_size = float(path.stat().st_size)
                    if row_count and row_count > 0:
                        bytes_per_row = max(1.0, file_size / row_count)
            except Exception:
                self._logger.debug("Unable to estimate bytes per row for source=%s", self.source_id)
            return TableStatistics(row_count_estimate=row_count, bytes_per_row=bytes_per_row)
        except Exception:
            self._logger.warning("Falling back to heuristic stats for file source %s", self.source_id)
            return TableStatistics(row_count_estimate=1_000_000.0, bytes_per_row=128.0)
        finally:
            connection.close()

    def _require_binding(self, table_key: str) -> VirtualTableBinding:
        binding = self._bindings.get(table_key)
        if binding is None and len(self._bindings) == 1:
            return next(iter(self._bindings.values()))
        if binding is None:
            raise KeyError(f"File source '{self.source_id}' has no binding for table '{table_key}'.")
        return binding

    def _register_binding(
        self,
        *,
        connection: duckdb.DuckDBPyConnection,
        binding: VirtualTableBinding,
    ) -> None:
        metadata = binding.metadata if isinstance(binding.metadata, dict) else {}
        storage_uri = self._storage_uri_from_binding(binding)
        file_format = str(metadata.get("file_format") or "").strip().lower()
        if file_format not in {"csv", "parquet"}:
            raise ValueError(f"Unsupported file format '{file_format}' for binding '{binding.table_key}'.")
        scan_sql = self._build_scan_sql(storage_uri=storage_uri, file_format=file_format, metadata=metadata)
        if binding.schema_name:
            connection.execute(
                f"CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(binding.schema_name)}"
            )
        connection.execute(
            f"CREATE OR REPLACE VIEW {self._qualified_relation_name(binding)} AS SELECT * FROM {scan_sql}"
        )

    @staticmethod
    def _storage_uri_from_binding(binding: VirtualTableBinding) -> str:
        metadata = binding.metadata if isinstance(binding.metadata, dict) else {}
        storage_uri = str(metadata.get("storage_uri") or "").strip()
        if not storage_uri:
            raise ValueError(f"File binding '{binding.table_key}' is missing storage_uri metadata.")
        return storage_uri

    @staticmethod
    def _build_scan_sql(*, storage_uri: str, file_format: str, metadata: dict[str, Any]) -> str:
        # path = resolve_local_storage_path(storage_uri).as_posix().replace("'", "''")
        path = storage_uri.replace("'", "''").replace("file:///app/", "")
        if file_format == "parquet":
            return f"read_parquet('{path}')"
        header = "true" if bool(metadata.get("header", True)) else "false"
        delimiter = str(metadata.get("delimiter") or ",").replace("'", "''")
        quote = str(metadata.get("quote") or '\"').replace("'", "''")
        return (
            "read_csv_auto("
            f"'{path}', "
            f"header={header}, "
            f"delim='{delimiter}', "
            f"quote='{quote}'"
            ")"
        )

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + str(identifier or "dataset_file").replace('"', '""') + '"'

    @classmethod
    def _qualified_relation_name(cls, binding: VirtualTableBinding) -> str:
        relation = cls._quote_identifier(binding.table)
        if binding.schema_name:
            return f"{cls._quote_identifier(binding.schema_name)}.{relation}"
        return relation
