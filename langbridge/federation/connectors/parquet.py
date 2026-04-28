import asyncio
import logging
import re
import time
from typing import Any
from urllib.parse import urlparse

import duckdb
import pyarrow as pa

from langbridge.connectors.base import StorageConnector
from langbridge.federation.connectors.base import (
    RemoteExecutionResult,
    RemoteSource,
    SourceCapabilities,
)
from langbridge.federation.executor.offload import (
    FederationExecutionOffloader,
    run_federation_blocking,
)
from langbridge.federation.models.plans import SourceSubplan
from langbridge.federation.models.virtual_dataset import (
    TableStatistics,
    VirtualTableBinding,
)
from langbridge.federation.utils import resolve_local_storage_path

_OPTION_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_EXTENSION_NAME_RE = re.compile(r"^[A-Za-z_][A-Za-z0-9_]*$")
_REMOTE_URI_SCHEMES = {"http", "https", "s3", "s3a", "s3n", "gcs", "gs", "r2", "azure", "az", "abfs", "abfss"}


class DuckDbParquetRemoteSource(RemoteSource):
    def __init__(
        self,
        *,
        source_id: str,
        bindings: list[VirtualTableBinding],
        storage_connector: StorageConnector | None = None,
        logger: logging.Logger | None = None,
        blocking_executor: FederationExecutionOffloader | None = None,
    ) -> None:
        self.source_id = source_id
        self._bindings = {binding.table_key: binding for binding in bindings}
        self._storage_connector = storage_connector
        self._logger = logger or logging.getLogger(__name__)
        self._blocking_executor = blocking_executor

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
        try:
            table = await run_federation_blocking(
                self._blocking_executor,
                self._execute_subplan_blocking,
                binding,
                subplan.sql,
            )
            return RemoteExecutionResult(
                table=table,
                elapsed_ms=int((time.perf_counter() - started) * 1000),
            )
        except Exception as exc:
            self._logger.error(
                "Error executing subplan on parquet source %s: %s",
                self.source_id,
                str(exc),
                exc_info=True,
            )
            raise

    async def estimate_table_stats(self, binding: VirtualTableBinding) -> TableStatistics:
        table_binding = self._require_binding(binding.table_key)
        if table_binding.stats is not None:
            return table_binding.stats

        try:
            return await run_federation_blocking(
                self._blocking_executor,
                self._estimate_table_stats_blocking,
                table_binding,
            )
        except Exception:
            metadata = self._binding_metadata(table_binding)
            bytes_per_row = _coerce_float(metadata.get("bytes_per_row")) or 128.0
            self._logger.warning("Falling back to heuristic stats for parquet source %s", self.source_id)
            return TableStatistics(row_count_estimate=1_000_000.0, bytes_per_row=bytes_per_row)

    def _execute_subplan_blocking(self, binding: VirtualTableBinding, sql: str | None) -> pa.Table:
        connection = duckdb.connect(database=":memory:")
        try:
            asyncio.run(self._configure_connection(connection=connection, binding=binding))
            asyncio.run(self._register_binding(connection=connection, binding=binding))
            result = connection.execute(sql)
            table = result.fetch_arrow_table()
            return table if isinstance(table, pa.Table) else pa.table({})
        finally:
            connection.close()

    def _estimate_table_stats_blocking(self, binding: VirtualTableBinding) -> TableStatistics:
        metadata = self._binding_metadata(binding)
        bytes_per_row = _coerce_float(metadata.get("bytes_per_row")) or 128.0
        connection = duckdb.connect(database=":memory:")
        try:
            asyncio.run(self._configure_connection(connection=connection, binding=binding))
            asyncio.run(self._register_binding(connection=connection, binding=binding))
            result = connection.execute(
                f"SELECT COUNT(*) AS row_count FROM {self._qualified_relation_name(binding)}"
            )
            rows = result.fetchall()
            row_count = float(rows[0][0]) if rows else None
            estimated_bytes = _coerce_float(metadata.get("estimated_bytes") or metadata.get("file_size_bytes"))
            if estimated_bytes is not None and row_count and row_count > 0:
                bytes_per_row = max(1.0, estimated_bytes / row_count)
            return TableStatistics(row_count_estimate=row_count, bytes_per_row=bytes_per_row)
        finally:
            connection.close()

    def _require_binding(self, table_key: str) -> VirtualTableBinding:
        binding = self._bindings.get(table_key)
        if binding is None and len(self._bindings) == 1:
            return next(iter(self._bindings.values()))
        if binding is None:
            raise KeyError(f"Parquet source '{self.source_id}' has no binding for table '{table_key}'.")
        return binding

    async def _configure_connection(
        self,
        *,
        connection: duckdb.DuckDBPyConnection,
        binding: VirtualTableBinding,
    ) -> None:
        if self._storage_connector is not None:
            await self._storage_connector.configure_duckdb_connection(
                connection,
                storage_uris=self._storage_uris_from_binding(binding),
                options=self._binding_metadata(binding),
            )
        for extension_name in self._required_extensions(binding):
            self._load_extension(connection=connection, extension_name=extension_name)

    async def _register_binding(
        self,
        *,
        connection: duckdb.DuckDBPyConnection,
        binding: VirtualTableBinding,
    ) -> None:
        scan_sql = await self._build_scan_sql(binding=binding)
        if binding.schema_name:
            connection.execute(
                f"CREATE SCHEMA IF NOT EXISTS {self._quote_identifier(binding.schema_name)}"
            )
        connection.execute(
            f"CREATE OR REPLACE VIEW {self._qualified_relation_name(binding)} AS SELECT * FROM {scan_sql}"
        )

    async def _build_scan_sql(self, *, binding: VirtualTableBinding) -> str:
        metadata = self._binding_metadata(binding)
        storage_uris = self._storage_uris_from_binding(binding)
        normalized_uris = await self._resolve_scan_uris(
            storage_uris=storage_uris,
            metadata=metadata,
        )

        parquet_options = dict(metadata.get("parquet_options") or {})
        for option_name in ("union_by_name", "filename", "hive_partitioning"):
            if option_name in metadata and option_name not in parquet_options:
                parquet_options[option_name] = metadata[option_name]

        formatted_uris = self._format_uri_argument(normalized_uris)
        formatted_options = self._format_parquet_options(parquet_options)
        if formatted_options:
            return f"read_parquet({formatted_uris}, {formatted_options})"
        return f"read_parquet({formatted_uris})"

    async def _resolve_scan_uris(
        self,
        *,
        storage_uris: list[str],
        metadata: dict[str, Any],
    ) -> list[str]:
        if self._storage_connector is not None:
            resolved_uris = await self._storage_connector.resolve_duckdb_scan_uris(
                storage_uris,
                options=metadata,
            )
            normalized_uris = [
                str(storage_uri or "").strip()
                for storage_uri in resolved_uris
                if str(storage_uri or "").strip()
            ]
            if not normalized_uris:
                raise ValueError(
                    f"Storage connector resolved no scan URIs for parquet source '{self.source_id}'."
                )
            return normalized_uris
        return [self._normalize_scan_uri(storage_uri) for storage_uri in storage_uris]

    @classmethod
    def is_remote_binding(cls, binding: VirtualTableBinding) -> bool:
        metadata = cls._binding_metadata(binding)
        file_format = str(
            metadata.get("file_format")
            or metadata.get("format")
            or metadata.get("storage_kind")
            or ""
        ).strip().lower()
        if file_format != "parquet":
            return False
        storage_uris = cls._storage_uris_from_binding(binding)
        if len(storage_uris) > 1:
            return True
        return any(cls._is_remote_uri(storage_uri) for storage_uri in storage_uris)

    @classmethod
    def _required_extensions(cls, binding: VirtualTableBinding) -> list[str]:
        metadata = cls._binding_metadata(binding)
        requested_extensions = [
            str(extension_name or "").strip()
            for extension_name in metadata.get("duckdb_extensions", [])
            if str(extension_name or "").strip()
        ]
        if any(cls._is_remote_uri(storage_uri) for storage_uri in cls._storage_uris_from_binding(binding)):
            requested_extensions.insert(0, "httpfs")
        deduped_extensions: list[str] = []
        for extension_name in requested_extensions:
            if extension_name not in deduped_extensions:
                deduped_extensions.append(extension_name)
        return deduped_extensions

    @staticmethod
    def _load_extension(
        *,
        connection: duckdb.DuckDBPyConnection,
        extension_name: str,
    ) -> None:
        normalized_name = str(extension_name or "").strip().lower()
        if not normalized_name or _EXTENSION_NAME_RE.fullmatch(normalized_name) is None:
            raise ValueError(f"Invalid DuckDB extension name '{extension_name}'.")
        try:
            connection.execute(f"LOAD {normalized_name}")
        except Exception:
            connection.execute(f"INSTALL {normalized_name}")
            connection.execute(f"LOAD {normalized_name}")

    @staticmethod
    def _binding_metadata(binding: VirtualTableBinding) -> dict[str, Any]:
        return binding.metadata if isinstance(binding.metadata, dict) else {}

    @classmethod
    def _storage_uris_from_binding(cls, binding: VirtualTableBinding) -> list[str]:
        metadata = cls._binding_metadata(binding)
        raw_storage_uris = metadata.get("storage_uris")
        storage_uris: list[str] = []
        if isinstance(raw_storage_uris, list):
            storage_uris.extend(
                str(storage_uri or "").strip()
                for storage_uri in raw_storage_uris
                if str(storage_uri or "").strip()
            )
        raw_storage_uri = str(metadata.get("storage_uri") or "").strip()
        if raw_storage_uri:
            storage_uris.append(raw_storage_uri)
        if not storage_uris:
            raise ValueError(f"Parquet binding '{binding.table_key}' is missing storage_uri or storage_uris metadata.")
        return storage_uris

    @staticmethod
    def _normalize_scan_uri(storage_uri: str) -> str:
        if DuckDbParquetRemoteSource._is_remote_uri(storage_uri):
            return storage_uri
        return resolve_local_storage_path(storage_uri).as_posix()

    @staticmethod
    def _is_remote_uri(storage_uri: str) -> bool:
        scheme = urlparse(str(storage_uri or "").strip()).scheme.lower()
        return scheme in _REMOTE_URI_SCHEMES

    @staticmethod
    def _format_uri_argument(storage_uris: list[str]) -> str:
        if len(storage_uris) == 1:
            return _quote_literal(storage_uris[0])
        return "[" + ", ".join(_quote_literal(storage_uri) for storage_uri in storage_uris) + "]"

    @staticmethod
    def _format_parquet_options(options: dict[str, Any]) -> str:
        formatted_options: list[str] = []
        for option_name, option_value in options.items():
            normalized_name = str(option_name or "").strip()
            if not normalized_name:
                continue
            if _OPTION_NAME_RE.fullmatch(normalized_name) is None:
                raise ValueError(f"Invalid DuckDB parquet option '{option_name}'.")
            formatted_options.append(f"{normalized_name}={_format_duckdb_value(option_value)}")
        return ", ".join(formatted_options)

    @staticmethod
    def _quote_identifier(identifier: str) -> str:
        return '"' + str(identifier or "dataset_parquet").replace('"', '""') + '"'

    @classmethod
    def _qualified_relation_name(cls, binding: VirtualTableBinding) -> str:
        relation = cls._quote_identifier(binding.table)
        if binding.schema_name:
            return f"{cls._quote_identifier(binding.schema_name)}.{relation}"
        return relation


def _quote_literal(value: str) -> str:
    return "'" + str(value or "").replace("'", "''") + "'"


def _format_duckdb_value(value: Any) -> str:
    if isinstance(value, bool):
        return "true" if value else "false"
    if value is None:
        return "NULL"
    if isinstance(value, (int, float)):
        return str(value)
    return _quote_literal(str(value))


def _coerce_float(value: Any) -> float | None:
    if value in {None, ""}:
        return None
    try:
        return float(value)
    except (TypeError, ValueError):
        return None
