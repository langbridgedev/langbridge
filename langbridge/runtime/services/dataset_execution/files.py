import uuid
from dataclasses import dataclass
from typing import Any

from langbridge.runtime.execution import DuckDbExecutionEngine, ExecutionEngine
from langbridge.runtime.utils.storage_uri import resolve_local_storage_path


@dataclass(frozen=True, slots=True)
class FileSchemaColumn:
    name: str
    data_type: str
    nullable: bool = True


class FileDatasetSqlBuilder:
    """Builds DuckDB file scan SQL and describes file-backed datasets."""

    def describe_file_source_schema(
        self,
        *,
        storage_uri: str,
        file_config: dict[str, Any] | None = None,
        execution_engine: ExecutionEngine | None = None,
    ) -> list[FileSchemaColumn]:
        source_sql = self.build_file_scan_sql(storage_uri=storage_uri, file_config=file_config)
        engine = execution_engine or DuckDbExecutionEngine()
        connection = engine.open_connection()
        try:
            describe_rows = connection.execute(f"DESCRIBE SELECT * FROM {source_sql}").fetchall()
        finally:
            connection.close()

        columns: list[FileSchemaColumn] = []
        for row in describe_rows:
            if len(row) < 2:
                continue
            name = str(row[0] or "").strip()
            data_type = str(row[1] or "unknown").strip() or "unknown"
            if not name:
                continue
            columns.append(
                FileSchemaColumn(
                    name=name,
                    data_type=data_type,
                    nullable=self.describe_nullable(row[2] if len(row) > 2 else None),
                )
            )
        return columns

    def build_file_scan_sql(
        self,
        *,
        storage_uri: str,
        file_config: dict[str, Any] | None = None,
    ) -> str:
        config_payload = dict(file_config or {})
        normalized_uri = resolve_local_storage_path(storage_uri).as_posix().replace("'", "''")
        configured = str(
            config_payload.get("format")
            or config_payload.get("file_format")
            or ""
        ).strip().lower()
        if configured == "parquet" or normalized_uri.lower().endswith(".parquet"):
            return f"read_parquet('{normalized_uri}')"
        header = "true" if bool(config_payload.get("header", True)) else "false"
        delimiter = str(config_payload.get("delimiter") or ",").replace("'", "''")
        quote = str(config_payload.get("quote") or '"').replace("'", "''")
        return (
            "read_csv_auto("
            f"'{normalized_uri}', "
            f"header={header}, "
            f"delim='{delimiter}', "
            f"quote='{quote}'"
            ")"
        )

    def synthetic_file_connector_id(self, dataset_id: uuid.UUID) -> uuid.UUID:
        return uuid.uuid5(uuid.NAMESPACE_DNS, f"langbridge-file-dataset:{dataset_id}")

    def describe_nullable(self, value: Any) -> bool:
        if isinstance(value, bool):
            return value
        normalized = str(value or "").strip().lower()
        if normalized in {"no", "false", "not null", "0"}:
            return False
        if normalized in {"yes", "true", "null", "nullable", "1"}:
            return True
        return True


_file_sql_builder = FileDatasetSqlBuilder()


def describe_file_source_schema(
    *,
    storage_uri: str,
    file_config: dict[str, Any] | None = None,
    execution_engine: ExecutionEngine | None = None,
) -> list[FileSchemaColumn]:
    return _file_sql_builder.describe_file_source_schema(
        storage_uri=storage_uri,
        file_config=file_config,
        execution_engine=execution_engine,
    )


def build_file_scan_sql(*, storage_uri: str, file_config: dict[str, Any] | None = None) -> str:
    return _file_sql_builder.build_file_scan_sql(storage_uri=storage_uri, file_config=file_config)


def synthetic_file_connector_id(dataset_id: uuid.UUID) -> uuid.UUID:
    return _file_sql_builder.synthetic_file_connector_id(dataset_id)
