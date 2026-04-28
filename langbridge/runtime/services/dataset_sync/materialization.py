import sys
import uuid
from pathlib import Path
from typing import Any

import pyarrow as pa
import pyarrow.parquet as pq

from langbridge.runtime.models.state import ConnectorSyncMode
from langbridge.runtime.services.dataset_sync.sources import enum_value
from langbridge.runtime.settings import runtime_settings as settings


class DatasetMaterializer:
    """Handles parquet storage, row merging, cursor selection, and Arrow coercion."""

    def dataset_parquet_path(
        self,
        *,
        workspace_id: uuid.UUID,
        connection_id: uuid.UUID,
        dataset_name: str,
    ) -> Path:
        return (
            Path(settings.DATASET_FILE_LOCAL_DIR)
            / "materialized"
            / str(workspace_id)
            / str(connection_id)
            / f"{dataset_name}.parquet"
        )

    def read_existing_rows(self, path: Path) -> tuple[list[dict[str, Any]], pa.Schema | None]:
        if not path.exists():
            return [], None
        table = pq.read_table(path)
        return table.to_pylist(), table.schema

    def merge_rows(
        self,
        *,
        existing_rows: list[dict[str, Any]],
        new_rows: list[dict[str, Any]],
        primary_key: str | None,
        full_refresh: bool,
    ) -> list[dict[str, Any]]:
        if full_refresh:
            return list(new_rows)
        if not primary_key:
            return [*existing_rows, *new_rows]

        merged: dict[str, dict[str, Any]] = {}
        extras: list[dict[str, Any]] = []
        for row in existing_rows:
            key = self.row_identity(row, primary_key)
            if key is None:
                extras.append(dict(row))
            else:
                merged[key] = dict(row)
        for row in new_rows:
            key = self.row_identity(row, primary_key)
            if key is None:
                extras.append(dict(row))
            else:
                merged[key] = dict(row)
        return [*merged.values(), *extras]

    def rows_to_table(
        self,
        *,
        rows: list[dict[str, Any]],
        existing_schema: pa.Schema | None,
    ) -> pa.Table:
        self.ensure_pyarrow_compatible_pandas_stub()
        normalized_rows = self.normalize_rows_for_arrow(rows)
        if normalized_rows:
            try:
                return pa.Table.from_pylist(normalized_rows)
            except (pa.ArrowInvalid, pa.ArrowTypeError):
                stringified_rows = [
                    {key: (None if value is None else str(value)) for key, value in row.items()}
                    for row in normalized_rows
                ]
                return pa.Table.from_pylist(stringified_rows)
        if existing_schema is not None:
            return pa.Table.from_arrays(
                [pa.array([], type=field.type) for field in existing_schema],
                schema=existing_schema,
            )
        return pa.table({})

    def ensure_pyarrow_compatible_pandas_stub(self) -> None:
        pandas_module = sys.modules.get("pandas")
        if pandas_module is not None and not hasattr(pandas_module, "__version__"):
            setattr(pandas_module, "__version__", "0.0.0")

    def normalize_rows_for_arrow(self, rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
        columns: set[str] = set()
        for row in rows:
            columns.update(str(key) for key in row.keys())
        ordered_columns = sorted(columns)

        category_map: dict[str, set[str]] = {column: set() for column in ordered_columns}
        for row in rows:
            for column in ordered_columns:
                value = row.get(column)
                category = self.value_category(value)
                if category is not None:
                    category_map[column].add(category)

        normalized: list[dict[str, Any]] = []
        for row in rows:
            normalized_row: dict[str, Any] = {}
            for column in ordered_columns:
                value = row.get(column)
                categories = category_map[column]
                if value is None:
                    normalized_row[column] = None
                elif categories <= {"int"}:
                    normalized_row[column] = int(value)
                elif categories <= {"int", "float"}:
                    normalized_row[column] = float(value)
                elif categories <= {"bool"}:
                    normalized_row[column] = bool(value)
                else:
                    normalized_row[column] = str(value)
            normalized.append(normalized_row)
        return normalized

    def value_category(self, value: Any) -> str | None:
        if value is None:
            return None
        if isinstance(value, bool):
            return "bool"
        if isinstance(value, int):
            return "int"
        if isinstance(value, float):
            return "float"
        return "string"

    def row_identity(self, row: dict[str, Any], primary_key: str) -> str | None:
        if primary_key == "_parent_id":
            parent_id = row.get("_parent_id")
            if parent_id is None or str(parent_id).strip() == "":
                return None
            return str(parent_id)
        if primary_key == "_child_identity":
            parent_id = row.get("_parent_id")
            child_index = row.get("_child_index")
            if parent_id is None or child_index is None:
                return None
            return f"{parent_id}:{child_index}"
        value = row.get(primary_key)
        if value is not None and str(value).strip():
            return str(value)
        return None

    def child_primary_key(self, rows: list[dict[str, Any]]) -> str | None:
        if any("id" in row for row in rows):
            return "id"
        if any("_parent_id" in row for row in rows) and any("_child_index" in row for row in rows):
            return "_child_identity"
        if any("_parent_id" in row for row in rows):
            return "_parent_id"
        return None

    def materialization_primary_key(
        self,
        *,
        resource_path: str,
        root_resource_name: str,
        root_primary_key: str | None,
        rows: list[dict[str, Any]],
    ) -> str | None:
        if resource_path == root_resource_name:
            return root_primary_key
        return self.child_primary_key(rows)

    def pick_newer_cursor(self, current: str | None, candidate: str | None) -> str | None:
        if not candidate:
            return current
        if not current:
            return candidate
        if current.isdigit() and candidate.isdigit():
            return str(max(int(current), int(candidate)))
        return max(current, candidate)

    def resolve_next_sql_cursor(
        self,
        *,
        rows: list[dict[str, Any]],
        cursor_field: str | None,
        current_cursor: str | None,
        sync_mode: ConnectorSyncMode,
    ) -> str | None:
        if sync_mode != ConnectorSyncMode.INCREMENTAL or not cursor_field:
            return current_cursor
        next_cursor = current_cursor
        for row in rows:
            value = row.get(cursor_field)
            if value is None or str(value).strip() == "":
                continue
            next_cursor = self.pick_newer_cursor(next_cursor, str(value))
        return next_cursor

    def sql_literal(self, value: Any) -> str:
        if value is None:
            return "NULL"
        if isinstance(value, bool):
            return "TRUE" if value else "FALSE"
        if isinstance(value, (int, float)):
            return str(value)
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    def describe_schema_drift(
        self,
        *,
        existing_schema: pa.Schema | None,
        next_schema: pa.Schema,
    ) -> dict[str, Any] | None:
        if existing_schema is None:
            return None

        previous_fields = {field.name: str(field.type) for field in existing_schema}
        next_fields = {field.name: str(field.type) for field in next_schema}

        added_columns = sorted(name for name in next_fields if name not in previous_fields)
        removed_columns = sorted(name for name in previous_fields if name not in next_fields)
        type_changes = [
            {
                "column": name,
                "before": previous_fields[name],
                "after": next_fields[name],
            }
            for name in sorted(previous_fields.keys() & next_fields.keys())
            if previous_fields[name] != next_fields[name]
        ]
        if not added_columns and not removed_columns and not type_changes:
            return None
        return {
            "added_columns": added_columns,
            "removed_columns": removed_columns,
            "type_changes": type_changes,
        }

    def sync_mode_label(self, sync_mode: ConnectorSyncMode) -> str:
        return enum_value(sync_mode).replace("_", " ").title()
