from typing import Any

from langbridge.runtime.services.errors import ExecutionValidationError


class SqlExecutionResultParser:
    """Normalizes federated execution payloads into SQL job result shapes."""

    def extract_columns(
        self,
        execution: dict[str, Any],
        rows: list[dict[str, Any]],
    ) -> list[dict[str, Any]]:
        raw_columns = execution.get("columns") if isinstance(execution, dict) else []
        columns = []
        if isinstance(raw_columns, list):
            columns = [str(column) for column in raw_columns if str(column).strip()]
        if not columns and rows:
            columns = [str(column) for column in rows[0].keys()]
        return [{"name": column, "type": None} for column in columns]

    def extract_rows(self, execution: dict[str, Any]) -> list[dict[str, Any]]:
        rows_payload = execution.get("rows") if isinstance(execution, dict) else []
        if rows_payload is None:
            return []
        if not isinstance(rows_payload, list):
            raise ExecutionValidationError("Federated SQL execution returned an invalid rows payload.")

        columns_payload = execution.get("columns") if isinstance(execution, dict) else []
        columns: list[str] = []
        if isinstance(columns_payload, list):
            columns = [str(column) for column in columns_payload if str(column).strip()]

        rows: list[dict[str, Any]] = []
        for row in rows_payload:
            if isinstance(row, dict):
                if columns:
                    rows.append({column: row.get(column) for column in columns})
                else:
                    rows.append({str(key): value for key, value in row.items()})
                continue
            if isinstance(row, (list, tuple)):
                if not columns:
                    columns = [f"column_{index + 1}" for index in range(len(row))]
                rows.append(
                    {
                        columns[index] if index < len(columns) else f"column_{index + 1}": value
                        for index, value in enumerate(row)
                    }
                )
                continue
            if not columns:
                columns = ["value"]
            rows.append({columns[0]: row})
        return rows

    def extract_meta(self, execution: dict[str, Any]) -> dict[str, int | None]:
        execution_payload = execution.get("execution") if isinstance(execution, dict) else {}
        if not isinstance(execution_payload, dict):
            return {"duration_ms": None, "bytes_scanned": None}
        total_runtime = execution_payload.get("total_runtime_ms")
        duration_ms = int(total_runtime) if isinstance(total_runtime, (int, float)) else None
        bytes_scanned = 0
        has_bytes = False
        for metric in execution_payload.get("stage_metrics") or []:
            if not isinstance(metric, dict):
                continue
            value = metric.get("bytes_written")
            if isinstance(value, (int, float)):
                bytes_scanned += int(value)
                has_bytes = True
        return {
            "duration_ms": duration_ms,
            "bytes_scanned": bytes_scanned if has_bytes else None,
        }
