from typing import Any


class DatasetExecutionResultParser:
    """Extracts stable summary values from execution-engine payloads."""

    def execution_meta(self, execution: dict[str, Any]) -> dict[str, int | None]:
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

    def single_numeric(
        self,
        execution: dict[str, Any],
        *,
        preferred_keys: list[str],
    ) -> int | None:
        rows_payload = execution.get("rows") or []
        if not isinstance(rows_payload, list) or not rows_payload:
            return None
        first_row = rows_payload[0]
        if not isinstance(first_row, dict):
            return None

        lowered = {str(key).lower(): value for key, value in first_row.items()}
        for key in preferred_keys:
            value = lowered.get(key)
            numeric = self._coerce_int(value)
            if numeric is not None:
                return numeric

        for value in first_row.values():
            numeric = self._coerce_int(value)
            if numeric is not None:
                return numeric
        return None

    def _coerce_int(self, value: Any) -> int | None:
        if isinstance(value, bool):
            return None
        if isinstance(value, (int, float)):
            return int(value)
        if isinstance(value, str) and value.strip().isdigit():
            return int(value.strip())
        return None
