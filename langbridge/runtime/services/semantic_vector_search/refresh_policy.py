import re
import uuid
from datetime import datetime, timedelta, timezone

from langbridge.runtime.models import SemanticVectorIndexMetadata
from langbridge.runtime.services.errors import ExecutionValidationError

_INTERVAL_PART_RE = re.compile(r"(?P<count>\d+)\s*(?P<unit>[smhdw])", re.IGNORECASE)
_SAFE_INDEX_RE = re.compile(r"[^a-zA-Z0-9_]+")


class SemanticVectorIndexRefreshPolicy:
    """Normalizes vector index names and decides refresh eligibility."""

    def normalize_timestamp(self, value: datetime | None) -> datetime | None:
        if value is None:
            return None
        if value.tzinfo is None or value.utcoffset() is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def build_index_name(
        self,
        *,
        workspace_id: uuid.UUID,
        semantic_model_id: uuid.UUID,
        dataset_key: str,
        dimension_name: str,
        configured_name: str | None,
    ) -> str:
        candidate = str(configured_name or "").strip()
        if not candidate:
            candidate = (
                f"semantic_{workspace_id.hex[:8]}_{semantic_model_id.hex[:8]}_"
                f"{dataset_key}_{dimension_name}"
            )
        candidate = _SAFE_INDEX_RE.sub("_", candidate).strip("_").lower()
        return candidate or f"semantic_{semantic_model_id.hex[:12]}"

    def parse_refresh_interval(self, value: str | None) -> int | None:
        normalized = str(value or "").strip().lower()
        if not normalized:
            return None
        total_seconds = 0
        consumed = ""
        for match in _INTERVAL_PART_RE.finditer(normalized):
            count = int(match.group("count"))
            unit = match.group("unit").lower()
            consumed += match.group(0)
            if unit == "s":
                total_seconds += count
            elif unit == "m":
                total_seconds += count * 60
            elif unit == "h":
                total_seconds += count * 3600
            elif unit == "d":
                total_seconds += count * 86400
            elif unit == "w":
                total_seconds += count * 604800
        if total_seconds <= 0 or normalized.replace(" ", "") != consumed.replace(" ", ""):
            raise ExecutionValidationError(
                f"Invalid semantic vector refresh interval '{value}'."
            )
        return total_seconds

    def should_refresh(self, index_metadata: SemanticVectorIndexMetadata) -> bool:
        last_refreshed_at = self.normalize_timestamp(index_metadata.last_refreshed_at)
        if last_refreshed_at is None:
            return True
        interval_seconds = index_metadata.refresh_interval_seconds
        if interval_seconds is None or interval_seconds <= 0:
            return False
        return datetime.now(timezone.utc) >= (
            last_refreshed_at + timedelta(seconds=interval_seconds)
        )
