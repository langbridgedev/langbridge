import os
from dataclasses import dataclass


def _read_bool(name: str, default: bool) -> bool:
    raw = os.getenv(name)
    if raw is None:
        return default
    normalized = raw.strip().lower()
    if normalized in {"1", "true", "yes", "on"}:
        return True
    if normalized in {"0", "false", "no", "off"}:
        return False
    return default


def _read_int(name: str, default: int) -> int:
    raw = os.getenv(name)
    if raw is None:
        return default
    try:
        return int(raw.strip())
    except (TypeError, ValueError):
        return default


def _default_federation_blocking_workers() -> int:
    stage_parallelism = max(1, _read_int("FEDERATION_STAGE_PARALLELISM", 4))
    cpu_count = max(1, os.cpu_count() or stage_parallelism)
    return max(1, min(max(stage_parallelism, 4), cpu_count, 8))


@dataclass(frozen=True)
class RuntimeSettings:
    SQL_DEFAULT_MAX_PREVIEW_ROWS: int = _read_int("SQL_DEFAULT_MAX_PREVIEW_ROWS", 1000)
    SQL_DEFAULT_MAX_EXPORT_ROWS: int = _read_int("SQL_DEFAULT_MAX_EXPORT_ROWS", 25000)
    SQL_POLICY_MAX_PREVIEW_ROWS_UPPER_BOUND: int = _read_int(
        "SQL_POLICY_MAX_PREVIEW_ROWS_UPPER_BOUND",
        50000,
    )
    SQL_POLICY_MAX_EXPORT_ROWS_UPPER_BOUND: int = _read_int(
        "SQL_POLICY_MAX_EXPORT_ROWS_UPPER_BOUND",
        500000,
    )
    SQL_FEDERATION_ENABLED: bool = _read_bool("SQL_FEDERATION_ENABLED", True)
    SQL_FEDERATION_MAX_ELIGIBLE_DATASETS: int = _read_int("SQL_FEDERATION_MAX_ELIGIBLE_DATASETS", 200)
    DATASET_FILE_LOCAL_DIR: str = os.getenv("DATASET_FILE_LOCAL_DIR", "./.langbridge/.langbridge-data/datasets")
    FEDERATION_ARTIFACT_DIR: str = os.getenv("FEDERATION_ARTIFACT_DIR", "./.langbridge/.langbridge-data/federation")
    MANAGED_VECTOR_FAISS_DB_DIR: str = os.getenv("MANAGED_VECTOR_FAISS_DB_DIR", "./.langbridge/.langbridge-data/vector_faiss_dbs")
    FEDERATION_BROADCAST_THRESHOLD_BYTES: int = _read_int(
        "FEDERATION_BROADCAST_THRESHOLD_BYTES",
        64 * 1024 * 1024,
    )
    FEDERATION_PARTITION_COUNT: int = _read_int("FEDERATION_PARTITION_COUNT", 8)
    FEDERATION_STAGE_MAX_RETRIES: int = _read_int("FEDERATION_STAGE_MAX_RETRIES", 4)
    FEDERATION_STAGE_PARALLELISM: int = _read_int("FEDERATION_STAGE_PARALLELISM", 4)
    FEDERATION_BLOCKING_MAX_WORKERS: int = _read_int(
        "FEDERATION_BLOCKING_MAX_WORKERS",
        _default_federation_blocking_workers(),
    )
    FEDERATION_DEFAULT_TTL_SECONDS: int = _read_int("FEDERATION_DEFAULT_TTL_SECONDS", 10)
    FEDERATION_DEFAULT_RETRY: int = _read_int("FEDERATION_DEFAULT_RETRY", 2)


runtime_settings = RuntimeSettings()
