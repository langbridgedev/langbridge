
import enum
import sys
from pathlib import Path
from types import SimpleNamespace


REPO_ROOT = Path(__file__).resolve().parents[2]
PACKAGE_ROOT = REPO_ROOT
if str(PACKAGE_ROOT) not in sys.path:
    sys.path.insert(0, str(PACKAGE_ROOT))

from langbridge.runtime.persistence.db.job import JobStatus  # noqa: E402
from langbridge.runtime.services.dataset_query_service import (  # noqa: E402
    DatasetQueryService,
)


class _CloudJobStatus(enum.Enum):
    queued = "queued"
    running = "running"
    succeeded = "succeeded"
    failed = "failed"
    cancelled = "cancelled"


def test_set_job_status_preserves_loaded_enum_type() -> None:
    job_record = SimpleNamespace(status=_CloudJobStatus.running)

    DatasetQueryService._set_job_status(job_record, JobStatus.succeeded)

    assert job_record.status is _CloudJobStatus.succeeded
