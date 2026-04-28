import enum

from langbridge.runtime.models import RuntimeJobStatus
from langbridge.runtime.ports import MutableJobHandle


class DatasetJobStatusWriter:
    """Updates runtime job status while preserving enum types loaded by other adapters."""

    def set_status(self, job_record: MutableJobHandle, desired_status: RuntimeJobStatus) -> None:
        current_status = getattr(job_record, "status", None)
        if isinstance(current_status, enum.Enum):
            status_type = type(current_status)
            try:
                job_record.status = status_type(desired_status.value)
                return
            except Exception:
                pass
        job_record.status = desired_status.value
