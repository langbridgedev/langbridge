from langbridge.runtime.models import (
    CreateDatasetBulkCreateJobRequest,
    CreateDatasetCsvIngestJobRequest,
    CreateDatasetPreviewJobRequest,
    CreateDatasetProfileJobRequest,
)

DatasetExecutionRequest = (
    CreateDatasetPreviewJobRequest
    | CreateDatasetProfileJobRequest
    | CreateDatasetCsvIngestJobRequest
    | CreateDatasetBulkCreateJobRequest
)
