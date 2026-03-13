from typing import Any

from langbridge.packages.runtime.providers.protocols import (
    ConnectorMetadataProvider,
    DatasetMetadataProvider,
    SemanticModelMetadataProvider,
    SyncStateProvider,
)

class MemoryDatasetProvider(DatasetMetadataProvider):
    """In-memory dataset metadata provider used for testing and ephemeral runtimes."""
    def __init__(self, datasets: dict[str, Any]) -> None:
        self._datasets = datasets
        
    def get_dataset(self, *, workspace_id, dataset_id):
        return self._datasets.get(dataset_id)