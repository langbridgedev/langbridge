from langbridge.packages.runtime.providers.repository import (
    RepositoryConnectorMetadataProvider,
    RepositoryDatasetMetadataProvider,
    RepositorySemanticModelMetadataProvider,
    RepositorySyncStateProvider,
)


class SqliteDatasetProvider(RepositoryDatasetMetadataProvider):
    """SQLite-backed metadata provider used by the local runtime bootstrap."""


class SqliteConnectorProvider(RepositoryConnectorMetadataProvider):
    """SQLite-backed connector provider used by the local runtime bootstrap."""


class SqliteSemanticModelProvider(RepositorySemanticModelMetadataProvider):
    """SQLite-backed semantic-model provider used by the local runtime bootstrap."""


class SqliteSyncStateProvider(RepositorySyncStateProvider):
    """SQLite-backed sync-state provider used by the local runtime bootstrap."""
