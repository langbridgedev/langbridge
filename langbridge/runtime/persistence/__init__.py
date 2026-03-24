from typing import Any

__all__ = [
    "RepositoryAgentDefinitionStore",
    "RepositoryConnectorSyncStateStore",
    "RepositoryConversationMemoryStore",
    "RepositoryDatasetCatalogStore",
    "RepositoryDatasetColumnStore",
    "RepositoryDatasetPolicyStore",
    "RepositoryDatasetRevisionStore",
    "RepositoryLLMConnectionStore",
    "RepositoryLineageEdgeStore",
    "RepositorySemanticModelStore",
    "RepositorySemanticVectorIndexStore",
    "RepositorySqlJobArtifactStore",
    "RepositorySqlJobStore",
    "RepositoryThreadMessageStore",
    "RepositoryThreadStore",
]


def __getattr__(name: str) -> Any:
    if name in set(__all__):
        from langbridge.runtime.persistence import stores as stores_module

        return getattr(stores_module, name)
    raise AttributeError(f"module {__name__!r} has no attribute {name!r}")
