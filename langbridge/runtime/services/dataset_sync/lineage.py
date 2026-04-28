from langbridge.runtime.models import DatasetMetadata, LineageEdge
from langbridge.runtime.ports import LineageEdgeStore
from langbridge.runtime.services.dataset_sync.sources import relation_parts
from langbridge.runtime.utils.lineage import (
    LineageEdgeType,
    LineageNodeType,
    build_api_resource_id,
    build_file_resource_id,
    build_source_table_resource_id,
)


class DatasetSyncLineageWriter:
    """Persists lineage edges for managed synced datasets."""

    def __init__(self, *, lineage_edge_repository: LineageEdgeStore | None) -> None:
        self._lineage_edge_repository = lineage_edge_repository

    async def replace_dataset_lineage(self, *, dataset: DatasetMetadata) -> None:
        if self._lineage_edge_repository is None:
            return
        await self._lineage_edge_repository.delete_for_target(
            workspace_id=dataset.workspace_id,
            target_type=LineageNodeType.DATASET.value,
            target_id=str(dataset.id),
        )

        file_config = dict(dataset.file_config_json or {})
        sync_meta = dict(dataset.sync_json or {})
        sync_source = dict(sync_meta.get("source") or {})
        storage_uri = str(dataset.storage_uri or "").strip()

        edges: list[LineageEdge] = []
        if dataset.connection_id is not None:
            edges.append(
                LineageEdge(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.CONNECTION.value,
                    source_id=str(dataset.connection_id),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.FEEDS.value,
                    metadata={"connection_id": str(dataset.connection_id)},
                )
            )
        resource_name = str(sync_source.get("resource") or "").strip()
        request_payload = sync_source.get("request")
        request_path = str(request_payload.get("path") or "").strip() if isinstance(request_payload, dict) else ""
        table_name = str(sync_source.get("table") or "").strip()
        if dataset.connection_id is not None and resource_name:
            edges.append(
                LineageEdge(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.API_RESOURCE.value,
                    source_id=build_api_resource_id(
                        connection_id=dataset.connection_id,
                        resource_name=resource_name,
                    ),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
                    metadata={
                        "connection_id": str(dataset.connection_id),
                        "resource_name": resource_name,
                        "source": sync_source,
                        "strategy": sync_meta.get("strategy"),
                        "cadence": sync_meta.get("cadence"),
                    },
                )
            )
        if dataset.connection_id is not None and request_path:
            edges.append(
                LineageEdge(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.API_RESOURCE.value,
                    source_id=build_api_resource_id(
                        connection_id=dataset.connection_id,
                        resource_name=f"request:{request_path}",
                    ),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
                    metadata={
                        "connection_id": str(dataset.connection_id),
                        "request": request_payload,
                        "extraction": sync_source.get("extraction"),
                        "strategy": sync_meta.get("strategy"),
                        "cadence": sync_meta.get("cadence"),
                    },
                )
            )
        if dataset.connection_id is not None and table_name:
            catalog_name, schema_name, base_table_name = relation_parts(table_name)
            edges.append(
                LineageEdge(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.SOURCE_TABLE.value,
                    source_id=build_source_table_resource_id(
                        connection_id=dataset.connection_id,
                        catalog_name=catalog_name,
                        schema_name=schema_name,
                        table_name=base_table_name,
                    ),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
                    metadata={
                        "connection_id": str(dataset.connection_id),
                        "catalog_name": catalog_name,
                        "schema_name": schema_name,
                        "table_name": base_table_name,
                        "qualified_name": table_name,
                        "source": sync_source,
                        "strategy": sync_meta.get("strategy"),
                        "cadence": sync_meta.get("cadence"),
                    },
                )
            )
        if storage_uri:
            edges.append(
                LineageEdge(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.FILE_RESOURCE.value,
                    source_id=build_file_resource_id(storage_uri),
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
                    metadata={
                        "storage_uri": storage_uri,
                        "file_config": file_config,
                    },
                )
            )
        for edge in edges:
            self._lineage_edge_repository.add(edge)
