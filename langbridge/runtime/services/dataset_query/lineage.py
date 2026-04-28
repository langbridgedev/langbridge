from langbridge.runtime.models import DatasetMetadata, LineageEdge
from langbridge.runtime.models.metadata import DatasetType
from langbridge.runtime.ports import LineageEdgeStore
from langbridge.runtime.services.dataset_query.metadata import DatasetQueryMetadataBuilder
from langbridge.runtime.utils.lineage import (
    LineageEdgeType,
    LineageNodeType,
    build_api_resource_id,
    build_file_resource_id,
    build_source_table_resource_id,
)


class DatasetQueryLineageWriter:
    """Persists lineage edges for datasets created or mutated by query jobs."""

    def __init__(
        self,
        *,
        lineage_edge_repository: LineageEdgeStore | None,
        metadata_builder: DatasetQueryMetadataBuilder,
    ) -> None:
        self._lineage_edge_repository = lineage_edge_repository
        self._metadata_builder = metadata_builder

    async def replace_dataset_lineage(self, dataset: DatasetMetadata) -> None:
        if self._lineage_edge_repository is None:
            return

        await self._lineage_edge_repository.delete_for_target(
            workspace_id=dataset.workspace_id,
            target_type=LineageNodeType.DATASET.value,
            target_id=str(dataset.id),
        )

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

        dataset_type = dataset.dataset_type
        if dataset_type == DatasetType.TABLE and dataset.connection_id is not None and dataset.table_name:
            edges.append(self._table_lineage_edge(dataset))
        elif dataset_type == DatasetType.API and dataset.connection_id is not None:
            api_edge = self._api_lineage_edge(dataset)
            if api_edge is not None:
                edges.append(api_edge)
        elif dataset_type == DatasetType.FILE:
            file_edge = self._file_lineage_edge(dataset)
            if file_edge is not None:
                edges.append(file_edge)
        elif dataset_type == DatasetType.FEDERATED:
            edges.extend(self._federated_lineage_edges(dataset))

        for edge in edges:
            self._lineage_edge_repository.add(edge)

    def _table_lineage_edge(self, dataset: DatasetMetadata) -> LineageEdge:
        qualified_name = ".".join(
            [
                part
                for part in (dataset.catalog_name, dataset.schema_name, dataset.table_name)
                if part and str(part).strip()
            ]
        ) or str(dataset.table_name)
        return LineageEdge(
            workspace_id=dataset.workspace_id,
            source_type=LineageNodeType.SOURCE_TABLE.value,
            source_id=build_source_table_resource_id(
                connection_id=dataset.connection_id,
                catalog_name=dataset.catalog_name,
                schema_name=dataset.schema_name,
                table_name=dataset.table_name,
            ),
            target_type=LineageNodeType.DATASET.value,
            target_id=str(dataset.id),
            edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
            metadata={
                "connection_id": str(dataset.connection_id),
                "catalog_name": dataset.catalog_name,
                "schema_name": dataset.schema_name,
                "table_name": dataset.table_name,
                "qualified_name": qualified_name,
            },
        )

    def _api_lineage_edge(self, dataset: DatasetMetadata) -> LineageEdge | None:
        source = dict(dataset.source_json or {})
        resource_name = str(source.get("resource") or "").strip()
        if not resource_name:
            return None
        return LineageEdge(
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
                "source": dataset.source_json,
            },
        )

    def _file_lineage_edge(self, dataset: DatasetMetadata) -> LineageEdge | None:
        storage_uri = self._metadata_builder.file_storage_uri(dataset)
        if not storage_uri:
            return None
        return LineageEdge(
            workspace_id=dataset.workspace_id,
            source_type=LineageNodeType.FILE_RESOURCE.value,
            source_id=build_file_resource_id(storage_uri),
            target_type=LineageNodeType.DATASET.value,
            target_id=str(dataset.id),
            edge_type=LineageEdgeType.MATERIALIZES_FROM.value,
            metadata={
                "storage_uri": storage_uri,
                "file_config": dict(dataset.file_config_json or {}),
            },
        )

    def _federated_lineage_edges(self, dataset: DatasetMetadata) -> list[LineageEdge]:
        edges: list[LineageEdge] = []
        seen_ids: set[str] = set()
        for item in self._metadata_builder.source_bindings(dataset):
            raw_id = item.get("dataset_id")
            if raw_id is None:
                continue
            source_id = str(raw_id)
            if not source_id or source_id == str(dataset.id) or source_id in seen_ids:
                continue
            seen_ids.add(source_id)
            edges.append(
                LineageEdge(
                    workspace_id=dataset.workspace_id,
                    source_type=LineageNodeType.DATASET.value,
                    source_id=source_id,
                    target_type=LineageNodeType.DATASET.value,
                    target_id=str(dataset.id),
                    edge_type=LineageEdgeType.DERIVES_FROM.value,
                    metadata={"match_type": "federated_child"},
                )
            )
        return edges
