from __future__ import annotations

import json
import uuid
from collections import deque
from collections.abc import Iterable
from dataclasses import dataclass
from typing import Any

import yaml

from langbridge.packages.common.langbridge_common.db.bi import BIDashboard
from langbridge.packages.common.langbridge_common.db.dataset import DatasetRecord
from langbridge.packages.common.langbridge_common.db.lineage import LineageEdgeRecord
from langbridge.packages.common.langbridge_common.db.semantic import SemanticModelEntry
from langbridge.packages.common.langbridge_common.db.sql import SqlSavedQueryRecord
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.repositories.connector_repository import (
    ConnectorRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dashboard_repository import (
    DashboardRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetRepository,
)
from langbridge.packages.common.langbridge_common.repositories.lineage_repository import (
    LineageEdgeRepository,
)
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import (
    SemanticModelRepository,
)
from langbridge.packages.common.langbridge_common.repositories.sql_repository import (
    SqlSavedQueryRepository,
)
from langbridge.packages.common.langbridge_common.utils.lineage import (
    LineageEdgeType,
    LineageNodeType,
    build_api_resource_id,
    build_file_resource_id,
    build_source_table_resource_id,
)
from langbridge.packages.common.langbridge_common.utils.datasets import (
    resolve_dataset_connector_kind,
    resolve_dataset_source_kind,
    resolve_dataset_storage_kind,
)
from langbridge.packages.common.langbridge_common.utils.sql import extract_table_references


@dataclass(frozen=True)
class _LineageEdgeInput:
    source_type: LineageNodeType
    source_id: str
    target_type: LineageNodeType
    target_id: str
    edge_type: LineageEdgeType
    metadata: dict[str, Any]


class LineageService:
    def __init__(
        self,
        *,
        lineage_edge_repository: LineageEdgeRepository,
        dataset_repository: DatasetRepository,
        semantic_model_repository: SemanticModelRepository,
        sql_saved_query_repository: SqlSavedQueryRepository,
        dashboard_repository: DashboardRepository,
        connector_repository: ConnectorRepository,
    ) -> None:
        self._lineage_edge_repository = lineage_edge_repository
        self._dataset_repository = dataset_repository
        self._semantic_model_repository = semantic_model_repository
        self._sql_saved_query_repository = sql_saved_query_repository
        self._dashboard_repository = dashboard_repository
        self._connector_repository = connector_repository

    async def replace_target_edges(
        self,
        *,
        workspace_id: uuid.UUID,
        target_type: LineageNodeType,
        target_id: str,
        edges: Iterable[_LineageEdgeInput],
    ) -> None:
        await self._lineage_edge_repository.delete_for_target(
            workspace_id=workspace_id,
            target_type=target_type.value,
            target_id=target_id,
        )
        for edge in self._dedupe_edges(edges):
            self._lineage_edge_repository.add(
                LineageEdgeRecord(
                    workspace_id=workspace_id,
                    source_type=edge.source_type.value,
                    source_id=edge.source_id,
                    target_type=edge.target_type.value,
                    target_id=edge.target_id,
                    edge_type=edge.edge_type.value,
                    metadata_json=dict(edge.metadata or {}),
                )
            )

    async def delete_node_lineage(
        self,
        *,
        workspace_id: uuid.UUID,
        node_type: LineageNodeType,
        node_id: str,
    ) -> None:
        await self._lineage_edge_repository.delete_for_node(
            workspace_id=workspace_id,
            node_type=node_type.value,
            node_id=node_id,
        )

    async def register_dataset_lineage(self, *, dataset: DatasetRecord) -> None:
        edges: list[_LineageEdgeInput] = []
        target_type = LineageNodeType.DATASET
        target_id = str(dataset.id)
        connection_connector_type: str | None = None
        if dataset.connection_id is not None and not dataset.connector_kind:
            connector = await self._connector_repository.get_by_id(dataset.connection_id)
            connection_connector_type = getattr(connector, "connector_type", None)
        connector_kind = resolve_dataset_connector_kind(
            explicit_connector_kind=dataset.connector_kind,
            connection_connector_type=connection_connector_type,
            file_config=dict(dataset.file_config_json or {}),
            storage_uri=dataset.storage_uri,
            legacy_dataset_type=dataset.dataset_type,
        )
        source_kind = resolve_dataset_source_kind(
            explicit_source_kind=dataset.source_kind,
            legacy_dataset_type=dataset.dataset_type,
            connector_kind=connector_kind,
            file_config=dict(dataset.file_config_json or {}),
        )
        storage_kind = resolve_dataset_storage_kind(
            explicit_storage_kind=dataset.storage_kind,
            legacy_dataset_type=dataset.dataset_type,
            file_config=dict(dataset.file_config_json or {}),
            storage_uri=dataset.storage_uri,
        )

        if dataset.connection_id is not None:
            edges.append(
                _LineageEdgeInput(
                    source_type=LineageNodeType.CONNECTION,
                    source_id=str(dataset.connection_id),
                    target_type=target_type,
                    target_id=target_id,
                    edge_type=LineageEdgeType.FEEDS,
                    metadata={"connection_id": str(dataset.connection_id)},
                )
            )

        if storage_kind.value == "table":
            resource_id = self._source_table_id(
                connection_id=dataset.connection_id,
                catalog_name=dataset.catalog_name,
                schema_name=dataset.schema_name,
                table_name=dataset.table_name,
            )
            if resource_id is not None:
                edges.append(
                    _LineageEdgeInput(
                        source_type=LineageNodeType.SOURCE_TABLE,
                        source_id=resource_id,
                        target_type=target_type,
                        target_id=target_id,
                        edge_type=LineageEdgeType.MATERIALIZES_FROM,
                        metadata=self._source_table_metadata(
                            connection_id=dataset.connection_id,
                            catalog_name=dataset.catalog_name,
                            schema_name=dataset.schema_name,
                            table_name=dataset.table_name,
                        ),
                    )
                )
        elif storage_kind.value == "view":
            dataset_refs, source_refs = await self._resolve_sql_references(
                workspace_id=dataset.workspace_id,
                project_id=dataset.project_id,
                connection_id=dataset.connection_id,
                query_text=dataset.sql_text or "",
                default_catalog=dataset.catalog_name,
            )
            for ref in dataset_refs:
                if ref == dataset.id:
                    continue
                edges.append(
                    _LineageEdgeInput(
                        source_type=LineageNodeType.DATASET,
                        source_id=str(ref),
                        target_type=target_type,
                        target_id=target_id,
                        edge_type=LineageEdgeType.DERIVES_FROM,
                        metadata={"match_type": "dataset_reference"},
                    )
                )
            for source_ref in source_refs:
                edges.append(
                    _LineageEdgeInput(
                        source_type=LineageNodeType.SOURCE_TABLE,
                        source_id=source_ref["resource_id"],
                        target_type=target_type,
                        target_id=target_id,
                        edge_type=LineageEdgeType.DERIVES_FROM,
                        metadata=dict(source_ref["metadata"]),
                    )
                )
        elif storage_kind.value in {"csv", "parquet", "json"}:
            storage_uri = self._resolve_file_storage_uri(dataset)
            file_config = dict(dataset.file_config_json or {})
            sync_meta = (
                file_config.get("connector_sync")
                if isinstance(file_config.get("connector_sync"), dict)
                else {}
            )
            resource_name = str(sync_meta.get("resource_name") or "").strip()
            if dataset.connection_id is not None and resource_name and source_kind.value in {"api", "saas"}:
                edges.append(
                    _LineageEdgeInput(
                        source_type=LineageNodeType.API_RESOURCE,
                        source_id=build_api_resource_id(
                            connection_id=dataset.connection_id,
                            resource_name=resource_name,
                        ),
                        target_type=target_type,
                        target_id=target_id,
                        edge_type=LineageEdgeType.MATERIALIZES_FROM,
                        metadata={
                            "connection_id": str(dataset.connection_id),
                            "connector_type": sync_meta.get("connector_type"),
                            "resource_name": resource_name,
                            "root_resource_name": sync_meta.get("root_resource_name"),
                            "parent_resource_name": sync_meta.get("parent_resource_name"),
                            "source_kind": source_kind.value,
                        },
                    )
                )
            if storage_uri:
                edges.append(
                    _LineageEdgeInput(
                        source_type=LineageNodeType.FILE_RESOURCE,
                        source_id=build_file_resource_id(storage_uri),
                        target_type=target_type,
                        target_id=target_id,
                        edge_type=LineageEdgeType.MATERIALIZES_FROM,
                        metadata={
                            "storage_uri": storage_uri,
                            "file_config": dict(dataset.file_config_json or {}),
                            "source_kind": source_kind.value,
                            "storage_kind": storage_kind.value,
                        },
                    )
                )
        elif storage_kind.value == "virtual":
            for child_id in self._extract_federated_dataset_ids(dataset):
                if child_id == dataset.id:
                    continue
                edges.append(
                    _LineageEdgeInput(
                        source_type=LineageNodeType.DATASET,
                        source_id=str(child_id),
                        target_type=target_type,
                        target_id=target_id,
                        edge_type=LineageEdgeType.DERIVES_FROM,
                        metadata={"match_type": "federated_child"},
                    )
                )

        await self.replace_target_edges(
            workspace_id=dataset.workspace_id,
            target_type=target_type,
            target_id=target_id,
            edges=edges,
        )

    async def register_semantic_model_lineage(self, *, model: SemanticModelEntry) -> None:
        payload = self._parse_model_payload(model.content_json, model.content_yaml)
        target_type = self._semantic_model_node_type_from_payload(payload)
        target_id = str(model.id)
        edges: list[_LineageEdgeInput] = []

        if target_type == LineageNodeType.UNIFIED_SEMANTIC_MODEL:
            for source_model_id in self._extract_unified_source_model_ids(payload):
                edges.append(
                    _LineageEdgeInput(
                        source_type=LineageNodeType.SEMANTIC_MODEL,
                        source_id=str(source_model_id),
                        target_type=target_type,
                        target_id=target_id,
                        edge_type=LineageEdgeType.FEEDS,
                        metadata={"match_type": "source_model"},
                    )
                )
        else:
            for dataset_id, table_keys in self._extract_standard_model_dataset_usage(payload).items():
                edges.append(
                    _LineageEdgeInput(
                        source_type=LineageNodeType.DATASET,
                        source_id=str(dataset_id),
                        target_type=target_type,
                        target_id=target_id,
                        edge_type=LineageEdgeType.FEEDS,
                        metadata={"table_keys": sorted(table_keys)},
                    )
                )

        await self.replace_target_edges(
            workspace_id=model.organization_id,
            target_type=target_type,
            target_id=target_id,
            edges=edges,
        )

    async def register_saved_query_lineage(self, *, record: SqlSavedQueryRecord) -> None:
        target_type = LineageNodeType.SAVED_QUERY
        target_id = str(record.id)
        edges: list[_LineageEdgeInput] = []

        if record.connection_id is not None:
            edges.append(
                _LineageEdgeInput(
                    source_type=LineageNodeType.CONNECTION,
                    source_id=str(record.connection_id),
                    target_type=target_type,
                    target_id=target_id,
                    edge_type=LineageEdgeType.REFERENCES,
                    metadata={"connection_id": str(record.connection_id)},
                )
            )

        dataset_refs, source_refs = await self._resolve_sql_references(
            workspace_id=record.workspace_id,
            project_id=record.project_id,
            connection_id=record.connection_id,
            query_text=record.query_text,
            default_catalog=None,
        )
        for ref in dataset_refs:
            edges.append(
                _LineageEdgeInput(
                    source_type=LineageNodeType.DATASET,
                    source_id=str(ref),
                    target_type=target_type,
                    target_id=target_id,
                    edge_type=LineageEdgeType.REFERENCES,
                    metadata={"match_type": "dataset_reference"},
                )
            )
        for source_ref in source_refs:
            edges.append(
                _LineageEdgeInput(
                    source_type=LineageNodeType.SOURCE_TABLE,
                    source_id=source_ref["resource_id"],
                    target_type=target_type,
                    target_id=target_id,
                    edge_type=LineageEdgeType.REFERENCES,
                    metadata=dict(source_ref["metadata"]),
                )
            )

        await self.replace_target_edges(
            workspace_id=record.workspace_id,
            target_type=target_type,
            target_id=target_id,
            edges=edges,
        )

    async def register_dashboard_lineage(self, *, dashboard: BIDashboard) -> None:
        source_model = await self._semantic_model_repository.get_by_id(dashboard.semantic_model_id)
        if source_model is None:
            raise BusinessValidationError("Dashboard semantic model was not found for lineage registration.")
        source_type = self._semantic_model_node_type_from_payload(
            self._parse_model_payload(source_model.content_json, source_model.content_yaml)
        )
        await self.replace_target_edges(
            workspace_id=dashboard.organization_id,
            target_type=LineageNodeType.DASHBOARD,
            target_id=str(dashboard.id),
            edges=[
                _LineageEdgeInput(
                    source_type=source_type,
                    source_id=str(source_model.id),
                    target_type=LineageNodeType.DASHBOARD,
                    target_id=str(dashboard.id),
                    edge_type=LineageEdgeType.FEEDS,
                    metadata={"semantic_model_id": str(source_model.id)},
                )
            ],
        )

    async def build_dataset_lineage_graph(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_id: uuid.UUID,
    ) -> tuple[list[dict[str, Any]], list[LineageEdgeRecord], int, int]:
        root_key = (LineageNodeType.DATASET.value, str(dataset_id))
        nodes: dict[tuple[str, str], dict[str, Any]] = {
            root_key: {
                "node_type": LineageNodeType.DATASET.value,
                "node_id": str(dataset_id),
                "direction": "root",
                "metadata": {},
            }
        }
        edges: dict[tuple[str, str, str, str, str], LineageEdgeRecord] = {}

        upstream_count = await self._walk_graph(
            workspace_id=workspace_id,
            start_type=LineageNodeType.DATASET,
            start_id=str(dataset_id),
            direction="upstream",
            nodes=nodes,
            edges=edges,
        )
        downstream_count = await self._walk_graph(
            workspace_id=workspace_id,
            start_type=LineageNodeType.DATASET,
            start_id=str(dataset_id),
            direction="downstream",
            nodes=nodes,
            edges=edges,
        )
        hydrated_nodes = await self._hydrate_nodes(workspace_id=workspace_id, raw_nodes=nodes)
        return hydrated_nodes, list(edges.values()), upstream_count, downstream_count

    async def build_dataset_impact(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_id: uuid.UUID,
    ) -> dict[str, Any]:
        root_id = str(dataset_id)
        nodes: dict[tuple[str, str], dict[str, Any]] = {}
        edges: dict[tuple[str, str, str, str, str], LineageEdgeRecord] = {}
        await self._walk_graph(
            workspace_id=workspace_id,
            start_type=LineageNodeType.DATASET,
            start_id=root_id,
            direction="downstream",
            nodes=nodes,
            edges=edges,
        )
        hydrated_nodes = await self._hydrate_nodes(workspace_id=workspace_id, raw_nodes=nodes)
        return await self._format_impact(
            workspace_id=workspace_id,
            dataset_id=root_id,
            nodes=hydrated_nodes,
        )

    @staticmethod
    def _dedupe_edges(edges: Iterable[_LineageEdgeInput]) -> list[_LineageEdgeInput]:
        deduped: dict[tuple[str, str, str, str, str], _LineageEdgeInput] = {}
        for edge in edges:
            key = (
                edge.source_type.value,
                edge.source_id,
                edge.target_type.value,
                edge.target_id,
                edge.edge_type.value,
            )
            deduped[key] = edge
        return list(deduped.values())

    async def _resolve_sql_references(
        self,
        *,
        workspace_id: uuid.UUID,
        project_id: uuid.UUID | None,
        connection_id: uuid.UUID | None,
        query_text: str,
        default_catalog: str | None,
    ) -> tuple[list[uuid.UUID], list[dict[str, Any]]]:
        refs = extract_table_references(query_text, dialect="tsql")
        if not refs:
            return [], []

        datasets = await self._dataset_repository.list_for_workspace(
            workspace_id=workspace_id,
            project_id=project_id,
            limit=5000,
        )
        dataset_lookup: dict[str, set[uuid.UUID]] = {}
        for dataset in datasets:
            for key in self._dataset_reference_keys(dataset):
                dataset_lookup.setdefault(key, set()).add(dataset.id)

        matched_dataset_ids: set[uuid.UUID] = set()
        source_refs: list[dict[str, Any]] = []
        for schema_name, table_name in refs:
            match_keys = [table_name.lower()]
            if schema_name:
                match_keys.insert(0, f"{schema_name.lower()}.{table_name.lower()}")
            if default_catalog and schema_name:
                match_keys.insert(0, f"{default_catalog.lower()}.{schema_name.lower()}.{table_name.lower()}")

            matched_ids: set[uuid.UUID] = set()
            for key in match_keys:
                matched_ids.update(dataset_lookup.get(key, set()))

            if len(matched_ids) == 1:
                matched_dataset_ids.update(matched_ids)
                continue

            resource_id = self._source_table_id(
                connection_id=connection_id,
                catalog_name=default_catalog,
                schema_name=schema_name,
                table_name=table_name,
            )
            if resource_id is None:
                continue
            source_refs.append(
                {
                    "resource_id": resource_id,
                    "metadata": self._source_table_metadata(
                        connection_id=connection_id,
                        catalog_name=default_catalog,
                        schema_name=schema_name,
                        table_name=table_name,
                    ),
                }
            )

        return sorted(matched_dataset_ids, key=str), source_refs

    @staticmethod
    def _dataset_reference_keys(dataset: DatasetRecord) -> set[str]:
        keys = {(dataset.name or "").strip().lower()}
        table_name = (dataset.table_name or "").strip().lower()
        schema_name = (dataset.schema_name or "").strip().lower()
        catalog_name = (dataset.catalog_name or "").strip().lower()
        if table_name:
            keys.add(table_name)
        if schema_name and table_name:
            keys.add(f"{schema_name}.{table_name}")
        if catalog_name and schema_name and table_name:
            keys.add(f"{catalog_name}.{schema_name}.{table_name}")
        return {key for key in keys if key}

    @staticmethod
    def _extract_federated_dataset_ids(dataset: DatasetRecord) -> list[uuid.UUID]:
        ids: set[uuid.UUID] = set()
        for raw_value in dataset.referenced_dataset_ids_json or []:
            try:
                ids.add(uuid.UUID(str(raw_value)))
            except (TypeError, ValueError):
                continue
        plan = dataset.federated_plan_json if isinstance(dataset.federated_plan_json, dict) else {}
        tables_payload = plan.get("tables")
        iterable: Iterable[Any]
        if isinstance(tables_payload, dict):
            iterable = tables_payload.values()
        elif isinstance(tables_payload, list):
            iterable = tables_payload
        else:
            iterable = []
        for item in iterable:
            if not isinstance(item, dict):
                continue
            raw_id = item.get("dataset_id") or item.get("datasetId")
            if raw_id is None:
                continue
            try:
                ids.add(uuid.UUID(str(raw_id)))
            except (TypeError, ValueError):
                continue
        return sorted(ids, key=str)

    @staticmethod
    def _resolve_file_storage_uri(dataset: DatasetRecord) -> str | None:
        candidates = [
            str((dataset.file_config_json or {}).get("source_storage_uri") or "").strip(),
            str((dataset.file_config_json or {}).get("storage_uri") or "").strip(),
            str(dataset.storage_uri or "").strip(),
        ]
        for candidate in candidates:
            if candidate:
                return candidate
        return None

    @staticmethod
    def _source_table_id(
        *,
        connection_id: uuid.UUID | None,
        catalog_name: str | None,
        schema_name: str | None,
        table_name: str | None,
    ) -> str | None:
        if connection_id is None or not table_name:
            return None
        return build_source_table_resource_id(
            connection_id=connection_id,
            catalog_name=catalog_name,
            schema_name=schema_name,
            table_name=table_name,
        )

    @staticmethod
    def _source_table_metadata(
        *,
        connection_id: uuid.UUID | None,
        catalog_name: str | None,
        schema_name: str | None,
        table_name: str | None,
    ) -> dict[str, Any]:
        qualified_name = ".".join(
            [part for part in (catalog_name, schema_name, table_name) if part and str(part).strip()]
        )
        return {
            "connection_id": str(connection_id) if connection_id else None,
            "catalog_name": catalog_name,
            "schema_name": schema_name,
            "table_name": table_name,
            "qualified_name": qualified_name or (table_name or ""),
        }

    @staticmethod
    def _parse_model_payload(content_json: str | None, content_yaml: str | None) -> dict[str, Any]:
        if content_json:
            try:
                parsed_json = json.loads(content_json)
                if isinstance(parsed_json, dict):
                    return parsed_json
            except Exception:
                pass
        if content_yaml:
            try:
                parsed_yaml = yaml.safe_load(content_yaml)
                if isinstance(parsed_yaml, dict):
                    return parsed_yaml
            except Exception:
                pass
        return {}

    @staticmethod
    def _semantic_model_node_type_from_payload(payload: dict[str, Any]) -> LineageNodeType:
        source_models_raw = payload.get("source_models") or payload.get("sourceModels")
        if isinstance(source_models_raw, list):
            return LineageNodeType.UNIFIED_SEMANTIC_MODEL
        return LineageNodeType.SEMANTIC_MODEL

    @staticmethod
    def _extract_unified_source_model_ids(payload: dict[str, Any]) -> list[uuid.UUID]:
        results: list[uuid.UUID] = []
        seen: set[uuid.UUID] = set()
        source_models_raw = payload.get("source_models") or payload.get("sourceModels")
        if not isinstance(source_models_raw, list):
            return results
        for entry in source_models_raw:
            if not isinstance(entry, dict):
                continue
            raw_id = entry.get("id")
            if raw_id is None:
                continue
            try:
                model_id = uuid.UUID(str(raw_id))
            except (TypeError, ValueError):
                continue
            if model_id in seen:
                continue
            seen.add(model_id)
            results.append(model_id)
        return results

    @staticmethod
    def _extract_standard_model_dataset_usage(payload: dict[str, Any]) -> dict[uuid.UUID, set[str]]:
        results: dict[uuid.UUID, set[str]] = {}
        tables = payload.get("tables")
        if not isinstance(tables, dict):
            return results
        for table_key, table_payload in tables.items():
            if not isinstance(table_payload, dict):
                continue
            raw_id = table_payload.get("dataset_id") or table_payload.get("datasetId")
            if raw_id is None:
                continue
            try:
                dataset_id = uuid.UUID(str(raw_id))
            except (TypeError, ValueError):
                continue
            results.setdefault(dataset_id, set()).add(str(table_key))
        return results

    async def _walk_graph(
        self,
        *,
        workspace_id: uuid.UUID,
        start_type: LineageNodeType,
        start_id: str,
        direction: str,
        nodes: dict[tuple[str, str], dict[str, Any]],
        edges: dict[tuple[str, str, str, str, str], LineageEdgeRecord],
    ) -> int:
        visited: set[tuple[str, str]] = set()
        queue = deque([(start_type.value, start_id)])
        discovered = 0

        while queue:
            node_type, node_id = queue.popleft()
            if (node_type, node_id) in visited:
                continue
            visited.add((node_type, node_id))

            if direction == "upstream":
                related_edges = await self._lineage_edge_repository.list_inbound(
                    workspace_id=workspace_id,
                    target_type=node_type,
                    target_id=node_id,
                )
                for edge in related_edges:
                    key = (
                        edge.source_type,
                        edge.source_id,
                        edge.target_type,
                        edge.target_id,
                        edge.edge_type,
                    )
                    edges[key] = edge
                    source_key = (edge.source_type, edge.source_id)
                    if source_key not in nodes:
                        discovered += 1
                    nodes.setdefault(
                        source_key,
                        {
                            "node_type": edge.source_type,
                            "node_id": edge.source_id,
                            "direction": "upstream",
                            "metadata": dict(edge.metadata_json or {}),
                        },
                    )
                    queue.append(source_key)
            else:
                related_edges = await self._lineage_edge_repository.list_outbound(
                    workspace_id=workspace_id,
                    source_type=node_type,
                    source_id=node_id,
                )
                for edge in related_edges:
                    key = (
                        edge.source_type,
                        edge.source_id,
                        edge.target_type,
                        edge.target_id,
                        edge.edge_type,
                    )
                    edges[key] = edge
                    target_key = (edge.target_type, edge.target_id)
                    if target_key not in nodes:
                        discovered += 1
                    nodes.setdefault(
                        target_key,
                        {
                            "node_type": edge.target_type,
                            "node_id": edge.target_id,
                            "direction": "downstream",
                            "metadata": dict(edge.metadata_json or {}),
                        },
                    )
                    queue.append(target_key)

        return discovered

    async def _hydrate_nodes(
        self,
        *,
        workspace_id: uuid.UUID,
        raw_nodes: dict[tuple[str, str], dict[str, Any]],
    ) -> list[dict[str, Any]]:
        hydrated: list[dict[str, Any]] = []
        for _, raw_node in raw_nodes.items():
            hydrated.append(
                await self._describe_node(
                    workspace_id=workspace_id,
                    node_type=str(raw_node["node_type"]),
                    node_id=str(raw_node["node_id"]),
                    direction=str(raw_node["direction"]),
                    hint_metadata=dict(raw_node.get("metadata") or {}),
                )
            )
        return sorted(hydrated, key=lambda item: (item["direction"], item["node_type"], item["label"]))

    async def _describe_node(
        self,
        *,
        workspace_id: uuid.UUID,
        node_type: str,
        node_id: str,
        direction: str,
        hint_metadata: dict[str, Any],
    ) -> dict[str, Any]:
        metadata = dict(hint_metadata or {})
        label = node_id

        if node_type == LineageNodeType.DATASET.value:
            dataset = await self._safe_get_uuid_record(self._dataset_repository, node_id)
            if dataset is not None:
                label = dataset.name
                metadata.update(
                    {
                        "name": dataset.name,
                        "dataset_type": dataset.dataset_type,
                        "status": dataset.status,
                        "project_id": str(dataset.project_id) if dataset.project_id else None,
                    }
                )
        elif node_type in {
            LineageNodeType.SEMANTIC_MODEL.value,
            LineageNodeType.UNIFIED_SEMANTIC_MODEL.value,
        }:
            model = await self._safe_get_uuid_record(self._semantic_model_repository, node_id)
            if model is not None:
                label = model.name
                metadata.update(
                    {
                        "name": model.name,
                        "project_id": str(model.project_id) if model.project_id else None,
                        "connector_id": str(model.connector_id),
                    }
                )
        elif node_type == LineageNodeType.SAVED_QUERY.value:
            query = await self._safe_get_uuid_record(self._sql_saved_query_repository, node_id)
            if query is not None:
                label = query.name
                metadata.update(
                    {
                        "name": query.name,
                        "project_id": str(query.project_id) if query.project_id else None,
                        "connection_id": str(query.connection_id) if query.connection_id else None,
                    }
                )
        elif node_type == LineageNodeType.DASHBOARD.value:
            dashboard = await self._safe_get_uuid_record(self._dashboard_repository, node_id)
            if dashboard is not None and str(dashboard.organization_id) == str(workspace_id):
                label = dashboard.name
                metadata.update(
                    {
                        "name": dashboard.name,
                        "project_id": str(dashboard.project_id) if dashboard.project_id else None,
                        "semantic_model_id": str(dashboard.semantic_model_id),
                    }
                )
        elif node_type == LineageNodeType.CONNECTION.value:
            connector = await self._safe_get_uuid_record(self._connector_repository, node_id)
            if connector is not None:
                label = getattr(connector, "name", None) or node_id
                metadata.update(
                    {
                        "name": getattr(connector, "name", None),
                        "connector_type": getattr(connector, "connector_type", None),
                    }
                )
        elif node_type == LineageNodeType.SOURCE_TABLE.value:
            label = str(metadata.get("qualified_name") or node_id.split(":", 1)[-1])
        elif node_type == LineageNodeType.API_RESOURCE.value:
            label = str(
                metadata.get("resource_name")
                or metadata.get("root_resource_name")
                or node_id.split(":api:", 1)[-1]
            )
        elif node_type == LineageNodeType.FILE_RESOURCE.value:
            label = str(metadata.get("storage_uri") or metadata.get("filename") or node_id)

        return {
            "node_type": node_type,
            "node_id": node_id,
            "direction": direction,
            "label": label,
            "metadata": metadata,
        }

    async def _format_impact(
        self,
        *,
        workspace_id: uuid.UUID,
        dataset_id: str,
        nodes: list[dict[str, Any]],
    ) -> dict[str, Any]:
        supported_types = {
            LineageNodeType.DATASET.value,
            LineageNodeType.SEMANTIC_MODEL.value,
            LineageNodeType.UNIFIED_SEMANTIC_MODEL.value,
            LineageNodeType.SAVED_QUERY.value,
            LineageNodeType.DASHBOARD.value,
        }
        direct_keys = {
            (edge.target_type, edge.target_id)
            for edge in await self._lineage_edge_repository.list_outbound(
                workspace_id=workspace_id,
                source_type=LineageNodeType.DATASET.value,
                source_id=dataset_id,
            )
            if edge.target_type in supported_types
        }

        items_by_type: dict[str, list[dict[str, Any]]] = {
            LineageNodeType.DATASET.value: [],
            LineageNodeType.SEMANTIC_MODEL.value: [],
            LineageNodeType.UNIFIED_SEMANTIC_MODEL.value: [],
            LineageNodeType.SAVED_QUERY.value: [],
            LineageNodeType.DASHBOARD.value: [],
        }
        direct_dependents: list[dict[str, Any]] = []

        for node in nodes:
            node_type = str(node["node_type"])
            node_id = str(node["node_id"])
            if node_type not in supported_types:
                continue
            if node_type == LineageNodeType.DATASET.value and node_id == dataset_id:
                continue
            item = {
                "node_type": node_type,
                "node_id": node_id,
                "label": node["label"],
                "direct": (node_type, node_id) in direct_keys,
                "metadata": dict(node.get("metadata") or {}),
            }
            items_by_type[node_type].append(item)
            if item["direct"]:
                direct_dependents.append(item)

        total = sum(len(items) for items in items_by_type.values())
        return {
            "total_downstream_assets": total,
            "direct_dependents": self._sort_items(direct_dependents),
            "dependent_datasets": self._sort_items(items_by_type[LineageNodeType.DATASET.value]),
            "semantic_models": self._sort_items(items_by_type[LineageNodeType.SEMANTIC_MODEL.value]),
            "unified_semantic_models": self._sort_items(
                items_by_type[LineageNodeType.UNIFIED_SEMANTIC_MODEL.value]
            ),
            "saved_queries": self._sort_items(items_by_type[LineageNodeType.SAVED_QUERY.value]),
            "dashboards": self._sort_items(items_by_type[LineageNodeType.DASHBOARD.value]),
        }

    @staticmethod
    async def _safe_get_uuid_record(repository: Any, raw_id: str) -> Any | None:
        try:
            parsed = uuid.UUID(str(raw_id))
        except (TypeError, ValueError):
            return None
        getter = getattr(repository, "get_by_id", None)
        if getter is None:
            return None
        return await getter(parsed)

    @staticmethod
    def _sort_items(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        return sorted(items, key=lambda item: (str(item.get("label") or ""), str(item.get("node_id") or "")))
