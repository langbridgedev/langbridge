import json
import logging
import re
import uuid
from collections.abc import Iterable, Mapping
from dataclasses import dataclass
from datetime import datetime
from typing import Any

import yaml

from langbridge.apps.worker.langbridge_worker.dataset_execution import (
    DatasetExecutionResolver,
    build_binding_for_dataset,
    synthetic_file_connector_id,
)
from langbridge.apps.worker.langbridge_worker.tools.federated_query_tool import FederatedQueryTool
from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.contracts.semantic import (
    SemanticQueryResponse,
    UnifiedSemanticSourceModelRequest,
    UnifiedSemanticQueryResponse,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.common.langbridge_common.repositories.semantic_model_repository import (
    SemanticModelRepository,
)
from langbridge.packages.common.langbridge_common.repositories.dataset_repository import (
    DatasetRepository,
)
from langbridge.packages.connectors.langbridge_connectors.api import (
    ConnectorRuntimeTypeSqlDialectMap,
    SqlConnector,
    SqlConnectorFactory,
    get_connector_config_factory,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import ConnectorRuntimeType
from langbridge.packages.semantic.langbridge_semantic.loader import (
    SemanticModelError,
    load_semantic_model,
    load_unified_semantic_model,
)
from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel
from langbridge.packages.semantic.langbridge_semantic.query import SemanticQuery, SemanticQueryEngine
from langbridge.packages.semantic.langbridge_semantic.unified_query import (
    TenantAwareQueryContext,
    UnifiedSourceModel,
    apply_tenant_aware_context,
    build_unified_semantic_model,
)

_DATE_RANGE_PRESETS = {
    "today",
    "yesterday",
    "last_7_days",
    "last_30_days",
    "month_to_date",
    "year_to_date",
}


def _normalize_unified_relationship_payload(relationship: Any) -> dict[str, Any]:
    if hasattr(relationship, "model_dump"):
        return relationship.model_dump(exclude_none=True)
    if isinstance(relationship, Mapping):
        return dict(relationship)
    return dict(relationship)
_YEAR_PATTERN = re.compile(r"^\d{4}$")
_YEAR_MONTH_PATTERN = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
_ISO_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")
_ISO_DATE_DOT_RANGE_PATTERN = re.compile(
    r"^\s*(\d{4}-\d{2}-\d{2})\s*\.\.\s*(\d{4}-\d{2}-\d{2})\s*$"
)
_DATE_MEMBER_HINTS = ("date", "time", "timestamp", "_at", "_ts")


@dataclass(frozen=True)
class UnifiedModelConfig:
    semantic_model_ids: list[uuid.UUID]
    source_models: list[UnifiedSemanticSourceModelRequest] | None = None
    relationships: list[dict[str, Any]] | None = None
    metrics: dict[str, Any] | None = None


@dataclass(frozen=True)
class UnifiedQueryExecutionResult:
    response: UnifiedSemanticQueryResponse
    compiled_sql: str


@dataclass(frozen=True)
class StandardQueryExecutionResult:
    response: SemanticQueryResponse
    compiled_sql: str


class SemanticQueryExecutionService:
    def __init__(
        self,
        *,
        semantic_model_repository: SemanticModelRepository | None,
        dataset_repository: DatasetRepository | None,
        federated_query_tool: FederatedQueryTool | None,
        logger: logging.Logger,
    ) -> None:
        self._semantic_model_repository = semantic_model_repository
        self._dataset_repository = dataset_repository
        self._federated_query_tool = federated_query_tool
        self._logger = logger
        self._engine = SemanticQueryEngine()
        self._dataset_execution_resolver = DatasetExecutionResolver(
            dataset_repository=dataset_repository
        )

    @staticmethod
    async def create_sql_connector(
        *,
        connector_type: ConnectorRuntimeType,
        connector_config: dict[str, Any],
        sql_connector_factory: SqlConnectorFactory,
        logger: logging.Logger,
    ) -> SqlConnector:
        dialect = ConnectorRuntimeTypeSqlDialectMap.get(connector_type)
        if dialect is None:
            raise BusinessValidationError(
                f"Connector type {connector_type.value} does not support SQL operations."
            )
        config_factory = get_connector_config_factory(connector_type)
        config_instance = config_factory.create(connector_config.get("config", {}))
        sql_connector = sql_connector_factory.create_sql_connector(
            dialect,
            config_instance,
            logger=logger,
        )
        await sql_connector.test_connection()
        return sql_connector

    @staticmethod
    def build_widget_query_payload(
        *,
        widget: dict[str, Any],
        global_filters: list[dict[str, Any]],
    ) -> dict[str, Any]:
        time_dimensions = []
        time_dimension = str(widget.get("timeDimension") or "").strip()
        if time_dimension:
            time_dimensions.append(
                {
                    "dimension": time_dimension,
                    "granularity": str(widget.get("timeGrain") or "").strip() or None,
                    "dateRange": SemanticQueryExecutionService.resolve_widget_time_date_range(widget),
                }
            )

        all_filters = [*global_filters, *list(widget.get("filters") or [])]
        filters_payload = SemanticQueryExecutionService.to_semantic_filters(all_filters)

        order_payload = [
            {entry["member"]: entry["direction"]}
            for entry in list(widget.get("orderBys") or [])
            if isinstance(entry, dict) and entry.get("member")
        ]

        return {
            "measures": list(widget.get("measures") or []),
            "dimensions": list(widget.get("dimensions") or []),
            "timeDimensions": time_dimensions,
            "filters": filters_payload,
            "order": order_payload or None,
            "limit": int(widget.get("limit") or 500),
        }

    @staticmethod
    def resolve_widget_time_date_range(widget: dict[str, Any]) -> str | list[str] | None:
        preset = str(widget.get("timeRangePreset") or "").strip()
        if not preset or preset == "no_filter":
            return None
        if preset in {"today", "yesterday", "last_7_days", "last_30_days", "month_to_date", "year_to_date"}:
            return preset

        from_date = str(widget.get("timeRangeFrom") or "").strip()
        to_date = str(widget.get("timeRangeTo") or "").strip()
        if preset == "custom_between":
            if from_date and to_date:
                return [from_date, to_date]
            return None
        if preset == "custom_before":
            date = from_date or to_date
            return f"before:{date}" if date else None
        if preset == "custom_after":
            date = from_date or to_date
            return f"after:{date}" if date else None
        if preset == "custom_on":
            date = from_date or to_date
            return f"on:{date}" if date else None
        return None

    @staticmethod
    def to_semantic_filters(filters: list[dict[str, Any]]) -> list[dict[str, Any]]:
        payload: list[dict[str, Any]] = []
        for filter_entry in filters:
            member = str(filter_entry.get("member") or "").strip()
            if not member:
                continue
            operator = str(filter_entry.get("operator") or "equals").strip().lower() or "equals"
            if operator in {"set", "notset"}:
                payload.append({"member": member, "operator": operator})
                continue

            raw_values = filter_entry.get("values")
            values: list[str] = []
            if isinstance(raw_values, str):
                values = [part.strip() for part in raw_values.split(",") if part.strip()]
            elif isinstance(raw_values, list):
                values = [str(part).strip() for part in raw_values if str(part).strip()]

            if not values:
                continue
            normalized_operator, normalized_values = SemanticQueryExecutionService._normalize_filter_values(
                member=member,
                operator=operator,
                values=values,
            )
            payload.append(
                {
                    "member": member,
                    "operator": normalized_operator,
                    "values": normalized_values,
                }
            )
        return payload

    @staticmethod
    def _normalize_filter_values(
        *,
        member: str,
        operator: str,
        values: list[str],
    ) -> tuple[str, list[str]]:
        op = operator.strip().lower()
        if op not in {"equals", "notequals", "indaterange", "notindaterange"}:
            return operator, values

        single_value = values[0].strip() if len(values) == 1 else None
        if single_value:
            preset = single_value.lower()
            if preset in _DATE_RANGE_PRESETS:
                return SemanticQueryExecutionService._to_date_range_operator(op), [preset]
            if preset.startswith(("before:", "after:", "on:")):
                return SemanticQueryExecutionService._to_date_range_operator(op), [single_value]

        if len(values) == 2 and all(_ISO_DATE_PATTERN.match(value.strip()) for value in values):
            return SemanticQueryExecutionService._to_date_range_operator(op), values

        if not SemanticQueryExecutionService._looks_date_like_member(member):
            return operator, values

        if single_value:
            normalized = SemanticQueryExecutionService._normalize_single_date_like_value(single_value)
            if normalized is not None:
                return SemanticQueryExecutionService._to_date_range_operator(op), normalized

        return operator, values

    @staticmethod
    def _to_date_range_operator(operator: str) -> str:
        return "notindaterange" if operator in {"notequals", "notindaterange"} else "indaterange"

    @staticmethod
    def _looks_date_like_member(member: str) -> bool:
        normalized = member.strip().lower()
        return any(hint in normalized for hint in _DATE_MEMBER_HINTS)

    @staticmethod
    def _normalize_single_date_like_value(value: str) -> list[str] | None:
        trimmed = value.strip()
        if not trimmed:
            return None

        if _YEAR_PATTERN.match(trimmed):
            return [f"{trimmed}-01-01", f"{trimmed}-12-31"]

        year_month_match = _YEAR_MONTH_PATTERN.match(trimmed)
        if year_month_match:
            year_str, month_str = trimmed.split("-")
            year = int(year_str)
            month = int(month_str)
            if month == 12:
                next_year, next_month = year + 1, 1
            else:
                next_year, next_month = year, month + 1
            last_day = (datetime(next_year, next_month, 1) - datetime(year, month, 1)).days
            return [f"{trimmed}-01", f"{trimmed}-{last_day:02d}"]

        if _ISO_DATE_PATTERN.match(trimmed):
            return [f"on:{trimmed}"]

        dot_range_match = _ISO_DATE_DOT_RANGE_PATTERN.match(trimmed)
        if dot_range_match:
            return [dot_range_match.group(1), dot_range_match.group(2)]

        return None

    async def execute_unified_query(
        self,
        *,
        organization_id: uuid.UUID,
        project_id: uuid.UUID | None,
        semantic_query: SemanticQuery,
        semantic_model_ids: Iterable[uuid.UUID],
        source_models: Iterable[UnifiedSemanticSourceModelRequest] | None = None,
        relationships: Iterable[Any] | None = None,
        metrics: Mapping[str, Any] | None = None,
    ) -> UnifiedQueryExecutionResult:
        if self._federated_query_tool is None:
            raise BusinessValidationError("Federated query tool is not configured on this worker.")

        semantic_model, table_connector_map = await self._build_unified_model_and_map(
            organization_id=organization_id,
            semantic_model_ids=semantic_model_ids,
            source_models=source_models,
            relationships=relationships,
            metrics=metrics,
        )
        execution_connector_id = self.build_unified_execution_connector_id(
            organization_id=organization_id
        )
        execution_model = apply_tenant_aware_context(
            semantic_model,
            context=TenantAwareQueryContext(
                organization_id=organization_id,
                execution_connector_id=execution_connector_id,
            ),
            table_connector_map=table_connector_map,
        )

        try:
            plan = self._engine.compile(
                semantic_query,
                execution_model,
                dialect="tsql",
            )
        except Exception as exc:
            raise BusinessValidationError(f"Semantic query translation failed: {exc}") from exc

        workflow_payload = await self._build_federation_workflow_payload(
            organization_id=organization_id,
            semantic_model=execution_model,
            source_semantic_model=semantic_model,
            table_connector_map=table_connector_map,
        )
        tool_payload = {
            "workspace_id": str(organization_id),
            "query": semantic_query.model_dump(by_alias=True, exclude_none=True),
            "dialect": "duckdb",
            "workflow": workflow_payload,
            "semantic_model": execution_model.model_dump(by_alias=True, exclude_none=True),
        }

        execution = await self._federated_query_tool.execute_federated_query(tool_payload)
        data_payload = execution.get("rows", [])
        if not isinstance(data_payload, list):
            raise BusinessValidationError("Federated query execution returned an invalid row payload.")

        response = UnifiedSemanticQueryResponse(
            id=uuid.uuid4(),
            organization_id=organization_id,
            project_id=project_id,
            connector_id=execution_connector_id,
            semantic_model_ids=list(self._normalize_model_ids(semantic_model_ids)),
            data=data_payload,
            annotations=plan.annotations,
            metadata=plan.metadata,
        )
        return UnifiedQueryExecutionResult(response=response, compiled_sql=plan.sql)

    async def execute_standard_query(
        self,
        *,
        organization_id: uuid.UUID,
        project_id: uuid.UUID | None,
        semantic_model_id: uuid.UUID,
        semantic_query: SemanticQuery,
    ) -> StandardQueryExecutionResult:
        if self._semantic_model_repository is None:
            raise BusinessValidationError("Semantic model repository is required for semantic query execution.")
        if self._federated_query_tool is None:
            raise BusinessValidationError("Federated query tool is not configured on this worker.")

        semantic_model_record = await self._semantic_model_repository.get_for_scope(
            model_id=semantic_model_id,
            organization_id=organization_id,
        )
        if semantic_model_record is None:
            raise BusinessValidationError("Semantic model not found.")

        semantic_model = self.load_model_payload(semantic_model_record.content_yaml)
        raw_payload = self._parse_model_payload_from_record(semantic_model_record) or {}
        raw_datasets = (
            raw_payload.get("datasets")
            if isinstance(raw_payload.get("datasets"), Mapping)
            else raw_payload.get("tables")
        )
        workflow, workflow_dialect = await self._dataset_execution_resolver.build_semantic_workflow(
            organization_id=organization_id,
            workflow_id=f"workflow_semantic_dataset_{semantic_model_id.hex[:12]}",
            dataset_name=f"semantic_dataset_{semantic_model_id.hex[:12]}",
            semantic_model=semantic_model,
            raw_datasets_payload=raw_datasets if isinstance(raw_datasets, Mapping) else None,
        )

        try:
            plan = self._engine.compile(
                semantic_query,
                semantic_model,
                dialect=workflow_dialect,
            )
        except Exception as exc:
            raise BusinessValidationError(f"Semantic query translation failed: {exc}") from exc

        execution = await self._federated_query_tool.execute_federated_query(
            {
                "workspace_id": str(organization_id),
                "query": semantic_query.model_dump(by_alias=True, exclude_none=True),
                "dialect": workflow_dialect,
                "workflow": workflow.model_dump(mode="json"),
                "semantic_model": semantic_model.model_dump(by_alias=True, exclude_none=True),
            }
        )
        rows_payload = execution.get("rows", [])
        if not isinstance(rows_payload, list):
            raise BusinessValidationError("Dataset-backed semantic query returned an invalid row payload.")

        response = SemanticQueryResponse(
            id=uuid.uuid4(),
            organization_id=organization_id,
            project_id=project_id,
            semantic_model_id=semantic_model_id,
            data=[row for row in rows_payload if isinstance(row, dict)],
            annotations=plan.annotations,
            metadata=plan.metadata,
        )
        return StandardQueryExecutionResult(response=response, compiled_sql=plan.sql)

    async def _build_unified_model_and_map(
        self,
        *,
        organization_id: uuid.UUID,
        semantic_model_ids: Iterable[uuid.UUID],
        source_models: Iterable[UnifiedSemanticSourceModelRequest] | None = None,
        relationships: Iterable[Any] | None = None,
        metrics: Mapping[str, Any] | None = None,
    ) -> tuple[SemanticModel, dict[str, uuid.UUID]]:
        if self._semantic_model_repository is None:
            raise BusinessValidationError("Semantic model repository is required for unified query execution.")
        if self._dataset_repository is None:
            raise BusinessValidationError("Dataset repository is required for unified query execution.")

        normalized_model_ids = self._normalize_model_ids(semantic_model_ids)
        source_model_defs_by_id = {
            source_model.id: source_model
            for source_model in (source_models or [])
        }
        loaded_source_models: list[UnifiedSourceModel] = []
        table_connector_map: dict[str, uuid.UUID] = {}
        seen_keys: set[str] = set()
        for semantic_model_id in normalized_model_ids:
            semantic_model_record = await self._semantic_model_repository.get_for_scope(
                model_id=semantic_model_id,
                organization_id=organization_id,
            )
            if semantic_model_record is None:
                raise BusinessValidationError(
                    f"Semantic model '{semantic_model_id}' not found for unified query."
                )
            semantic_model = self.load_model_payload(semantic_model_record.content_yaml)
            raw_payload = self._parse_model_payload_from_record(semantic_model_record) or {}
            raw_datasets = {}
            if isinstance(raw_payload, dict):
                candidate = raw_payload.get("datasets")
                if not isinstance(candidate, Mapping):
                    candidate = raw_payload.get("tables")
                if isinstance(candidate, Mapping):
                    raw_datasets = candidate
            source_model_def = source_model_defs_by_id.get(semantic_model_id)
            source_key = self._build_source_model_key(
                preferred_name=(
                    source_model_def.alias
                    if source_model_def is not None and source_model_def.alias
                    else semantic_model_record.name
                ),
                model_id=semantic_model_id,
                seen_keys=seen_keys,
            )
            loaded_source_models.append(
                UnifiedSourceModel(
                    model_id=semantic_model_record.id,
                    key=source_key,
                    model=semantic_model,
                    connector_id=semantic_model_record.connector_id,
                    name=(
                        source_model_def.name
                        if source_model_def is not None and source_model_def.name
                        else semantic_model_record.name
                    ),
                    description=(
                        source_model_def.description
                        if source_model_def is not None and source_model_def.description
                        else semantic_model_record.description
                    ),
                )
            )
            for dataset_key, semantic_dataset in semantic_model.datasets.items():
                materialized_dataset_key = f"{source_key}__{dataset_key}"
                raw_dataset = raw_datasets.get(dataset_key) if isinstance(raw_datasets, Mapping) else None
                dataset_payload = raw_dataset if isinstance(raw_dataset, Mapping) else {}
                dataset_ref = (
                    dataset_payload.get("dataset_id")
                    or dataset_payload.get("datasetId")
                    or semantic_dataset.dataset_id
                )
                if not dataset_ref:
                    raise BusinessValidationError(
                        f"Unified semantic model dataset '{dataset_key}' must define dataset_id for federated execution."
                    )
                try:
                    dataset_id = uuid.UUID(str(dataset_ref))
                except (TypeError, ValueError) as exc:
                    raise BusinessValidationError(
                        f"Unified semantic model dataset '{dataset_key}' contains an invalid dataset_id."
                    ) from exc
                dataset = await self._dataset_repository.get_for_workspace(
                    dataset_id=dataset_id,
                    workspace_id=organization_id,
                )
                if dataset is None:
                    raise BusinessValidationError(
                        f"Dataset '{dataset_id}' referenced by unified semantic dataset '{dataset_key}' was not found."
                    )
                dataset_type = str(dataset.dataset_type or "").upper()
                if dataset_type == "FILE":
                    table_connector_map[materialized_dataset_key] = synthetic_file_connector_id(dataset.id)
                elif dataset.connection_id is not None:
                    table_connector_map[materialized_dataset_key] = dataset.connection_id
                else:
                    raise BusinessValidationError(
                        f"Dataset '{dataset.id}' referenced by unified semantic dataset '{dataset_key}' has no execution binding."
                    )

        relationships_payload = [
            _normalize_unified_relationship_payload(relationship)
            for relationship in (relationships or [])
        ]
        metrics_payload: dict[str, Any] = {}
        for metric_name, metric_value in (metrics or {}).items():
            if hasattr(metric_value, "model_dump"):
                metrics_payload[metric_name] = metric_value.model_dump(
                    by_alias=True, exclude_none=True
                )
            elif isinstance(metric_value, Mapping):
                metrics_payload[metric_name] = dict(metric_value)
            else:
                metrics_payload[metric_name] = metric_value

        try:
            semantic_model, _ = build_unified_semantic_model(
                source_models=loaded_source_models,
                relationships=relationships_payload,
                metrics=metrics_payload or None,
            )
            return semantic_model, table_connector_map
        except (SemanticModelError, ValueError) as exc:
            raise BusinessValidationError(
                f"Unified semantic model failed validation: {exc}"
            ) from exc

    async def _build_federation_workflow_payload(
        self,
        *,
        organization_id: uuid.UUID,
        semantic_model: SemanticModel,
        source_semantic_model: SemanticModel,
        table_connector_map: Mapping[str, uuid.UUID],
    ) -> dict[str, Any]:
        if self._dataset_repository is None:
            raise BusinessValidationError("Dataset repository is required for unified query execution.")
        from langbridge.packages.federation.models import (
            FederationWorkflow,
            VirtualDataset,
            VirtualTableBinding,
        )

        workspace_id = str(organization_id)
        semantic_model_id = str(uuid.uuid4())
        workflow_dataset_id = f"unified_semantic_{organization_id.hex[:12]}_{semantic_model_id[:12]}"
        tables: dict[str, dict[str, Any]] = {}
        for dataset_key, semantic_dataset in semantic_model.datasets.items():
            source_dataset = source_semantic_model.datasets.get(dataset_key, semantic_dataset)
            dataset_ref = source_dataset.dataset_id
            if not dataset_ref:
                raise BusinessValidationError(
                    f"Unified semantic model dataset '{dataset_key}' must define dataset_id for federated execution."
                )
            try:
                referenced_dataset_id = uuid.UUID(str(dataset_ref))
            except (TypeError, ValueError) as exc:
                raise BusinessValidationError(
                    f"Unified semantic model dataset '{dataset_key}' has an invalid dataset_id."
                ) from exc
            dataset = await self._dataset_repository.get_for_workspace(
                dataset_id=referenced_dataset_id,
                workspace_id=organization_id,
            )
            if dataset is None:
                raise BusinessValidationError(
                    f"Dataset '{referenced_dataset_id}' referenced by unified semantic dataset '{dataset_key}' was not found."
                )
            binding, _ = build_binding_for_dataset(
                dataset,
                table_key=dataset_key,
                logical_schema=semantic_dataset.schema_name,
                logical_table=semantic_dataset.relation_name,
                logical_catalog=semantic_dataset.catalog_name,
            )
            tables[dataset_key] = binding

        relationships = [
            {
                "name": relationship.name,
                "left_table": relationship.source_dataset,
                "right_table": relationship.target_dataset,
                "join_type": relationship.type,
                "condition": relationship.join_condition,
            }
            for relationship in (semantic_model.relationships or [])
        ]
        workflow = FederationWorkflow(
            id=f"workflow_{workflow_dataset_id}",
            workspace_id=workspace_id,
            dataset=VirtualDataset(
                id=workflow_dataset_id,
                name="Unified Semantic Dataset",
                workspace_id=workspace_id,
                tables={table_key: VirtualTableBinding.model_validate(binding) for table_key, binding in tables.items()},
                relationships=[],
            ),
            broadcast_threshold_bytes=settings.FEDERATION_BROADCAST_THRESHOLD_BYTES,
            partition_count=settings.FEDERATION_PARTITION_COUNT,
            max_stage_retries=settings.FEDERATION_STAGE_MAX_RETRIES,
            stage_parallelism=settings.FEDERATION_STAGE_PARALLELISM,
        )
        payload = workflow.model_dump(mode="json")
        payload["dataset"]["relationships"] = relationships
        return payload

    @staticmethod
    def build_unified_execution_connector_id(*, organization_id: uuid.UUID) -> uuid.UUID:
        return uuid.uuid5(
            uuid.NAMESPACE_DNS,
            f"langbridge-unified-federation:{organization_id}",
        )

    @staticmethod
    def _normalize_model_ids(semantic_model_ids: Iterable[uuid.UUID]) -> list[uuid.UUID]:
        ordered_unique: list[uuid.UUID] = []
        seen: set[uuid.UUID] = set()
        for model_id in semantic_model_ids:
            if model_id in seen:
                continue
            seen.add(model_id)
            ordered_unique.append(model_id)
        if not ordered_unique:
            raise BusinessValidationError(
                "semantic_model_ids must include at least one model id."
            )
        return ordered_unique

    @staticmethod
    def _build_source_model_key(
        *,
        preferred_name: str | None,
        model_id: uuid.UUID,
        seen_keys: set[str],
    ) -> str:
        base = re.sub(r"[^0-9A-Za-z_]+", "_", str(preferred_name or "").strip()).strip("_")
        if not base:
            base = f"model_{model_id.hex[:8]}"
        candidate = base
        if candidate in seen_keys:
            candidate = f"{base}_{model_id.hex[:8]}"
        seen_keys.add(candidate)
        return candidate

    @staticmethod
    def load_model_payload(content_yaml: str) -> SemanticModel:
        try:
            return load_semantic_model(content_yaml)
        except SemanticModelError as exc:
            raise BusinessValidationError(
                f"Semantic model failed validation: {exc}"
            ) from exc

    @staticmethod
    def parse_unified_model_config_from_record(model_record: Any) -> UnifiedModelConfig | None:
        payload = SemanticQueryExecutionService._parse_model_payload_from_record(model_record)
        if payload is None:
            return None

        source_models_raw = payload.get("source_models") or payload.get("sourceModels")
        if not isinstance(source_models_raw, list):
            if isinstance(payload.get("semantic_models"), list):
                raise BusinessValidationError(
                    "Unified semantic model is missing source_models metadata required for execution."
                )
            return None
        try:
            unified_model = load_unified_semantic_model(payload)
        except SemanticModelError as exc:
            raise BusinessValidationError(
                f"Unified semantic model failed validation: {exc}"
            ) from exc

        semantic_model_ids = [
            source_model.id
            for source_model in unified_model.source_models
        ]
        if not semantic_model_ids:
            raise BusinessValidationError(
                "Unified semantic model is missing source model ids."
            )

        relationships = [
            relationship.model_dump(mode="json", by_alias=True, exclude_none=True)
            for relationship in (unified_model.relationships or [])
        ] or None
        metrics = {
            metric_name: metric.model_dump(mode="json", by_alias=True, exclude_none=True)
            for metric_name, metric in (unified_model.metrics or {}).items()
        } or None

        return UnifiedModelConfig(
            semantic_model_ids=semantic_model_ids,
            source_models=[
                UnifiedSemanticSourceModelRequest.model_validate(
                    source_model.model_dump(mode="json", by_alias=True, exclude_none=True)
                )
                for source_model in unified_model.source_models
            ],
            relationships=relationships,
            metrics=metrics,
        )

    @staticmethod
    def _parse_model_payload_from_record(model_record: Any) -> dict[str, Any] | None:
        content_json = getattr(model_record, "content_json", None)
        if isinstance(content_json, dict):
            return content_json
        if isinstance(content_json, str) and content_json.strip():
            try:
                parsed = json.loads(content_json)
                if isinstance(parsed, dict):
                    return parsed
            except json.JSONDecodeError:
                pass

        content_yaml = getattr(model_record, "content_yaml", None)
        if isinstance(content_yaml, str) and content_yaml.strip():
            try:
                parsed_yaml = yaml.safe_load(content_yaml)
                if isinstance(parsed_yaml, dict):
                    return parsed_yaml
            except Exception:
                return None
        return None










