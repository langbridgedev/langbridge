from __future__ import annotations

import sqlite3
import uuid
from dataclasses import dataclass
from datetime import date, datetime
from pathlib import Path
from typing import Any

import yaml
from pydantic import BaseModel, ConfigDict, Field

from langbridge.packages.runtime.context import RuntimeContext
from langbridge.packages.semantic.langbridge_semantic.model import SemanticModel
from langbridge.packages.semantic.langbridge_semantic.query import (
    SemanticQuery,
    SemanticQueryEngine,
)


class LocalRuntimeConnectorConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    type: str
    connection: dict[str, Any] = Field(default_factory=dict)


class LocalRuntimeDatasetSourceConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    table: str | None = None
    sql: str | None = None


class LocalRuntimeDatasetConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    label: str | None = None
    description: str | None = None
    connector: str
    source: LocalRuntimeDatasetSourceConfig
    semantic_model: str | None = None
    default_time_dimension: str | None = None


class LocalRuntimeSemanticModelConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    description: str | None = None
    default: bool = False
    model: dict[str, Any] | None = None
    datasets: list[str] = Field(default_factory=list)


class LocalRuntimeAgentConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    name: str
    description: str | None = None
    semantic_model: str
    dataset: str
    default: bool = False
    instructions: str | None = None


class LocalRuntimeConfig(BaseModel):
    model_config = ConfigDict(extra="allow")

    version: int | str = 1
    runtime: dict[str, Any] = Field(default_factory=dict)
    connectors: list[LocalRuntimeConnectorConfig] = Field(default_factory=list)
    datasets: list[LocalRuntimeDatasetConfig] = Field(default_factory=list)
    semantic_models: list[LocalRuntimeSemanticModelConfig] = Field(default_factory=list)
    agents: list[LocalRuntimeAgentConfig] = Field(default_factory=list)


@dataclass(slots=True, frozen=True)
class LocalRuntimeDatasetRecord:
    id: uuid.UUID
    name: str
    label: str
    description: str | None
    connector_name: str
    relation_name: str
    semantic_model_name: str | None
    default_time_dimension: str | None


def _stable_uuid(namespace: str, value: str) -> uuid.UUID:
    return uuid.uuid5(uuid.NAMESPACE_URL, f"langbridge:{namespace}:{value}")


def _connect_sqlite(db_path: Path) -> sqlite3.Connection:
    connection = sqlite3.connect(str(db_path))
    connection.row_factory = sqlite3.Row
    return connection


def _rows_to_payload(cursor: sqlite3.Cursor) -> tuple[list[dict[str, Any]], list[dict[str, Any]]]:
    columns = [{"name": str(item[0]), "data_type": None} for item in (cursor.description or [])]
    rows = [{key: row[key] for key in row.keys()} for row in cursor.fetchall()]
    return columns, rows


class ConfiguredLocalRuntimeHost:
    def __init__(
        self,
        *,
        config_path: str | Path,
        context: RuntimeContext,
    ) -> None:
        self._config_path = Path(config_path).resolve()
        self.context = context
        self._config = self._load_config(self._config_path)
        self._connectors = {connector.name: connector for connector in self._config.connectors}
        self._datasets = self._build_dataset_records()
        self._datasets_by_id = {record.id: record for record in self._datasets.values()}
        self._semantic_models = self._build_semantic_models()
        self._agents = {agent.name: agent for agent in self._config.agents}
        self._default_agent = next((agent for agent in self._config.agents if agent.default), None)
        self._default_semantic_model = next(
            (model.name for model in self._config.semantic_models if model.default),
            self._config.semantic_models[0].name if self._config.semantic_models else None,
        )
        self._engine = SemanticQueryEngine()

    @staticmethod
    def _load_config(path: Path) -> LocalRuntimeConfig:
        payload = yaml.safe_load(path.read_text(encoding="utf-8")) or {}
        return LocalRuntimeConfig.model_validate(payload)

    def _build_dataset_records(self) -> dict[str, LocalRuntimeDatasetRecord]:
        records: dict[str, LocalRuntimeDatasetRecord] = {}
        for dataset in self._config.datasets:
            relation_name = str(dataset.source.table or "").strip()
            if not relation_name:
                raise ValueError(f"Dataset '{dataset.name}' must define source.table for local runtime.")
            records[dataset.name] = LocalRuntimeDatasetRecord(
                id=_stable_uuid("dataset", f"{self._config_path}:{dataset.name}"),
                name=dataset.name,
                label=dataset.label or dataset.name.replace("_", " ").title(),
                description=dataset.description,
                connector_name=dataset.connector,
                relation_name=relation_name,
                semantic_model_name=dataset.semantic_model,
                default_time_dimension=dataset.default_time_dimension,
            )
        return records

    def _build_semantic_models(self) -> dict[str, SemanticModel]:
        models: dict[str, SemanticModel] = {}
        for item in self._config.semantic_models:
            payload = dict(item.model or {})
            if not payload:
                payload = {
                    "version": "1",
                    "name": item.name,
                    "datasets": {
                        dataset_name: {
                            "relation_name": self._datasets[dataset_name].relation_name,
                        }
                        for dataset_name in item.datasets
                        if dataset_name in self._datasets
                    },
                }
            models[item.name] = SemanticModel.model_validate(payload)
        return models

    def _resolve_connector_path(self, connector_name: str) -> Path:
        connector = self._connectors.get(connector_name)
        if connector is None:
            raise ValueError(f"Unknown connector '{connector_name}'.")
        connector_type = str(connector.type).strip().lower()
        if connector_type != "sqlite":
            raise ValueError(f"Local config runtime currently supports sqlite connectors, not '{connector.type}'.")
        path_value = connector.connection.get("path")
        if not path_value:
            raise ValueError(f"Connector '{connector_name}' is missing connection.path.")
        db_path = Path(str(path_value))
        if not db_path.is_absolute():
            db_path = (self._config_path.parent / db_path).resolve()
        return db_path

    async def list_datasets(self) -> list[dict[str, Any]]:
        return [
            {
                "id": record.id,
                "name": record.name,
                "label": record.label,
                "description": record.description,
                "connector": record.connector_name,
                "semantic_model": record.semantic_model_name,
            }
            for record in self._datasets.values()
        ]

    async def query_dataset(self, *, request) -> dict[str, Any]:
        dataset_record = self._datasets_by_id.get(request.dataset_id)
        if dataset_record is None:
            raise ValueError(f"Unknown dataset id: {request.dataset_id}")
        limit = int(request.enforced_limit or request.requested_limit or 10)
        sql = f"SELECT * FROM {dataset_record.relation_name} LIMIT {limit}"
        with _connect_sqlite(self._resolve_connector_path(dataset_record.connector_name)) as connection:
            cursor = connection.execute(sql)
            columns, rows = _rows_to_payload(cursor)
        return {
            "columns": columns,
            "rows": rows,
            "row_count_preview": len(rows),
            "effective_limit": limit,
            "redaction_applied": False,
            "duration_ms": 5,
            "bytes_scanned": None,
            "generated_sql": sql,
        }

    async def query_dataset_by_name(
        self,
        *,
        dataset_name: str,
        metrics: list[str] | None = None,
        dimensions: list[str] | None = None,
        filters: list[dict[str, Any]] | None = None,
        limit: int | None = None,
        order: dict[str, str] | list[dict[str, str]] | None = None,
        time_dimensions: list[dict[str, Any]] | None = None,
    ) -> dict[str, Any]:
        dataset_record = self._datasets.get(dataset_name)
        if dataset_record is None:
            raise ValueError(f"Unknown dataset '{dataset_name}'.")

        if not metrics and not dimensions and not time_dimensions:
            preview_limit = int(limit or 10)
            return await self._preview_dataset_by_name(dataset_record=dataset_record, limit=preview_limit)

        semantic_model_name = dataset_record.semantic_model_name or self._default_semantic_model
        if semantic_model_name is None:
            raise ValueError(f"Dataset '{dataset_name}' does not have a semantic model configured.")
        semantic_model = self._semantic_models.get(semantic_model_name)
        if semantic_model is None:
            raise ValueError(f"Semantic model '{semantic_model_name}' is not configured.")
        if dataset_name not in semantic_model.datasets:
            raise ValueError(
                f"Semantic model '{semantic_model_name}' does not define dataset '{dataset_name}'."
            )
        semantic_dataset = semantic_model.datasets[dataset_name]
        sql = self._build_semantic_sql(
            dataset_name=dataset_name,
            relation_name=dataset_record.relation_name,
            semantic_dataset=semantic_dataset,
            metrics=metrics or [],
            dimensions=dimensions or [],
            filters=filters or [],
            time_dimensions=time_dimensions or [],
            order=order,
            limit=limit,
        )
        with _connect_sqlite(self._resolve_connector_path(dataset_record.connector_name)) as connection:
            cursor = connection.execute(sql)
            columns, rows = _rows_to_payload(cursor)
        return {
            "dataset_id": dataset_record.id,
            "dataset_name": dataset_name,
            "columns": columns,
            "rows": rows,
            "row_count_preview": len(rows),
            "effective_limit": int(limit or 100),
            "redaction_applied": False,
            "duration_ms": 8,
            "bytes_scanned": None,
            "generated_sql": sql,
        }

    async def _preview_dataset_by_name(
        self,
        *,
        dataset_record: LocalRuntimeDatasetRecord,
        limit: int,
    ) -> dict[str, Any]:
        sql = f"SELECT * FROM {dataset_record.relation_name} LIMIT {limit}"
        with _connect_sqlite(self._resolve_connector_path(dataset_record.connector_name)) as connection:
            cursor = connection.execute(sql)
            columns, rows = _rows_to_payload(cursor)
        return {
            "dataset_id": dataset_record.id,
            "dataset_name": dataset_record.name,
            "columns": columns,
            "rows": rows,
            "row_count_preview": len(rows),
            "effective_limit": limit,
            "redaction_applied": False,
            "duration_ms": 5,
            "bytes_scanned": None,
            "generated_sql": sql,
        }

    async def execute_sql_text(
        self,
        *,
        query: str,
        connection_name: str | None = None,
        requested_limit: int | None = None,
    ) -> dict[str, Any]:
        sql = str(query or "").strip()
        if not sql.lower().startswith(("select", "with")):
            raise ValueError("Local runtime SQL execution only supports read-only SELECT statements.")
        connector_name = connection_name or next(iter(self._connectors), None)
        if connector_name is None:
            raise ValueError("No connectors are configured for the local runtime.")
        with _connect_sqlite(self._resolve_connector_path(connector_name)) as connection:
            cursor = connection.execute(sql)
            columns, rows = _rows_to_payload(cursor)
        preview_rows = rows[: requested_limit or len(rows)]
        return {
            "columns": [{"name": item["name"], "type": item["data_type"]} for item in columns],
            "rows": preview_rows,
            "row_count_preview": len(preview_rows),
            "total_rows_estimate": len(rows),
            "bytes_scanned": None,
            "duration_ms": 6,
            "redaction_applied": False,
            "generated_sql": sql,
        }

    async def execute_sql(self, *, request) -> dict[str, Any]:
        connection_name = None
        connection_id = getattr(request, "connection_id", None)
        if connection_id is not None:
            connection_name = next(iter(self._connectors), None)
        return await self.execute_sql_text(
            query=request.query,
            connection_name=connection_name,
            requested_limit=getattr(request, "requested_limit", None),
        )

    async def ask_agent(
        self,
        *,
        prompt: str,
        agent_name: str | None = None,
    ) -> dict[str, Any]:
        agent = self._resolve_agent(agent_name)
        semantic_model = self._semantic_models.get(agent.semantic_model)
        if semantic_model is None:
            raise ValueError(f"Agent '{agent.name}' references unknown semantic model '{agent.semantic_model}'.")
        dataset_name = agent.dataset
        dimension = self._infer_dimension(prompt=prompt, semantic_model=semantic_model, dataset_name=dataset_name)
        metric = self._infer_metric(prompt=prompt, semantic_model=semantic_model, dataset_name=dataset_name)
        limit = 5 if "top" in prompt.lower() else 10
        order = {metric: "desc"} if metric else None
        time_dimensions = self._infer_time_dimensions(
            prompt=prompt,
            dataset_name=dataset_name,
            dataset_record=self._datasets[dataset_name],
            semantic_model=semantic_model,
        )
        result = await self.query_dataset_by_name(
            dataset_name=dataset_name,
            metrics=[metric] if metric else [],
            dimensions=[dimension] if dimension else [],
            time_dimensions=time_dimensions,
            order=order,
            limit=limit,
        )
        summary = self._summarize_agent_response(
            prompt=prompt,
            metric=metric,
            dimension=dimension,
            rows=result["rows"],
        )
        visualization = None
        if metric and dimension:
            visualization = {
                "chart_type": "bar",
                "x": dimension,
                "y": metric,
                "title": f"{metric.replace('_', ' ').title()} by {dimension.replace('_', ' ').title()}",
            }
        return {
            "summary": summary,
            "result": {
                "columns": [column["name"] for column in result["columns"]],
                "rows": result["rows"],
                "generated_sql": result.get("generated_sql"),
            },
            "visualization": visualization,
        }

    def _resolve_agent(self, agent_name: str | None) -> LocalRuntimeAgentConfig:
        if agent_name:
            agent = self._agents.get(agent_name)
            if agent is None:
                raise ValueError(f"Unknown agent '{agent_name}'.")
            return agent
        if self._default_agent is not None:
            return self._default_agent
        if self._config.agents:
            return self._config.agents[0]
        raise ValueError("No agents are configured for this local runtime.")

    @staticmethod
    def _qualify_member(dataset_name: str, member: str) -> str:
        value = str(member or "").strip()
        if "." in value:
            return value
        return f"{dataset_name}.{value}"

    def _infer_dimension(
        self,
        *,
        prompt: str,
        semantic_model: SemanticModel,
        dataset_name: str,
    ) -> str | None:
        lowered = prompt.lower()
        dataset = semantic_model.datasets[dataset_name]
        for dimension in dataset.dimensions or []:
            candidates = {dimension.name.lower(), *(item.lower() for item in (dimension.synonyms or []))}
            if any(candidate in lowered for candidate in candidates):
                return dimension.name
            if dimension.name.endswith("y") and dimension.name[:-1] + "ies" in lowered:
                return dimension.name
        return next((dimension.name for dimension in (dataset.dimensions or []) if dimension.type != "time"), None)

    def _infer_metric(
        self,
        *,
        prompt: str,
        semantic_model: SemanticModel,
        dataset_name: str,
    ) -> str | None:
        lowered = prompt.lower()
        dataset = semantic_model.datasets[dataset_name]
        for measure in dataset.measures or []:
            candidates = {measure.name.lower(), *(item.lower() for item in (measure.synonyms or []))}
            if any(candidate in lowered for candidate in candidates):
                return measure.name
        return next((measure.name for measure in (dataset.measures or [])), None)

    def _infer_time_dimensions(
        self,
        *,
        prompt: str,
        dataset_name: str,
        dataset_record: LocalRuntimeDatasetRecord,
        semantic_model: SemanticModel,
    ) -> list[dict[str, Any]]:
        lowered = prompt.lower()
        if "quarter" not in lowered:
            return []
        time_dimension = dataset_record.default_time_dimension
        if not time_dimension:
            dataset = semantic_model.datasets[dataset_name]
            time_dimension = next(
                (dimension.name for dimension in (dataset.dimensions or []) if dimension.type == "time"),
                None,
            )
        if not time_dimension:
            return []
        max_date = self._get_max_date(
            connector_name=dataset_record.connector_name,
            relation_name=dataset_record.relation_name,
            column_name=time_dimension,
        )
        if max_date is None:
            return []
        quarter_start_month = ((max_date.month - 1) // 3) * 3 + 1
        quarter_start = date(max_date.year, quarter_start_month, 1)
        if quarter_start_month == 10:
            quarter_end = date(max_date.year, 12, 31)
        else:
            next_quarter = date(max_date.year, quarter_start_month + 3, 1)
            quarter_end = date.fromordinal(next_quarter.toordinal() - 1)
        return [
            {
                "dimension": time_dimension,
                "dateRange": [quarter_start.isoformat(), quarter_end.isoformat()],
            }
        ]

    def _get_max_date(
        self,
        *,
        connector_name: str,
        relation_name: str,
        column_name: str,
    ) -> date | None:
        sql = f"SELECT MAX({column_name}) AS max_value FROM {relation_name}"
        with _connect_sqlite(self._resolve_connector_path(connector_name)) as connection:
            row = connection.execute(sql).fetchone()
        if row is None or row["max_value"] is None:
            return None
        return datetime.fromisoformat(str(row["max_value"])).date()

    @staticmethod
    def _summarize_agent_response(
        *,
        prompt: str,
        metric: str | None,
        dimension: str | None,
        rows: list[dict[str, Any]],
    ) -> str:
        if not rows:
            return "No matching rows were found for the question."
        first_row = rows[0]
        if metric and dimension and dimension in first_row and metric in first_row:
            return (
                f"{first_row[dimension]} is leading for {metric.replace('_', ' ')} "
                f"at {first_row[metric]}."
            )
        return f"Answered question: {prompt}"

    def _sort_rows(
        self,
        *,
        rows: list[dict[str, Any]],
        order: dict[str, str] | list[dict[str, str]] | None,
    ) -> list[dict[str, Any]]:
        if not order or not rows:
            return rows
        entries = [order] if isinstance(order, dict) else order
        sorted_rows = list(rows)
        for entry in reversed(entries):
            for key, direction in entry.items():
                reverse = str(direction).strip().lower() == "desc"
                sorted_rows.sort(key=lambda row: row.get(key), reverse=reverse)
        return sorted_rows

    def _build_semantic_sql(
        self,
        *,
        dataset_name: str,
        relation_name: str,
        semantic_dataset: Any,
        metrics: list[str],
        dimensions: list[str],
        filters: list[dict[str, Any]],
        time_dimensions: list[dict[str, Any]],
        order: dict[str, str] | list[dict[str, str]] | None,
        limit: int | None,
    ) -> str:
        select_clauses: list[str] = []
        group_by_clauses: list[str] = []
        where_clauses: list[str] = []

        for requested_dimension in dimensions:
            dimension = self._resolve_dimension_definition(semantic_dataset, requested_dimension)
            expression = str(dimension.expression or dimension.name)
            select_clauses.append(f'{expression} AS "{requested_dimension}"')
            group_by_clauses.append(expression)

        for requested_metric in metrics:
            measure = self._resolve_measure_definition(semantic_dataset, requested_metric)
            expression = str(measure.expression or measure.name)
            aggregation = str(measure.aggregation or "sum").strip().lower()
            aggregate_sql = self._render_aggregate(aggregation=aggregation, expression=expression)
            select_clauses.append(f'{aggregate_sql} AS "{requested_metric}"')

        if not select_clauses:
            select_clauses.append("*")

        for filter_entry in filters:
            member = str(filter_entry.get("member") or "").strip()
            if not member:
                continue
            values = filter_entry.get("values") or []
            if not values:
                continue
            expression = self._resolve_member_expression(semantic_dataset, member)
            where_clauses.append(f"{expression} = {self._quote_literal(values[0])}")

        for item in time_dimensions:
            requested_dimension = str(item.get("dimension") or "").strip()
            if not requested_dimension:
                continue
            dimension = self._resolve_dimension_definition(semantic_dataset, requested_dimension)
            date_range = item.get("dateRange")
            if isinstance(date_range, list) and len(date_range) == 2:
                expression = str(dimension.expression or dimension.name)
                where_clauses.append(
                    f"{expression} BETWEEN {self._quote_literal(date_range[0])} AND {self._quote_literal(date_range[1])}"
                )

        sql_parts = [f'SELECT {", ".join(select_clauses)}', f"FROM {relation_name}"]
        if where_clauses:
            sql_parts.append(f'WHERE {" AND ".join(where_clauses)}')
        if group_by_clauses:
            sql_parts.append(f'GROUP BY {", ".join(group_by_clauses)}')
        order_clause = self._build_order_clause(order)
        if order_clause:
            sql_parts.append(order_clause)
        if limit:
            sql_parts.append(f"LIMIT {int(limit)}")
        return " ".join(sql_parts)

    @staticmethod
    def _render_aggregate(*, aggregation: str, expression: str) -> str:
        if aggregation in {"count", "count_distinct", "countdistinct"}:
            if aggregation in {"count_distinct", "countdistinct"}:
                return f"COUNT(DISTINCT {expression})"
            return f"COUNT({expression})"
        if aggregation in {"sum", "avg", "min", "max"}:
            return f"{aggregation.upper()}({expression})"
        return f"{aggregation.upper()}({expression})"

    def _resolve_dimension_definition(self, semantic_dataset: Any, requested_member: str) -> Any:
        normalized = str(requested_member or "").strip().lower().split(".")[-1]
        for dimension in semantic_dataset.dimensions or []:
            candidates = {dimension.name.lower(), *(item.lower() for item in (dimension.synonyms or []))}
            if normalized in candidates:
                return dimension
        raise ValueError(f"Unknown dimension '{requested_member}'.")

    def _resolve_measure_definition(self, semantic_dataset: Any, requested_member: str) -> Any:
        normalized = str(requested_member or "").strip().lower().split(".")[-1]
        for measure in semantic_dataset.measures or []:
            candidates = {measure.name.lower(), *(item.lower() for item in (measure.synonyms or []))}
            if normalized in candidates:
                return measure
        raise ValueError(f"Unknown metric '{requested_member}'.")

    def _resolve_member_expression(self, semantic_dataset: Any, requested_member: str) -> str:
        try:
            dimension = self._resolve_dimension_definition(semantic_dataset, requested_member)
            return str(dimension.expression or dimension.name)
        except ValueError:
            measure = self._resolve_measure_definition(semantic_dataset, requested_member)
            return str(measure.expression or measure.name)

    @staticmethod
    def _quote_literal(value: Any) -> str:
        escaped = str(value).replace("'", "''")
        return f"'{escaped}'"

    @staticmethod
    def _build_order_clause(order: dict[str, str] | list[dict[str, str]] | None) -> str | None:
        if not order:
            return None
        entries = [order] if isinstance(order, dict) else order
        parts: list[str] = []
        for entry in entries:
            for key, direction in entry.items():
                direction_sql = "DESC" if str(direction).strip().lower() == "desc" else "ASC"
                parts.append(f'"{key}" {direction_sql}')
        if not parts:
            return None
        return f'ORDER BY {", ".join(parts)}'


def build_configured_local_runtime(
    *,
    config_path: str | Path,
    tenant_id: uuid.UUID | None = None,
    workspace_id: uuid.UUID | None = None,
    user_id: uuid.UUID | None = None,
    roles: list[str] | tuple[str, ...] | None = None,
    request_id: str | None = None,
) -> ConfiguredLocalRuntimeHost:
    resolved_config_path = Path(config_path).resolve()
    runtime_tenant_id = tenant_id or _stable_uuid("tenant", str(resolved_config_path))
    context = RuntimeContext.build(
        tenant_id=runtime_tenant_id,
        workspace_id=workspace_id or runtime_tenant_id,
        user_id=user_id or _stable_uuid("user", str(resolved_config_path)),
        roles=roles,
        request_id=request_id or f"local-runtime:{resolved_config_path.name}",
    )
    return ConfiguredLocalRuntimeHost(config_path=resolved_config_path, context=context)


__all__ = [
    "ConfiguredLocalRuntimeHost",
    "LocalRuntimeConfig",
    "build_configured_local_runtime",
]
