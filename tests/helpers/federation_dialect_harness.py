from __future__ import annotations

import sqlite3
import tempfile
import uuid
from collections.abc import Mapping, Sequence
from dataclasses import dataclass
from pathlib import Path
from typing import Any

import sqlglot

from langbridge.connectors.builtin.sqlite.config import SqliteConnectorConfig
from langbridge.connectors.builtin.sqlite.connector import SqliteConnector
from langbridge.federation.connectors import SourceCapabilities
from langbridge.federation.connectors.sql import SqlConnectorRemoteSource
from langbridge.federation.executor import ArtifactStore
from langbridge.federation.models import (
    FederationWorkflow,
    TableStatistics,
    VirtualDataset,
    VirtualTableBinding,
)
from langbridge.federation.models.plans import StageDefinition, StageType
from langbridge.federation.planner import FederatedPlanner, PlanningOutput
from langbridge.federation.service import FederatedQueryService
from langbridge.federation.utils.sql import normalize_sql_dialect
from tests.helpers.sql_normalize import normalize_rows, normalize_sql


@dataclass(slots=True, frozen=True)
class SqliteTableFixture:
    name: str
    ddl: str
    rows: Sequence[Sequence[Any]]
    columns: Sequence[str]


class FederationDialectHarness:
    def __init__(self, tmp_path: Path) -> None:
        self._tmp_path = tmp_path
        self._planner = FederatedPlanner()

    def sqlite_source(
        self,
        *,
        source_id: str,
        database_name: str,
        tables: Sequence[SqliteTableFixture],
    ) -> SqlConnectorRemoteSource:
        database_path = self._tmp_path / database_name
        self._seed_sqlite_database(database_path=database_path, tables=tables)
        connector = SqliteConnector(config=SqliteConnectorConfig(location=str(database_path)))
        return SqlConnectorRemoteSource(
            source_id=source_id,
            connector=connector,
            dialect="sqlite",
        )

    def workflow(
        self,
        *,
        source_by_table: Mapping[str, str],
        table_names: Mapping[str, str] | None = None,
        table_schemas: Mapping[str, str | None] | None = None,
        table_catalogs: Mapping[str, str | None] | None = None,
        table_metadata: Mapping[str, Mapping[str, Any]] | None = None,
        workflow_id: str = "wf-dialect",
        workspace_id: str = "ws-dialect",
    ) -> FederationWorkflow:
        physical_names = dict(table_names or {})
        schemas = dict(table_schemas or {})
        catalogs = dict(table_catalogs or {})
        metadata_by_table = dict(table_metadata or {})
        return FederationWorkflow(
            id=workflow_id,
            workspace_id=workspace_id,
            dataset=VirtualDataset(
                id=f"{workflow_id}-dataset",
                name="Dialect test dataset",
                workspace_id=workspace_id,
                tables={
                    table_key: VirtualTableBinding(
                        table_key=table_key,
                        source_id=source_id,
                        connector_id=uuid.uuid5(uuid.NAMESPACE_DNS, source_id),
                        schema=schemas.get(table_key),
                        table=physical_names.get(table_key, table_key),
                        catalog=catalogs.get(table_key),
                        stats=TableStatistics(row_count_estimate=10, bytes_per_row=64),
                        metadata=dict(metadata_by_table.get(table_key, {})),
                    )
                    for table_key, source_id in source_by_table.items()
                },
            ),
        )

    def plan_sql(
        self,
        *,
        sql: str,
        workflow: FederationWorkflow,
        input_dialect: str,
        source_dialects: Mapping[str, str],
        source_capabilities: Mapping[str, SourceCapabilities] | None = None,
    ) -> PlanningOutput:
        return self._planner.plan_sql(
            sql=sql,
            dialect=input_dialect,
            local_dialect="duckdb",
            workflow=workflow,
            source_dialects=dict(source_dialects),
            source_capabilities=dict(source_capabilities or {}),
        )

    async def execute_sql(
        self,
        *,
        sql: str,
        workflow: FederationWorkflow,
        input_dialect: str,
        sources: Mapping[str, SqlConnectorRemoteSource],
    ) -> list[dict[str, Any]]:
        with tempfile.TemporaryDirectory() as artifact_dir:
            service = FederatedQueryService(artifact_store=ArtifactStore(base_dir=artifact_dir))
            try:
                handle = await service.execute(
                    query=sql,
                    dialect=input_dialect,
                    workspace_id=workflow.workspace_id,
                    workflow=workflow,
                    sources=dict(sources),
                )
                table = await service.fetch_arrow(handle)
                return normalize_rows(table.to_pylist())
            finally:
                await service.aclose()

    def assert_stage_sql_parses(
        self,
        *,
        stages: Sequence[StageDefinition],
        source_dialects: Mapping[str, str],
    ) -> None:
        for stage in stages:
            if stage.stage_type in {StageType.REMOTE_SCAN, StageType.REMOTE_FULL_QUERY}:
                if stage.subplan is None or not stage.subplan.sql:
                    raise AssertionError(f"Remote stage {stage.stage_id} did not include SQL.")
                dialect = source_dialects.get(str(stage.source_id or ""), "duckdb")
                self._assert_sql_parses(
                    sql=stage.subplan.sql,
                    dialect=dialect,
                    stage_id=stage.stage_id,
                )
            elif stage.stage_type == StageType.LOCAL_COMPUTE:
                if not stage.sql:
                    raise AssertionError(f"Local stage {stage.stage_id} did not include SQL.")
                self._assert_sql_parses(
                    sql=stage.sql,
                    dialect=stage.sql_dialect or "duckdb",
                    stage_id=stage.stage_id,
                )

    def assert_stage_types(
        self,
        *,
        stages: Sequence[StageDefinition],
        expected: Sequence[StageType],
    ) -> None:
        actual = self.stage_types(stages)
        expected_tuple = tuple(expected)
        if actual != expected_tuple:
            raise AssertionError(f"Expected stage types {expected_tuple}, got {actual}.")

    def assert_sql_fragments(
        self,
        *,
        sql_fragments: Sequence[str],
        required: Sequence[str] = (),
        forbidden: Sequence[str] = (),
    ) -> None:
        combined_sql = "\n".join(sql_fragments).upper()
        missing = [fragment for fragment in required if fragment.upper() not in combined_sql]
        present = [fragment for fragment in forbidden if fragment.upper() in combined_sql]
        if missing:
            raise AssertionError(f"Expected SQL fragment(s) missing: {missing}\n{combined_sql}")
        if present:
            raise AssertionError(f"Forbidden SQL fragment(s) present: {present}\n{combined_sql}")

    def stage_types(self, stages: Sequence[StageDefinition]) -> tuple[StageType, ...]:
        return tuple(stage.stage_type for stage in stages)

    def remote_sql_by_stage(self, stages: Sequence[StageDefinition]) -> dict[str, str]:
        return {
            stage.stage_id: stage.subplan.sql
            for stage in stages
            if stage.subplan is not None and stage.subplan.sql is not None
        }

    def local_sql_by_stage(self, stages: Sequence[StageDefinition]) -> dict[str, str]:
        return {
            stage.stage_id: stage.sql
            for stage in stages
            if stage.stage_type == StageType.LOCAL_COMPUTE and stage.sql is not None
        }

    def plan_contract(
        self,
        *,
        output: PlanningOutput,
        input_dialect: str,
        source_dialects: Mapping[str, str],
    ) -> dict[str, Any]:
        return {
            "pushdown_full_query": output.physical_plan.pushdown_full_query,
            "pushdown_reasons": list(output.physical_plan.pushdown_reasons),
            "result_stage_id": output.physical_plan.result_stage_id,
            "stages": [
                self._stage_contract(
                    stage=stage,
                    input_dialect=input_dialect,
                    source_dialects=source_dialects,
                )
                for stage in output.physical_plan.stages
            ],
        }

    def _seed_sqlite_database(
        self,
        *,
        database_path: Path,
        tables: Sequence[SqliteTableFixture],
    ) -> None:
        with sqlite3.connect(database_path) as connection:
            cursor = connection.cursor()
            for table in tables:
                cursor.execute(f"DROP TABLE IF EXISTS {table.name}")
                cursor.execute(table.ddl)
                if table.rows:
                    placeholders = ", ".join(["?"] * len(table.columns))
                    columns = ", ".join(table.columns)
                    cursor.executemany(
                        f"INSERT INTO {table.name} ({columns}) VALUES ({placeholders})",
                        list(table.rows),
                    )
            connection.commit()

    def _assert_sql_parses(self, *, sql: str, dialect: str, stage_id: str) -> None:
        normalized_dialect = normalize_sql_dialect(dialect, default="duckdb")
        try:
            sqlglot.parse_one(sql, read=normalized_dialect)
        except sqlglot.ParseError as exc:
            raise AssertionError(
                f"Generated SQL for stage {stage_id} did not parse as {normalized_dialect}: {sql}"
            ) from exc

    def _stage_contract(
        self,
        *,
        stage: StageDefinition,
        input_dialect: str,
        source_dialects: Mapping[str, str],
    ) -> dict[str, Any]:
        payload: dict[str, Any] = {
            "stage_id": stage.stage_id,
            "stage_type": stage.stage_type.value,
            "source_id": stage.source_id,
            "dependencies": list(stage.dependencies),
            "sql": None,
            "subplan": None,
        }
        if stage.sql is not None:
            payload["sql"] = normalize_sql(
                stage.sql,
                read_dialect=input_dialect,
                write_dialect=stage.sql_dialect or "duckdb",
            )
        if stage.subplan is not None:
            source_dialect = source_dialects.get(stage.subplan.source_id, input_dialect)
            payload["subplan"] = {
                "stage_id": stage.subplan.stage_id,
                "source_id": stage.subplan.source_id,
                "alias": stage.subplan.alias,
                "table_key": stage.subplan.table_key,
                "sql": normalize_sql(
                    stage.subplan.sql or "",
                    read_dialect=source_dialect,
                    write_dialect=source_dialect,
                ),
                "projected_columns": list(stage.subplan.projected_columns),
                "pushed_filters": list(stage.subplan.pushed_filters),
                "pushed_limit": stage.subplan.pushed_limit,
                "pushdown": {
                    "filter": stage.subplan.pushdown.filter.pushed,
                    "projection": stage.subplan.pushdown.projection.pushed,
                    "aggregation": stage.subplan.pushdown.aggregation.pushed,
                    "limit": stage.subplan.pushdown.limit.pushed,
                    "join": stage.subplan.pushdown.join.pushed,
                    "full_query": stage.subplan.pushdown.full_query.pushed,
                },
            }
        return payload
