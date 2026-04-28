
import tempfile
import uuid
from collections.abc import Mapping
from pathlib import Path
from typing import Any

import pandas as pd
import pyarrow as pa

from langbridge.federation.executor import ArtifactStore
from langbridge.federation.models import (
    FederationWorkflow,
    SMQQuery,
    TableStatistics,
    VirtualDataset,
    VirtualRelationship,
    VirtualTableBinding,
)
from langbridge.federation.planner import FederatedPlanner
from langbridge.federation.service import FederatedQueryService
from langbridge.semantic.model import SemanticModel

from tests.helpers.federation_mock import MockArrowRemoteSource
from tests.helpers.semantic_harness import SemanticHarness
from tests.helpers.sql_normalize import normalize_rows, normalize_sql


class FederationHarness:
    def __init__(self, fixture_root: Path | None = None) -> None:
        self.semantic = SemanticHarness(fixture_root=fixture_root)
        self.fixture_root = self.semantic.fixture_root

    def load_query_fixture(self, name: str) -> SMQQuery:
        return SMQQuery.model_validate(
            self.semantic.read_yaml("queries", "federation", f"{name}.yml")
        )

    def build_workflow_for_model(
        self,
        *,
        model: SemanticModel,
        source_by_dataset: Mapping[str, str],
        stats_by_dataset: Mapping[str, TableStatistics] | None = None,
        workspace_id: str = "workspace-fixture",
        workflow_id: str = "workflow-fixture",
        dataset_id: str = "virtual-dataset-fixture",
    ) -> FederationWorkflow:
        tables: dict[str, VirtualTableBinding] = {}
        for dataset_key, dataset in model.datasets.items():
            source_id = source_by_dataset[dataset_key]
            tables[dataset_key] = VirtualTableBinding(
                table_key=dataset_key,
                source_id=source_id,
                connector_id=uuid.uuid5(uuid.NAMESPACE_DNS, source_id),
                schema=dataset.schema_name,
                table=dataset.get_relation_name(dataset_key),
                catalog=dataset.catalog_name,
                stats=(stats_by_dataset or {}).get(dataset_key),
            )

        relationships = [
            VirtualRelationship(
                name=relationship.name,
                left_table=relationship.source_dataset,
                right_table=relationship.target_dataset,
                join_type=relationship.type,
                condition=relationship.join_condition,
            )
            for relationship in (model.relationships or [])
        ]

        return FederationWorkflow(
            id=workflow_id,
            workspace_id=workspace_id,
            dataset=VirtualDataset(
                id=dataset_id,
                name="Fixture Dataset",
                workspace_id=workspace_id,
                tables=tables,
                relationships=relationships,
            ),
        )

    def plan_smq(
        self,
        *,
        model: SemanticModel,
        query_name: str,
        workflow: FederationWorkflow,
        dialect: str,
        source_dialects: Mapping[str, str],
    ) -> Any:
        planner = FederatedPlanner()
        return planner.plan_smq(
            query=self.load_query_fixture(query_name),
            semantic_model=model,
            dialect=dialect,
            workflow=workflow,
            source_dialects=dict(source_dialects),
        )

    def plan_sql(
        self,
        *,
        sql: str,
        workflow: FederationWorkflow,
        dialect: str,
        source_dialects: Mapping[str, str],
    ) -> Any:
        planner = FederatedPlanner()
        return planner.plan_sql(
            sql=sql,
            dialect=dialect,
            workflow=workflow,
            source_dialects=dict(source_dialects),
        )

    async def execute_sql(
        self,
        *,
        sql: str,
        workflow: FederationWorkflow,
        source_dialects: Mapping[str, str] | None = None,
    ) -> list[dict[str, Any]]:
        _ = source_dialects
        with tempfile.TemporaryDirectory() as artifact_dir:
            service = FederatedQueryService(
                artifact_store=ArtifactStore(base_dir=artifact_dir),
            )
            handle = await service.execute(
                query=sql,
                dialect="postgres",
                workspace_id=workflow.workspace_id,
                workflow=workflow,
                sources=self.build_mock_sources(workflow),
            )
            table = await service.fetch_arrow(handle)
            return normalize_rows(table.to_pylist())

    async def execute_smq(
        self,
        *,
        model: SemanticModel,
        query_name: str,
        workflow: FederationWorkflow,
        dialect: str,
    ) -> list[dict[str, Any]]:
        with tempfile.TemporaryDirectory() as artifact_dir:
            service = FederatedQueryService(
                artifact_store=ArtifactStore(base_dir=artifact_dir),
            )
            handle = await service.execute(
                query=self.load_query_fixture(query_name),
                dialect=dialect,
                workspace_id=workflow.workspace_id,
                workflow=workflow,
                sources=self.build_mock_sources(workflow),
                semantic_model=model,
            )
            table = await service.fetch_arrow(handle)
            return normalize_rows(table.to_pylist())

    def build_mock_sources(self, workflow: FederationWorkflow) -> dict[str, MockArrowRemoteSource]:
        tables_by_source: dict[str, dict[str, pa.Table]] = {}
        for table_key, binding in workflow.dataset.tables.items():
            csv_path = self.semantic._dataset_path(
                dataset_key=table_key,
                relation_name=binding.table,
            )
            arrow_table = pa.Table.from_pandas(pd.read_csv(csv_path), preserve_index=False)
            tables_by_source.setdefault(binding.source_id, {})[table_key] = arrow_table

        sources: dict[str, MockArrowRemoteSource] = {}
        for source_id, tables in tables_by_source.items():
            sources[source_id] = MockArrowRemoteSource(
                source_id=source_id,
                tables=tables,
                dialect="postgres",
            )
        return sources

    def normalize_planning_output(
        self,
        *,
        output: Any,
        input_dialect: str,
        source_dialects: Mapping[str, str],
    ) -> dict[str, Any]:
        logical_plan = output.logical_plan
        physical_plan = output.physical_plan
        return {
            "sql": normalize_sql(
                output.sql,
                read_dialect=input_dialect,
                write_dialect=input_dialect,
            ),
            "logical_plan": {
                "query_type": logical_plan.query_type.value,
                "from_alias": logical_plan.from_alias,
                "tables": {
                    alias: {
                        "table_key": table_ref.table_key,
                        "source_id": table_ref.source_id,
                        "schema_name": table_ref.schema_name,
                        "table": table_ref.table,
                        "catalog": table_ref.catalog,
                    }
                    for alias, table_ref in sorted(logical_plan.tables.items())
                },
                "joins": [
                    {
                        "left_alias": join.left_alias,
                        "right_alias": join.right_alias,
                        "join_type": join.join_type,
                        "on_sql": normalize_sql(
                            f"SELECT * FROM fixture WHERE {join.on_sql}",
                            read_dialect=input_dialect,
                            write_dialect=input_dialect,
                        ).replace("SELECT * FROM fixture WHERE ", ""),
                    }
                    for join in logical_plan.joins
                ],
                "where_sql": (
                    normalize_sql(
                        f"SELECT * FROM fixture WHERE {logical_plan.where_sql}",
                        read_dialect=input_dialect,
                        write_dialect=input_dialect,
                    ).replace("SELECT * FROM fixture WHERE ", "")
                    if logical_plan.where_sql
                    else None
                ),
                "group_by_sql": [
                    normalize_sql(
                        f"SELECT {group_item} FROM fixture",
                        read_dialect=input_dialect,
                        write_dialect=input_dialect,
                    ).replace("SELECT ", "").replace(" FROM fixture", "")
                    for group_item in logical_plan.group_by_sql
                ],
                "order_by_sql": [
                    normalize_sql(
                        f"SELECT * FROM fixture ORDER BY {order_item}",
                        read_dialect=input_dialect,
                        write_dialect=input_dialect,
                    ).replace("SELECT * FROM fixture ORDER BY ", "")
                    for order_item in logical_plan.order_by_sql
                ],
                "limit": logical_plan.limit,
                "offset": logical_plan.offset,
                "has_cte": logical_plan.has_cte,
            },
            "physical_plan": {
                "result_stage_id": physical_plan.result_stage_id,
                "join_order": list(physical_plan.join_order),
                "join_strategies": {
                    key: value.value
                    for key, value in sorted(physical_plan.join_strategies.items())
                },
                "stages": [
                    self._normalize_stage(
                        stage=stage,
                        input_dialect=input_dialect,
                        source_dialects=source_dialects,
                    )
                    for stage in physical_plan.stages
                ],
            },
        }

    def expected_plan(self, name: str) -> dict[str, Any]:
        path = self.fixture_root / "expected" / "federation_plans" / f"{name}.json"
        if path.exists():
            payload = self.semantic.read_json("expected", "federation_plans", f"{name}.json")
        else:
            payload = self.semantic.read_json("expected", "federation_plans", "goldens.json")[name]
        if not isinstance(payload, dict):
            raise ValueError(f"Expected federation plan {name} must contain a mapping.")
        return payload

    def _normalize_stage(
        self,
        *,
        stage: Any,
        input_dialect: str,
        source_dialects: Mapping[str, str],
    ) -> dict[str, Any]:
        source_dialect = source_dialects.get(stage.source_id or "", input_dialect)
        payload: dict[str, Any] = {
            "stage_id": stage.stage_id,
            "stage_type": stage.stage_type.value,
            "dependencies": list(stage.dependencies),
            "source_id": stage.source_id,
            "sql": None,
            "subplan": None,
        }
        if stage.sql is not None:
            payload["sql"] = normalize_sql(
                stage.sql,
                read_dialect=input_dialect,
                write_dialect="duckdb",
            )
        if stage.subplan is not None:
            payload["subplan"] = {
                "stage_id": stage.subplan.stage_id,
                "source_id": stage.subplan.source_id,
                "alias": stage.subplan.alias,
                "table_key": stage.subplan.table_key,
                "sql": normalize_sql(
                    stage.subplan.sql,
                    read_dialect=source_dialect,
                    write_dialect=source_dialect,
                ),
                "projected_columns": sorted(stage.subplan.projected_columns),
                "pushed_filters": sorted(
                    normalize_sql(
                        f"SELECT * FROM fixture WHERE {predicate}",
                        read_dialect=source_dialect,
                        write_dialect=source_dialect,
                    ).replace("SELECT * FROM fixture WHERE ", "")
                    for predicate in stage.subplan.pushed_filters
                ),
                "pushed_limit": stage.subplan.pushed_limit,
            }
        return payload
