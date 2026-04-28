from datetime import datetime, timezone
from typing import Any

import sqlglot

from langbridge.federation.models import FederationWorkflow
from langbridge.runtime.models import CreateSqlJobRequest, SqlJob
from langbridge.runtime.services.errors import ExecutionValidationError
from langbridge.runtime.services.sql_query.federation import SqlFederatedWorkflowBuilder


class SqlExplainResultWriter:
    """Stores logical and federated EXPLAIN payloads on SQL jobs."""

    def __init__(self, *, federation: SqlFederatedWorkflowBuilder) -> None:
        self._federation = federation

    def store_single_result(
        self,
        job: SqlJob,
        request: CreateSqlJobRequest,
        rendered_query: str,
        *,
        source_dialect: str,
        target_dialect: str,
    ) -> None:
        try:
            expression = sqlglot.parse_one(rendered_query, read=target_dialect)
            normalized_sql = expression.sql(dialect=target_dialect)
            table_refs = [
                {
                    "schema": (table.db or None),
                    "table": table.name,
                }
                for table in expression.find_all(sqlglot.exp.Table)
            ]
        except sqlglot.ParseError as exc:
            raise ExecutionValidationError(f"EXPLAIN parse failed: {exc}") from exc

        now = datetime.now(timezone.utc)
        job.status = "succeeded"
        job.result_columns_json = [
            {"name": "section", "type": "string"},
            {"name": "value", "type": "string"},
        ]
        job.result_rows_json = [
            {"section": "mode", "value": "logical"},
            {"section": "source_dialect", "value": source_dialect},
            {"section": "target_dialect", "value": target_dialect},
            {"section": "normalized_sql", "value": normalized_sql},
            {"section": "table_count", "value": str(len(table_refs))},
        ]
        job.row_count_preview = len(job.result_rows_json)
        job.total_rows_estimate = None
        job.bytes_scanned = None
        job.duration_ms = 0
        job.result_cursor = "0"
        job.redaction_applied = False
        job.error_json = None
        job.finished_at = now
        job.updated_at = now
        job.stats_json = {
            "explain": {
                "mode": "logical",
                "tables": table_refs,
                "query_hash": job.query_hash,
            }
        }

    def store_federated_result(
        self,
        *,
        job: SqlJob,
        request: CreateSqlJobRequest,
        explain_payload: dict[str, Any],
        query_sql: str,
        source_dialect: str,
        workflow: FederationWorkflow,
    ) -> None:
        logical_plan = explain_payload.get("logical_plan") if isinstance(explain_payload, dict) else {}
        physical_plan = explain_payload.get("physical_plan") if isinstance(explain_payload, dict) else {}
        logical_tables = logical_plan.get("tables") if isinstance(logical_plan, dict) else {}
        logical_joins = logical_plan.get("joins") if isinstance(logical_plan, dict) else []
        physical_stages = physical_plan.get("stages") if isinstance(physical_plan, dict) else []
        federation_diagnostics = self._federation.build_diagnostics(
            workflow=workflow,
            planning_payload=explain_payload,
            execution_payload=None,
        )

        now = datetime.now(timezone.utc)
        job.status = "succeeded"
        job.result_columns_json = [
            {"name": "section", "type": "string"},
            {"name": "value", "type": "string"},
        ]
        job.result_rows_json = [
            {"section": "mode", "value": "federated"},
            {"section": "source_dialect", "value": source_dialect},
            {"section": "normalized_sql", "value": query_sql},
            {
                "section": "source_alias_count",
                "value": str(len(request.federated_datasets or [])),
            },
            {
                "section": "table_count",
                "value": str(len(logical_tables) if isinstance(logical_tables, dict) else 0),
            },
            {
                "section": "join_count",
                "value": str(len(logical_joins) if isinstance(logical_joins, list) else 0),
            },
            {
                "section": "stage_count",
                "value": str(len(physical_stages) if isinstance(physical_stages, list) else 0),
            },
        ]
        job.row_count_preview = len(job.result_rows_json)
        job.total_rows_estimate = None
        job.bytes_scanned = None
        job.duration_ms = 0
        job.result_cursor = "0"
        job.redaction_applied = False
        job.error_json = None
        job.finished_at = now
        job.updated_at = now
        job.stats_json = {
            "explain": {
                "mode": "federated",
                "query_hash": job.query_hash,
                "workflow": workflow.model_dump(mode="json"),
                "plan": explain_payload,
            },
            "federation_diagnostics": (
                federation_diagnostics.model_dump(mode="json")
                if federation_diagnostics is not None
                else None
            ),
        }
