from __future__ import annotations

import asyncio
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from pathlib import Path

import httpx

from langbridge.packages.common.langbridge_common.contracts.jobs.agent_job import (
    AgentJobStateResponse,
    JobFinalResponse,
)
from langbridge.packages.common.langbridge_common.contracts.datasets import DatasetListResponse
from langbridge.packages.common.langbridge_common.contracts.sql import (
    SqlJobResultsResponse,
    SqlJobStatus,
    SqlJobResponse,
    SqlWorkbenchMode,
    SqlExecutionMode,
)
from langbridge.packages.common.langbridge_common.contracts.threads import ThreadResponse
from langbridge.packages.common.langbridge_common.db.threads import ThreadMessage
from langbridge import LangbridgeClient


class _FakeRuntimeHost:
    def __init__(self, *, user_id: uuid.UUID) -> None:
        self.context = SimpleNamespace(
            workspace_id=uuid.uuid4(),
            user_id=user_id,
            request_id="sdk-local-request",
        )

    async def query_dataset(self, *, request):
        return {
            "columns": [{"name": "order_id", "data_type": "integer"}],
            "rows": [{"order_id": 1}],
            "row_count_preview": 1,
            "effective_limit": request.enforced_limit,
            "redaction_applied": False,
            "duration_ms": 4,
            "bytes_scanned": 128,
        }

    async def execute_sql(self, *, request):
        return {
            "columns": [{"name": "value", "type": "integer"}],
            "rows": [{"value": 7}],
            "row_count_preview": 1,
            "total_rows_estimate": 1,
            "bytes_scanned": 64,
            "duration_ms": 5,
            "redaction_applied": False,
        }

    async def create_agent(self, *, job_id, request, event_emitter=None):
        return SimpleNamespace(
            response={
                "summary": f"Answered for {request.thread_id}",
                "result": {"text": "hello"},
                "visualization": None,
            }
        )


class _FakeThreadRepository:
    def __init__(self) -> None:
        self.items: dict[uuid.UUID, object] = {}

    def add(self, thread) -> None:
        self.items[thread.id] = thread

    async def get_by_id(self, thread_id: uuid.UUID):
        return self.items.get(thread_id)


class _FakeThreadMessageRepository:
    def __init__(self) -> None:
        self.items: list[ThreadMessage] = []

    def add(self, message: ThreadMessage) -> None:
        self.items.append(message)


def test_remote_sdk_dataset_query_polls_preview_job() -> None:
    workspace_id = uuid.uuid4()
    dataset_id = uuid.uuid4()
    job_id = uuid.uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == f"/api/v1/datasets/{dataset_id}/preview":
            return httpx.Response(
                200,
                json={
                    "job_id": str(job_id),
                    "status": "queued",
                    "dataset_id": str(dataset_id),
                    "effective_limit": 10,
                },
            )
        if request.method == "GET" and request.url.path == f"/api/v1/datasets/{dataset_id}/preview/jobs/{job_id}":
            return httpx.Response(
                200,
                json={
                    "job_id": str(job_id),
                    "status": "succeeded",
                    "dataset_id": str(dataset_id),
                    "columns": [{"name": "order_id", "data_type": "integer"}],
                    "rows": [{"order_id": 1}],
                    "row_count_preview": 1,
                    "effective_limit": 10,
                    "redaction_applied": False,
                    "duration_ms": 3,
                    "bytes_scanned": 42,
                },
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://sdk.test")
    client = LangbridgeClient.for_remote_api(
        base_url="https://sdk.test",
        http_client=http_client,
        default_workspace_id=workspace_id,
    )

    result = client.datasets.query(dataset_id=dataset_id, limit=10)

    assert result.status == "succeeded"
    assert result.rows == [{"order_id": 1}]
    assert result.columns[0].name == "order_id"


def test_remote_sdk_list_datasets() -> None:
    workspace_id = uuid.uuid4()
    dataset_id = uuid.uuid4()

    payload = DatasetListResponse.model_validate(
        {
            "items": [
                {
                    "id": str(dataset_id),
                    "workspace_id": str(workspace_id),
                    "name": "orders",
                    "sql_alias": "orders",
                    "description": "Orders dataset",
                    "status": "published",
                    "dataset_type": "TABLE",
                    "source_kind": "database",
                    "storage_kind": "table",
                    "relation_identity": {
                        "canonical_reference": "orders",
                        "relation_name": "orders",
                        "source_kind": "database",
                        "storage_kind": "table",
                    },
                    "execution_capabilities": {},
                    "policy": {
                        "max_rows_preview": 100,
                        "max_export_rows": 1000,
                        "redaction_rules": {},
                        "row_filters": [],
                        "allow_dml": False,
                    },
                    "stats": {},
                    "created_at": datetime.now(timezone.utc).isoformat(),
                    "updated_at": datetime.now(timezone.utc).isoformat(),
                }
            ],
            "total": 1,
        }
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/v1/datasets":
            return httpx.Response(200, json=payload.model_dump(mode="json"))
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://sdk.test")
    client = LangbridgeClient.for_remote_api(
        base_url="https://sdk.test",
        http_client=http_client,
        default_workspace_id=workspace_id,
    )

    result = client.datasets.list()

    assert result.total == 1
    assert result.items[0].name == "orders"


def test_remote_sdk_sql_query_polls_job_and_fetches_results() -> None:
    workspace_id = uuid.uuid4()
    user_id = uuid.uuid4()
    sql_job_id = uuid.uuid4()

    job_response = SqlJobResponse(
        id=sql_job_id,
        workspace_id=workspace_id,
        user_id=user_id,
        workbench_mode=SqlWorkbenchMode.direct_sql,
        connection_id=uuid.uuid4(),
        selected_datasets=[],
        execution_mode=SqlExecutionMode.single,
        status=SqlJobStatus.succeeded,
        query="select 7 as value",
        query_hash="hash",
        enforced_limit=100,
        enforced_timeout_seconds=30,
        row_count_preview=1,
        total_rows_estimate=1,
        bytes_scanned=64,
        duration_ms=5,
        redaction_applied=False,
        created_at=datetime.now(timezone.utc),
        artifacts=[],
    )
    results_response = SqlJobResultsResponse(
        sql_job_id=sql_job_id,
        status=SqlJobStatus.succeeded,
        columns=[{"name": "value", "type": "integer"}],
        rows=[{"value": 7}],
        row_count_preview=1,
        total_rows_estimate=1,
        artifacts=[],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == "/api/v1/sql/execute":
            return httpx.Response(202, json={"sql_job_id": str(sql_job_id), "warnings": []})
        if request.method == "GET" and request.url.path == f"/api/v1/sql/jobs/{sql_job_id}":
            return httpx.Response(200, json=job_response.model_dump(mode="json"))
        if request.method == "GET" and request.url.path == f"/api/v1/sql/jobs/{sql_job_id}/results":
            return httpx.Response(200, json=results_response.model_dump(mode="json"))
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://sdk.test")
    client = LangbridgeClient.for_remote_api(
        base_url="https://sdk.test",
        http_client=http_client,
        default_workspace_id=workspace_id,
    )

    result = client.sql.query(query="select 7 as value", connection_id=uuid.uuid4())

    assert result.status == "succeeded"
    assert result.rows == [{"value": 7}]
    assert result.columns[0].name == "value"


def test_remote_sdk_agents_ask_creates_thread_and_polls_job() -> None:
    organization_id = uuid.uuid4()
    project_id = uuid.uuid4()
    thread_id = uuid.uuid4()
    agent_id = uuid.uuid4()
    job_id = uuid.uuid4()

    state = AgentJobStateResponse(
        id=job_id,
        job_type="agent",
        status="succeeded",
        progress=100,
        final_response=JobFinalResponse(summary="done", result={"text": "hello"}),
        events=[],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "POST" and request.url.path == f"/api/v1/thread/{organization_id}/":
            return httpx.Response(
                201,
                json=ThreadResponse(id=thread_id, project_id=project_id, title="sdk").model_dump(mode="json"),
            )
        if request.method == "POST" and request.url.path == f"/api/v1/thread/{organization_id}/{thread_id}/chat":
            return httpx.Response(200, json={"job_id": str(job_id), "job_status": "queued"})
        if request.method == "GET" and request.url.path == f"/api/v1/jobs/{organization_id}/{job_id}":
            return httpx.Response(200, json=state.model_dump(mode="json"))
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://sdk.test")
    client = LangbridgeClient.for_remote_api(
        base_url="https://sdk.test",
        http_client=http_client,
        default_organization_id=organization_id,
        default_project_id=project_id,
    )

    result = client.agents.ask(agent_id=agent_id, message="hello")

    assert result.status == "succeeded"
    assert result.thread_id == thread_id
    assert result.summary == "done"
    assert result.result == {"text": "hello"}


def test_local_sdk_dataset_and_sql_queries_use_runtime_adapter() -> None:
    user_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(user_id=user_id)
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        default_workspace_id=runtime_host.context.workspace_id,
        default_user_id=user_id,
    )

    dataset_result = client.datasets.query(dataset_id=uuid.uuid4(), limit=25)
    sql_result = client.sql.query(query="select 7 as value", connection_id=uuid.uuid4())

    assert dataset_result.status == "succeeded"
    assert dataset_result.row_count_preview == 1
    assert sql_result.status == "succeeded"
    assert sql_result.rows == [{"value": 7}]


def test_local_sdk_agents_ask_uses_runtime_and_thread_repositories() -> None:
    user_id = uuid.uuid4()
    organization_id = uuid.uuid4()
    project_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(user_id=user_id)
    thread_repository = _FakeThreadRepository()
    thread_message_repository = _FakeThreadMessageRepository()
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        thread_repository=thread_repository,
        thread_message_repository=thread_message_repository,
        default_organization_id=organization_id,
        default_project_id=project_id,
        default_user_id=user_id,
    )

    result = client.agents.ask(agent_id=uuid.uuid4(), message="hello local")

    assert result.status == "succeeded"
    assert result.summary is not None
    assert result.thread_id in thread_repository.items
    assert len(thread_message_repository.items) == 1


def test_local_sdk_from_config_supports_async_usage(tmp_path: Path) -> None:
    db_path = tmp_path / "example.db"
    connection = sqlite3.connect(db_path)
    cursor = connection.cursor()
    cursor.executescript(
        """
        CREATE TABLE orders_enriched (
            order_id TEXT PRIMARY KEY,
            order_date TEXT NOT NULL,
            country TEXT NOT NULL,
            net_revenue REAL NOT NULL,
            gross_margin REAL NOT NULL,
            acquisition_channel TEXT NOT NULL,
            loyalty_tier TEXT NOT NULL,
            order_status TEXT NOT NULL,
            customer_id INTEGER NOT NULL
        );
        INSERT INTO orders_enriched VALUES
            ('O-1', '2025-04-08', 'United Kingdom', 180.0, 72.0, 'Direct', 'Gold', 'fulfilled', 1001),
            ('O-2', '2025-05-14', 'United States', 210.0, 84.0, 'Paid Search', 'Silver', 'fulfilled', 1002),
            ('O-3', '2025-05-18', 'United Kingdom', 260.0, 101.0, 'Email', 'Gold', 'fulfilled', 1003);
        """
    )
    connection.commit()
    connection.close()

    config_path = tmp_path / "langbridge.yml"
    config_path.write_text(
        f"""
version: 1
connectors:
  - name: commerce_demo
    type: sqlite
    connection:
      path: {db_path.name}
datasets:
  - name: shopify_orders
    connector: commerce_demo
    semantic_model: commerce_performance
    default_time_dimension: order_date
    source:
      table: orders_enriched
semantic_models:
  - name: commerce_performance
    default: true
    model:
      version: "1"
      name: commerce_performance
      datasets:
        shopify_orders:
          relation_name: orders_enriched
          dimensions:
            - name: country
              expression: country
              type: string
              synonyms: [countries]
            - name: order_date
              expression: order_date
              type: time
          measures:
            - name: net_sales
              expression: net_revenue
              type: number
              aggregation: sum
              synonyms: [net revenue]
            - name: gross_margin
              expression: gross_margin
              type: number
              aggregation: sum
agents:
  - name: commerce_analyst
    semantic_model: commerce_performance
    dataset: shopify_orders
    default: true
""".strip(),
        encoding="utf-8",
    )

    async def run_flow() -> None:
        client = LangbridgeClient.local(config_path=str(config_path))
        datasets = await client.datasets.list()
        result = await client.datasets.query(
            "shopify_orders",
            metrics=["net_sales"],
            dimensions=["country"],
            order={"net_sales": "desc"},
            limit=5,
        )
        answer = await client.agents.ask("Show me top countries by net sales this quarter")

        assert datasets.total == 1
        assert datasets.items[0].name == "shopify_orders"
        assert result.status == "succeeded"
        assert result.rows[0]["country"] == "United Kingdom"
        assert answer.status == "succeeded"
        assert "United Kingdom" in str(answer.text)

    asyncio.run(run_flow())
