
import asyncio
import json
import sqlite3
import uuid
from dataclasses import dataclass
from datetime import datetime, timezone
from types import SimpleNamespace
from pathlib import Path

import httpx

from langbridge import LangbridgeClient
from langbridge.client.client import (
    AgentJobStateResponse,
    JobFinalResponse,
    SqlJobResultsResponse,
    SqlJobResponse,
    ThreadResponse,
)
from langbridge.runtime.hosting.api_models import RuntimeDatasetListResponse
from langbridge.runtime.models import SqlWorkbenchMode


class _FakeRuntimeHost:
    def __init__(self, *, actor_id: uuid.UUID) -> None:
        self.context = SimpleNamespace(
            workspace_id=uuid.uuid4(),
            actor_id=actor_id,
            request_id="sdk-local-request",
        )
        self.execute_sql_calls: list[object] = []
        self.execute_sql_text_calls: list[dict[str, object]] = []
        self.query_semantic_calls: list[dict[str, object]] = []
        self.sync_calls: list[dict[str, object]] = []
        self.close_calls = 0

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
        self.execute_sql_calls.append(request)
        return {
            "columns": [{"name": "value", "type": "integer"}],
            "rows": [{"value": 7}],
            "row_count_preview": 1,
            "total_rows_estimate": 1,
            "bytes_scanned": 64,
            "duration_ms": 5,
            "redaction_applied": False,
        }

    async def execute_sql_text(
        self,
        *,
        query: str,
        connection_name: str | None = None,
        requested_limit: int | None = None,
    ):
        self.execute_sql_text_calls.append(
            {
                "query": query,
                "connection_name": connection_name,
                "requested_limit": requested_limit,
            }
        )
        return {
            "columns": [{"name": "value", "type": "integer"}],
            "rows": [{"value": 7}],
            "row_count_preview": 1,
            "total_rows_estimate": 1,
            "bytes_scanned": 64,
            "duration_ms": 5,
            "redaction_applied": False,
        }

    async def query_semantic_models(
        self,
        *,
        semantic_models,
        measures=None,
        dimensions=None,
        filters=None,
        limit=None,
        order=None,
        time_dimensions=None,
    ):
        self.query_semantic_calls.append(
            {
                "semantic_models": list(semantic_models or []),
                "measures": list(measures or []),
                "dimensions": list(dimensions or []),
                "filters": list(filters or []),
                "limit": limit,
                "order": order,
                "time_dimensions": list(time_dimensions or []),
            }
        )
        return {
            "rows": [{"shopify_orders.country": "United Kingdom", "shopify_orders.net_sales": 180.0}],
            "annotations": [{"member": "shopify_orders.net_sales"}],
            "metadata": [{"name": "shopify_orders.net_sales"}],
            "generated_sql": "SELECT country, SUM(net_revenue) AS net_sales FROM orders_enriched",
            "semantic_model_ids": [uuid.uuid4()],
        }

    async def ask_agent(self, *, prompt: str, agent_name: str | None = None):
        return {
            "thread_id": uuid.uuid4(),
            "job_id": uuid.uuid4(),
            "summary": f"Answered: {prompt}",
            "result": {"text": "hello"},
            "visualization": None,
        }

    async def create_agent(self, *, job_id, request, event_emitter=None):
        return SimpleNamespace(
            response={
                "summary": f"Answered for {request.thread_id}",
                "result": {"text": "hello"},
                "visualization": None,
            }
        )

    async def list_connectors(self):
        return [
            {
                "id": uuid.uuid4(),
                "name": "billing_demo",
                "connector_type": "STRIPE",
                "connector_family": "api",
                "supports_sync": True,
                "supported_resources": ["customers"],
                "default_sync_strategy": "INCREMENTAL",
                "capabilities": {
                    "supports_live_datasets": False,
                    "supports_synced_datasets": True,
                    "supports_incremental_sync": True,
                    "supports_query_pushdown": False,
                    "supports_preview": False,
                    "supports_federated_execution": False,
                },
                "managed": False,
            }
        ]

    async def list_sync_resources(self, *, connector_name: str):
        return [
            {
                "name": "customers",
                "label": "Customers",
                "supports_incremental": True,
                "default_sync_mode": "INCREMENTAL",
                "status": "never_synced",
                "dataset_ids": [],
                "dataset_names": [],
                "records_synced": 0,
            }
        ]

    async def list_sync_states(self, *, connector_name: str):
        return [
            {
                "resource_name": "customers",
                "status": "succeeded",
                "sync_mode": "INCREMENTAL",
                "dataset_ids": [],
                "dataset_names": ["stripe_demo_customers"],
                "records_synced": 2,
                "state": {},
            }
        ]

    async def sync_dataset(
        self,
        *,
        dataset_ref: str,
        sync_mode: str,
        force_full_refresh: bool,
    ):
        self.sync_calls.append(
            {
                "dataset_ref": dataset_ref,
                "sync_mode": sync_mode,
                "force_full_refresh": force_full_refresh,
            }
        )
        return {
            "status": "succeeded",
            "dataset_name": dataset_ref,
            "connector_name": "billing_demo",
            "sync_mode": sync_mode,
            "resources": [
                {
                    "resource_name": "customers",
                    "sync_mode": sync_mode,
                    "records_synced": 2,
                    "dataset_ids": [],
                    "dataset_names": [dataset_ref],
                }
            ],
            "summary": f"Dataset sync completed for '{dataset_ref}'.",
        }

    async def aclose(self) -> None:
        self.close_calls += 1

def test_remote_sdk_dataset_query_polls_preview_job() -> None:
    workspace_id = uuid.uuid4()
    dataset_id = uuid.uuid4()
    job_id = uuid.uuid4()

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/api/runtime/v1/"):
            return httpx.Response(404, json={"detail": "not found"})
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

    payload = RuntimeDatasetListResponse.model_validate(
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
                    "materialization_mode": "live",
                    "management_mode": "runtime_managed",
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
        if request.url.path.startswith("/api/runtime/v1/"):
            return httpx.Response(404, json={"detail": "not found"})
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
    assert result.items[0].materialization_mode == "live"


def test_remote_sdk_sql_query_polls_job_and_fetches_results() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    sql_job_id = uuid.uuid4()

    job_response = SqlJobResponse(
        id=sql_job_id,
        workspace_id=workspace_id,
        actor_id=actor_id,
        workbench_mode=SqlWorkbenchMode.direct_sql,
        connection_id=uuid.uuid4(),
        selected_datasets=[],
        execution_mode="single",
        status="succeeded",
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
        status="succeeded",
        columns=[{"name": "value", "type": "integer"}],
        rows=[{"value": 7}],
        row_count_preview=1,
        total_rows_estimate=1,
        artifacts=[],
    )

    def handler(request: httpx.Request) -> httpx.Response:
        if request.url.path.startswith("/api/runtime/v1/"):
            return httpx.Response(404, json={"detail": "not found"})
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
    workspace_id = uuid.uuid4()
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
        if request.url.path.startswith("/api/runtime/v1/"):
            return httpx.Response(404, json={"detail": "not found"})
        if request.method == "POST" and request.url.path == f"/api/v1/thread/{workspace_id}/":
            return httpx.Response(
                201,
                json=ThreadResponse(id=thread_id, workspace_id=workspace_id, title="sdk").model_dump(mode="json"),
            )
        if request.method == "POST" and request.url.path == f"/api/v1/thread/{workspace_id}/{thread_id}/chat":
            return httpx.Response(200, json={"job_id": str(job_id), "job_status": "queued"})
        if request.method == "GET" and request.url.path == f"/api/v1/jobs/{workspace_id}/{job_id}":
            return httpx.Response(200, json=state.model_dump(mode="json"))
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://sdk.test")
    client = LangbridgeClient.for_remote_api(
        base_url="https://sdk.test",
        http_client=http_client,
        default_workspace_id=workspace_id,
    )

    result = client.agents.ask(agent_id=agent_id, message="hello")

    assert result.status == "succeeded"
    assert result.thread_id == thread_id
    assert result.summary == "done"
    assert result.result == {"text": "hello"}


def test_local_sdk_dataset_and_sql_queries_use_runtime_adapter() -> None:
    actor_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(actor_id=actor_id)
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        default_workspace_id=runtime_host.context.workspace_id,
        default_actor_id=actor_id,
    )

    dataset_result = client.datasets.query(dataset_id=uuid.uuid4(), limit=25)
    sql_result = client.sql.query(query="select 7 as value", connection_id=uuid.uuid4())

    assert dataset_result.status == "succeeded"
    assert dataset_result.row_count_preview == 1
    assert sql_result.status == "succeeded"
    assert sql_result.query_scope == "source"
    assert sql_result.rows == [{"value": 7}]
    assert len(runtime_host.execute_sql_text_calls) == 0
    assert len(runtime_host.execute_sql_calls) == 1


def test_local_sdk_semantic_queries_use_dedicated_semantic_client() -> None:
    actor_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(actor_id=actor_id)
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        default_workspace_id=runtime_host.context.workspace_id,
        default_actor_id=actor_id,
    )

    result = client.semantic.query(
        "commerce_performance",
        measures=["shopify_orders.net_sales"],
        dimensions=["shopify_orders.country"],
        order={"shopify_orders.net_sales": "desc"},
        limit=5,
    )

    assert result.status == "succeeded"
    assert result.rows[0]["shopify_orders.country"] == "United Kingdom"
    assert result.generated_sql == "SELECT country, SUM(net_revenue) AS net_sales FROM orders_enriched"
    assert runtime_host.query_semantic_calls == [
        {
            "semantic_models": ["commerce_performance"],
            "measures": ["shopify_orders.net_sales"],
            "dimensions": ["shopify_orders.country"],
            "filters": [],
            "limit": 5,
            "order": {"shopify_orders.net_sales": "desc"},
            "time_dimensions": [],
        }
    ]


def test_dataset_client_rejects_semantic_style_arguments() -> None:
    actor_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(actor_id=actor_id)
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        default_workspace_id=runtime_host.context.workspace_id,
        default_actor_id=actor_id,
    )

    try:
        client.datasets.query("shopify_orders", metrics=["net_sales"])  # type: ignore[call-arg]
    except TypeError as exc:
        assert "metrics" in str(exc)
    else:
        raise AssertionError("Expected semantic-style dataset query to be rejected.")


def test_local_sdk_direct_sql_by_connection_name_uses_runtime_shortcut() -> None:
    actor_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(actor_id=actor_id)
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        default_workspace_id=runtime_host.context.workspace_id,
        default_actor_id=actor_id,
    )

    result = client.sql.query(
        query="SELECT value FROM source_table",
        connection_name="commerce_demo",
    )

    assert result.status == "succeeded"
    assert result.query_scope == "source"
    assert len(runtime_host.execute_sql_calls) == 0
    assert len(runtime_host.execute_sql_text_calls) == 1


def test_local_sdk_federated_sql_defaults_to_runtime_sql_service_without_selected_datasets() -> None:
    actor_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(actor_id=actor_id)
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        default_workspace_id=runtime_host.context.workspace_id,
        default_actor_id=actor_id,
    )

    result = client.sql.query(query="SELECT value FROM sales_orders")

    assert result.status == "succeeded"
    assert result.query_scope == "dataset"
    assert len(runtime_host.execute_sql_calls) == 1
    assert len(runtime_host.execute_sql_text_calls) == 0


def test_local_sdk_selected_datasets_is_a_uuid_subset_selector() -> None:
    actor_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(actor_id=actor_id)
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        default_workspace_id=runtime_host.context.workspace_id,
        default_actor_id=actor_id,
    )
    selected_dataset_id = uuid.uuid4()

    result = client.sql.query(
        query="SELECT value FROM some_dataset",
        selected_datasets=[selected_dataset_id],
    )

    assert result.status == "succeeded"
    assert result.query_scope == "dataset"
    assert len(runtime_host.execute_sql_calls) == 1
    request = runtime_host.execute_sql_calls[0]
    assert request.selected_datasets == [selected_dataset_id]


def test_local_sdk_agents_ask_uses_runtime_host() -> None:
    actor_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(actor_id=actor_id)
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        default_workspace_id=runtime_host.context.workspace_id,
        default_actor_id=actor_id,
    )

    result = client.agents.ask(agent_id=uuid.uuid4(), message="hello local")

    assert result.status == "succeeded"
    assert result.summary is not None
    assert result.thread_id is not None
    assert result.job_id is not None


def test_local_sdk_close_closes_runtime_host() -> None:
    actor_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(actor_id=actor_id)
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        default_workspace_id=runtime_host.context.workspace_id,
        default_actor_id=actor_id,
    )

    client.close()
    client.close()

    assert runtime_host.close_calls == 2


def test_remote_sdk_runtime_host_requests_use_runtime_payload_shapes() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()
    dataset_id = uuid.uuid4()
    thread_id = uuid.uuid4()
    job_id = uuid.uuid4()

    def _payload(request: httpx.Request) -> dict[str, object]:
        return json.loads(request.content.decode("utf-8")) if request.content else {}

    def _runtime_payload(request: httpx.Request) -> dict[str, object]:
        payload = _payload(request)
        return payload

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/runtime/v1/info":
            return httpx.Response(
                200,
                json={
                    "runtime_mode": "configured_local",
                    "workspace_id": str(workspace_id),
                    "actor_id": str(actor_id),
                    "roles": ["runtime:viewer"],
                    "capabilities": [
                        "datasets.preview",
                        "semantic.query",
                        "sql.query",
                        "agents.ask",
                    ],
                },
            )
        if request.method == "POST" and request.url.path == f"/api/runtime/v1/datasets/{dataset_id}/preview":
            payload = _runtime_payload(request)
            assert payload == {"limit": 5, "filters": {}, "sort": [], "user_context": {}}
            return httpx.Response(
                200,
                json={
                    "dataset_id": str(dataset_id),
                    "status": "succeeded",
                    "columns": [{"name": "order_id", "data_type": "integer"}],
                    "rows": [{"order_id": 1}],
                    "row_count_preview": 1,
                    "effective_limit": 5,
                    "redaction_applied": False,
                },
            )
        if request.method == "POST" and request.url.path == "/api/runtime/v1/semantic/query":
            payload = _runtime_payload(request)
            assert payload == {
                "semantic_models": ["commerce_performance"],
                "measures": ["shopify_orders.net_sales"],
                "dimensions": ["shopify_orders.country"],
                "filters": [],
                "time_dimensions": [],
                "limit": 5,
            }
            return httpx.Response(
                200,
                json={
                    "status": "succeeded",
                    "semantic_model_ids": [],
                    "data": [{"shopify_orders.country": "United Kingdom"}],
                    "annotations": [],
                },
            )
        if request.method == "POST" and request.url.path == "/api/runtime/v1/sql/query":
            payload = _runtime_payload(request)
            assert payload == {
                "query_scope": "source",
                "query": "select 7 as value",
                "connection_name": "commerce_demo",
                "query_dialect": "tsql",
                "params": {},
                "selected_datasets": [],
                "explain": False,
            }
            return httpx.Response(
                200,
                json={
                    "sql_job_id": str(uuid.uuid4()),
                    "status": "succeeded",
                    "columns": [{"name": "value", "type": "integer"}],
                    "rows": [{"value": 7}],
                    "row_count_preview": 1,
                    "redaction_applied": False,
                    "query": "select 7 as value",
                },
            )
        if request.method == "POST" and request.url.path == "/api/runtime/v1/agents/ask":
            payload = _runtime_payload(request)
            assert payload == {
                "message": "hello runtime",
                "agent_name": "commerce_analyst",
            }
            return httpx.Response(
                200,
                json={
                    "thread_id": str(thread_id),
                    "status": "succeeded",
                    "job_id": str(job_id),
                    "summary": "done",
                    "result": {"text": "hello"},
                    "events": [],
                },
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://sdk.test")
    client = LangbridgeClient.remote(
        base_url="https://sdk.test",
        http_client=http_client,
    )

    dataset_result = client.datasets.query(dataset_id=dataset_id, limit=5)
    semantic_result = client.semantic.query(
        "commerce_performance",
        measures=["shopify_orders.net_sales"],
        dimensions=["shopify_orders.country"],
        limit=5,
    )
    sql_result = client.sql.query(query="select 7 as value", connection_name="commerce_demo")
    agent_result = client.agents.ask(message="hello runtime", agent_name="commerce_analyst")

    assert dataset_result.status == "succeeded"
    assert semantic_result.status == "succeeded"
    assert sql_result.status == "succeeded"
    assert agent_result.status == "succeeded"
    assert agent_result.thread_id == thread_id
    assert agent_result.job_id == job_id


def test_remote_sdk_runtime_host_sql_defaults_to_federation_without_selected_datasets() -> None:
    workspace_id = uuid.uuid4()
    actor_id = uuid.uuid4()

    def _payload(request: httpx.Request) -> dict[str, object]:
        return json.loads(request.content.decode("utf-8")) if request.content else {}

    def handler(request: httpx.Request) -> httpx.Response:
        if request.method == "GET" and request.url.path == "/api/runtime/v1/info":
            return httpx.Response(
                200,
                json={
                    "runtime_mode": "configured_local",
                    "workspace_id": str(workspace_id),
                    "actor_id": str(actor_id),
                    "roles": ["runtime:viewer"],
                    "capabilities": ["sql.query"],
                },
            )
        if request.method == "POST" and request.url.path == "/api/runtime/v1/sql/query":
            payload = _payload(request)
            assert payload == {
                "query_scope": "dataset",
                "query": "SELECT * FROM sales_orders",
                "selected_datasets": [],
                "query_dialect": "tsql",
                "params": {},
                "explain": False,
            }
            return httpx.Response(
                200,
                json={
                    "sql_job_id": str(uuid.uuid4()),
                    "status": "succeeded",
                    "columns": [{"name": "value", "type": "integer"}],
                    "rows": [{"value": 7}],
                    "row_count_preview": 1,
                    "redaction_applied": False,
                    "query": "SELECT * FROM sales_orders",
                },
            )
        raise AssertionError(f"Unexpected request: {request.method} {request.url}")

    http_client = httpx.Client(transport=httpx.MockTransport(handler), base_url="https://sdk.test")
    client = LangbridgeClient.remote(
        base_url="https://sdk.test",
        http_client=http_client,
    )

    result = client.sql.query(query="SELECT * FROM sales_orders")

    assert result.status == "succeeded"
    assert result.rows == [{"value": 7}]


def test_local_sdk_sync_clients_use_runtime_host() -> None:
    actor_id = uuid.uuid4()
    runtime_host = _FakeRuntimeHost(actor_id=actor_id)
    client = LangbridgeClient.for_local_runtime(
        runtime_host=runtime_host,
        default_workspace_id=runtime_host.context.workspace_id,
        default_actor_id=actor_id,
    )

    connectors = client.connectors.list()
    resources = client.sync.resources(connector_name="billing_demo")
    states = client.sync.states(connector_name="billing_demo")
    run = client.sync.run(
        dataset="billing_demo_customers",
    )

    assert connectors.total == 1
    assert connectors.items[0].name == "billing_demo"
    assert connectors.items[0].connector_family == "api"
    assert connectors.items[0].capabilities["supports_synced_datasets"] is True
    assert resources.total == 1
    assert resources.items[0].name == "customers"
    assert states.total == 1
    assert states.items[0].dataset_names == ["stripe_demo_customers"]
    assert run.status == "succeeded"
    assert run.dataset_name == "billing_demo_customers"
    assert run.resources[0].dataset_names == ["billing_demo_customers"]
    assert runtime_host.sync_calls == [
        {
            "dataset_ref": "billing_demo_customers",
            "sync_mode": "INCREMENTAL",
            "force_full_refresh": False,
        }
    ]


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
    materialization_mode: live
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
llm_connections:
  - name: local_openai
    provider: openai
    model: gpt-4o-mini
    api_key: test-key
    default: true
ai:
  profiles:
    - name: commerce_analyst
      llm:
        llm_connection: local_openai
      scope:
        semantic_models: [commerce_performance]
      prompts:
        system: You are a commerce analytics agent.
      access:
        allowed_connectors: [commerce_demo]
        denied_connectors: []
      execution:
        max_iterations: 3
""".strip(),
        encoding="utf-8",
    )

    async def run_flow() -> None:
        client = LangbridgeClient.local(config_path=str(config_path))
        datasets = await client.datasets.list()
        preview = await client.datasets.query(dataset_id=datasets.items[0].id, limit=2)
        result = await client.semantic.query(
            "commerce_performance",
            measures=["shopify_orders.net_sales"],
            dimensions=["shopify_orders.country"],
            order={"shopify_orders.net_sales": "desc"},
            limit=5,
        )
        sql_result = await client.sql.query(
            query=(
                "SELECT country, SUM(net_revenue) AS net_sales "
                "FROM orders_enriched "
                "GROUP BY country "
                "ORDER BY net_sales DESC"
            ),
            connection_name="commerce_demo",
        )

        assert datasets.total == 1
        assert datasets.items[0].name == "shopify_orders"
        assert preview.status == "succeeded"
        assert len(preview.rows) == 2
        assert result.status == "succeeded"
        assert result.rows[0]["shopify_orders.country"] == "United Kingdom"
        assert sql_result.status == "succeeded"
        assert sql_result.rows[0]["country"] == "United Kingdom"

    asyncio.run(run_flow())
