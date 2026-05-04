from __future__ import annotations

import time
from collections.abc import Mapping
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from fastapi.testclient import TestClient

from tests.integration.runtime_host.test_runtime_host_api import (
    _build_runtime,
    _build_runtime_with_relational_semantic_models,
    _create_runtime_app,
    _extract_sse_payloads,
    _wait_for_runtime_job,
)


@dataclass(slots=True, frozen=True)
class RuntimeSqlJobCase:
    name: str
    runtime_builder: str
    request: Mapping[str, Any]
    expected_rows: list[dict[str, Any]]
    expected_query_scope: str
    expected_generated_sql_fragment: str | None = None
    expected_federation_query_type: str | None = None


SQL_JOB_MATRIX: tuple[RuntimeSqlJobCase, ...] = (
    RuntimeSqlJobCase(
        name="source_scope_sql_job",
        runtime_builder="source",
        request={
            "query_scope": "source",
            "query": "SELECT COUNT(*) AS row_count FROM orders_enriched",
            "connection_name": "commerce_demo",
        },
        expected_rows=[{"row_count": 3}],
        expected_query_scope="source",
        expected_generated_sql_fragment="COUNT",
    ),
    RuntimeSqlJobCase(
        name="dataset_scope_federated_sql_job",
        runtime_builder="relational",
        request={
            "query_scope": "dataset",
            "query": (
                "SELECT c.region, SUM(o.net_revenue) AS net_sales "
                "FROM shopify_orders AS o "
                "JOIN shopify_customers AS c ON o.customer_id = c.customer_id "
                "GROUP BY c.region "
                "ORDER BY net_sales DESC"
            ),
        },
        expected_rows=[
            {"net_sales": 440.0, "region": "Europe"},
            {"net_sales": 210.0, "region": "North America"},
        ],
        expected_query_scope="dataset",
        expected_generated_sql_fragment="shopify_orders",
        expected_federation_query_type="sql",
    ),
    RuntimeSqlJobCase(
        name="semantic_scope_sql_job",
        runtime_builder="relational",
        request={
            "query_scope": "semantic",
            "query": (
                "SELECT region, net_sales "
                "FROM commerce_performance "
                "WHERE order_status = 'fulfilled' "
                "ORDER BY net_sales DESC"
            ),
        },
        expected_rows=[
            {"region": "North America", "net_sales": 210.0},
            {"region": "Europe", "net_sales": 180.0},
        ],
        expected_query_scope="semantic",
        expected_generated_sql_fragment="LEFT JOIN customer_profiles",
        expected_federation_query_type="semantic",
    ),
)


def test_runtime_sql_job_matrix(tmp_path: Path) -> None:
    for case in SQL_JOB_MATRIX:
        runtime = _runtime_for_case(tmp_path / case.name, case)
        with TestClient(_create_runtime_app(runtime)) as client:
            queued = client.post("/api/runtime/v1/sql/query/jobs", json=dict(case.request))
            assert queued.status_code == 202
            queued_payload = queued.json()
            assert queued_payload["status"] == "queued"
            assert queued_payload["job_type"] == "sql.query"
            assert queued_payload["query_scope"] == case.expected_query_scope
            assert queued_payload["stream_path"] == f"/api/runtime/v1/jobs/{queued_payload['job_id']}/stream"

            job = _wait_for_runtime_job(client, queued_payload["job_id"])
            stream_payloads = _job_stream_payloads(client, queued_payload["job_id"])

        assert job["status"] == "succeeded"
        assert job["job_type"] == "sql.query"
        assert job["result"]["rows"] == case.expected_rows
        assert job["result"]["query_scope"] == case.expected_query_scope
        assert {artifact["artifact_key"] for artifact in job["artifacts"]} == {
            "result_table",
            "sql_diagnostics",
        }
        assert job["tasks"][0]["status"] == "succeeded"
        diagnostics = job["tasks"][0]["diagnostics"]
        assert diagnostics["query_scope"] == case.expected_query_scope
        assert diagnostics["generated_sql"]
        if case.expected_generated_sql_fragment is not None:
            assert case.expected_generated_sql_fragment in diagnostics["generated_sql"]
        if case.expected_federation_query_type is not None:
            assert diagnostics["federation_diagnostics"]["summary"]["query_type"] == case.expected_federation_query_type
        assert [payload["event"] for payload in stream_payloads][:2] == [
            "job.started",
            "sql.query.started",
        ]
        assert stream_payloads[-1]["event"] == "job.succeeded"
        assert stream_payloads[-1]["terminal"] is True


def test_runtime_sql_job_failure_records_error_and_public_events(tmp_path: Path) -> None:
    runtime = _build_runtime(tmp_path)
    with TestClient(_create_runtime_app(runtime)) as client:
        queued = client.post(
            "/api/runtime/v1/sql/query/jobs",
            json={
                "query_scope": "source",
                "query": "SELECT * FROM missing_orders",
                "connection_name": "commerce_demo",
            },
        )
        assert queued.status_code == 202
        job_id = queued.json()["job_id"]
        failed = _wait_for_failed_job(client, job_id)
        stream_payloads = _job_stream_payloads(client, job_id)

    assert failed["status"] == "failed"
    assert failed["error"]["message"]
    assert "missing_orders" in failed["error"]["message"]
    assert failed["tasks"][0]["status"] == "failed"
    assert "sql.query.failed" in [
        event.get("event") or event.get("event_type")
        for event in failed["events"]
    ]
    assert stream_payloads[-1]["event"] == "job.failed"
    assert stream_payloads[-1]["terminal"] is True


def _runtime_for_case(tmp_path: Path, case: RuntimeSqlJobCase):
    tmp_path.mkdir(parents=True, exist_ok=True)
    if case.runtime_builder == "source":
        return _build_runtime(tmp_path)
    if case.runtime_builder == "relational":
        return _build_runtime_with_relational_semantic_models(tmp_path)
    raise AssertionError(f"Unknown runtime builder for SQL job matrix case '{case.name}'.")


def _wait_for_failed_job(client: TestClient, job_id: str) -> dict[str, Any]:
    job = None
    for _ in range(50):
        response = client.get(f"/api/runtime/v1/jobs/{job_id}")
        assert response.status_code == 200
        job = response.json()
        if job["status"] == "failed":
            return job
        if job["status"] in {"succeeded", "cancelled"}:
            raise AssertionError(f"Expected failed job {job_id}, got {job['status']}: {job}")
        time.sleep(0.1)
    raise AssertionError(f"Runtime job {job_id} did not fail: {job}")


def _job_stream_payloads(client: TestClient, job_id: str) -> list[dict[str, object]]:
    with client.stream("GET", f"/api/runtime/v1/jobs/{job_id}/stream") as response:
        body = "".join(response.iter_text())
    assert response.status_code == 200
    return _extract_sse_payloads(body)
