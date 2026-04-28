from __future__ import annotations

import asyncio
import copy
from types import SimpleNamespace

from langbridge.runtime.application.semantic import SemanticApplication
from langbridge.runtime.models import SqlQueryRequest
from langbridge.runtime.services.semantic_sql_query_service import SemanticSqlQueryService
from langbridge.semantic.model import SemanticModel


def _semantic_model() -> SemanticModel:
    return SemanticModel.model_validate(
        {
            "version": "1",
            "name": "commerce_performance",
            "datasets": {
                "orders": {
                    "dimensions": [
                        {"name": "country", "type": "string"},
                        {"name": "order_status", "type": "string"},
                        {"name": "order_date", "type": "time"},
                    ],
                    "measures": [
                        {
                            "name": "net_sales",
                            "expression": "net_revenue",
                            "type": "number",
                            "aggregation": "sum",
                        }
                    ],
                }
            },
            "metrics": {
                "net_sales_metric": {
                    "expression": "SUM(orders.net_revenue)",
                }
            },
        }
    )


class _StubRuntimeHost:
    def __init__(self, semantic_model: SemanticModel) -> None:
        self._service = SemanticSqlQueryService()
        self._semantic_model = semantic_model

    def parse_semantic_sql_query(self, **kwargs):
        return self._service.parse_query(**kwargs)

    def build_semantic_sql_query(self, **kwargs):
        return self._service.build_query_plan(**kwargs)


class _StubHost:
    def __init__(self, semantic_model: SemanticModel) -> None:
        self._runtime_host = _StubRuntimeHost(semantic_model)
        self._record = SimpleNamespace(
            name="commerce_performance",
            semantic_model=semantic_model,
        )

    def _resolve_semantic_model_record(self, model_ref: str):
        assert model_ref == "commerce_performance"
        return self._record


class _SemanticApplicationWithPayload(SemanticApplication):
    def __init__(self, host, payload: dict) -> None:
        super().__init__(host)
        self._payload = payload

    async def query_semantic_models(self, *args, **kwargs):
        return copy.deepcopy(self._payload)


def test_query_semantic_sql_maps_alias_keys_with_metadata_sources() -> None:
    app = _SemanticApplicationWithPayload(
        _StubHost(_semantic_model()),
        payload={
            "semantic_model_id": "model-1",
            "semantic_model_ids": ["model-1"],
            "rows": [
                {
                    "ORDERS__COUNTRY": "United States",
                    "NET_SALES_METRIC": 210.0,
                }
            ],
            "metadata": [
                {"column": "orders__country", "source": "orders.country", "name": "country"},
                {
                    "column": "net_sales_metric",
                    "source": "net_sales_metric",
                    "name": "net_sales_metric",
                },
            ],
            "generated_sql": "SELECT ...",
        },
    )

    response = asyncio.run(
        app.query_semantic_sql(
            request=SqlQueryRequest(
                query_scope="semantic",
                query=(
                    "SELECT country AS market, net_sales_metric "
                    "FROM commerce_performance "
                    "WHERE order_status = 'fulfilled' "
                    "GROUP BY 1"
                ),
                query_dialect="postgres",
            )
        )
    )

    assert response["rows"] == [{"market": "United States", "net_sales_metric": 210.0}]
    assert [column["name"] for column in response["columns"]] == ["market", "net_sales_metric"]


def test_query_semantic_sql_maps_time_bucket_rows_from_engine_aliases() -> None:
    app = _SemanticApplicationWithPayload(
        _StubHost(_semantic_model()),
        payload={
            "semantic_model_id": "model-1",
            "semantic_model_ids": ["model-1"],
            "rows": [
                {
                    "ORDERS__ORDER_DATE_MONTH": "2025-01-01",
                    "ORDERS__NET_SALES": 210.0,
                }
            ],
            "metadata": [
                {
                    "column": "orders__order_date_month",
                    "source": "orders.order_date",
                    "name": "order_date (month)",
                },
                {"column": "orders__net_sales", "source": "orders.net_sales", "name": "net_sales"},
            ],
            "generated_sql": "SELECT ...",
        },
    )

    response = asyncio.run(
        app.query_semantic_sql(
            request=SqlQueryRequest(
                query_scope="semantic",
                query=(
                    "SELECT DATE_TRUNC('month', order_date) AS month, net_sales "
                    "FROM commerce_performance "
                    "GROUP BY 1"
                ),
                query_dialect="postgres",
            )
        )
    )

    assert response["rows"] == [{"month": "2025-01-01", "net_sales": 210.0}]
    assert [column["name"] for column in response["columns"]] == ["month", "net_sales"]
