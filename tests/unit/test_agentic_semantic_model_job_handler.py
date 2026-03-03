from types import SimpleNamespace

import pytest

from langbridge.apps.worker.langbridge_worker.handlers.jobs.agentic_semantic_model_job_request_handler import (
    AgenticSemanticModelJobRequestHandler,
)
from langbridge.packages.common.langbridge_common.errors.application_errors import (
    BusinessValidationError,
)
from langbridge.packages.semantic.langbridge_semantic.loader import load_semantic_model


def _build_handler() -> AgenticSemanticModelJobRequestHandler:
    return AgenticSemanticModelJobRequestHandler(
        job_repository=SimpleNamespace(),
        semantic_model_repository=SimpleNamespace(),
        connector_repository=SimpleNamespace(),
        llm_repository=SimpleNamespace(),
        message_broker=SimpleNamespace(),
    )


def _table_blueprints():
    return [
        {
            "entity_name": "sales_orders",
            "schema": "sales",
            "table_name": "orders",
            "table_reference": "sales.orders",
            "columns": [
                SimpleNamespace(name="order_id", data_type="integer", is_primary_key=True),
                SimpleNamespace(name="customer_id", data_type="integer", is_primary_key=False),
                SimpleNamespace(name="amount", data_type="decimal", is_primary_key=False),
            ],
            "foreign_keys": [
                SimpleNamespace(
                    schema="sales",
                    table="customers",
                    column="customer_id",
                    foreign_key="customer_id",
                )
            ],
        },
        {
            "entity_name": "sales_customers",
            "schema": "sales",
            "table_name": "customers",
            "table_reference": "sales.customers",
            "columns": [
                SimpleNamespace(name="customer_id", data_type="integer", is_primary_key=True),
                SimpleNamespace(name="region", data_type="string", is_primary_key=False),
            ],
            "foreign_keys": [],
        },
    ]


def test_agentic_handler_builds_valid_yaml_from_selection_blueprints() -> None:
    handler = _build_handler()
    blueprints = _table_blueprints()

    payload, warnings = handler._build_payload_from_table_blueprints(
        connector_name="sales-db",
        table_blueprints=blueprints,
        question_prompts=["revenue by region", "top customers", "monthly trend"],
    )
    yaml_text = handler._render_and_validate_yaml(payload, blueprints)
    parsed = load_semantic_model(yaml_text)

    assert "sales_orders" in parsed.tables
    assert "sales_customers" in parsed.tables
    assert parsed.relationships is not None
    assert len(parsed.relationships) == 1
    assert parsed.relationships[0].from_ == "sales_orders"
    assert parsed.relationships[0].to == "sales_customers"
    assert isinstance(warnings, list)


def test_agentic_handler_rejects_mismatched_column_mapping() -> None:
    handler = _build_handler()
    blueprints = _table_blueprints()
    payload, _ = handler._build_payload_from_table_blueprints(
        connector_name="sales-db",
        table_blueprints=blueprints,
        question_prompts=["revenue by region", "top customers", "monthly trend"],
    )

    # Drop one selected column from generated payload to force validation failure.
    dimensions = payload["tables"]["sales_orders"]["dimensions"]
    payload["tables"]["sales_orders"]["dimensions"] = [
        dimension for dimension in dimensions if dimension["name"] != "customer_id"
    ]

    with pytest.raises(BusinessValidationError):
        handler._render_and_validate_yaml(payload, blueprints)
