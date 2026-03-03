import uuid

import pytest
from pydantic import ValidationError

from langbridge.packages.common.langbridge_common.contracts.jobs.agentic_semantic_model_job import (
    CreateAgenticSemanticModelJobRequest,
)
from langbridge.packages.messaging.langbridge_messaging.broker.redis import RedisStreams
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType
from langbridge.packages.messaging.langbridge_messaging.contracts.stream_mapping import STREAM_MAPPING


def _valid_payload() -> dict:
    return {
        "organisation_id": uuid.uuid4(),
        "project_id": None,
        "user_id": uuid.uuid4(),
        "semantic_model_id": uuid.uuid4(),
        "connector_id": uuid.uuid4(),
        "selected_tables": ["sales.orders", "sales.customers"],
        "selected_columns": {
            "sales.orders": ["order_id", "customer_id", "amount"],
            "sales.customers": ["customer_id", "region"],
        },
        "question_prompts": [
            "revenue by region",
            "top customers by sales",
            "monthly trend",
        ],
        "include_sample_values": False,
    }


def test_agentic_job_contract_accepts_valid_payload() -> None:
    payload = CreateAgenticSemanticModelJobRequest(**_valid_payload())

    assert payload.job_type.value == "agentic_semantic_model"
    assert len(payload.selected_tables) == 2
    assert len(payload.question_prompts) == 3


def test_agentic_job_contract_rejects_prompt_count_outside_range() -> None:
    invalid_payload = _valid_payload()
    invalid_payload["question_prompts"] = ["only one prompt"]

    with pytest.raises(ValidationError):
        CreateAgenticSemanticModelJobRequest(**invalid_payload)


def test_agentic_job_contract_rejects_unknown_selected_columns_table_key() -> None:
    invalid_payload = _valid_payload()
    invalid_payload["selected_columns"] = {
        "sales.orders": ["order_id"],
        "unknown.table": ["id"],
    }

    with pytest.raises(ValidationError):
        CreateAgenticSemanticModelJobRequest(**invalid_payload)


def test_agentic_message_type_routes_to_worker_stream() -> None:
    assert STREAM_MAPPING[MessageType.AGENTIC_SEMANTIC_MODEL_JOB_REQUEST] == RedisStreams.WORKER
