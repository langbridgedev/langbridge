import uuid

import pytest
from pydantic import ValidationError

from langbridge.contracts.jobs.agentic_semantic_model_job import (
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
        "dataset_ids": [uuid.uuid4(), uuid.uuid4()],
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
    assert len(payload.dataset_ids) == 2
    assert len(payload.question_prompts) == 3


def test_agentic_job_contract_rejects_prompt_count_outside_range() -> None:
    invalid_payload = _valid_payload()
    invalid_payload["question_prompts"] = ["only one prompt"]

    with pytest.raises(ValidationError):
        CreateAgenticSemanticModelJobRequest(**invalid_payload)


def test_agentic_job_contract_rejects_empty_dataset_ids() -> None:
    invalid_payload = _valid_payload()
    invalid_payload["dataset_ids"] = []

    with pytest.raises(ValidationError):
        CreateAgenticSemanticModelJobRequest(**invalid_payload)


def test_agentic_message_type_routes_to_worker_stream() -> None:
    assert STREAM_MAPPING[MessageType.AGENTIC_SEMANTIC_MODEL_JOB_REQUEST] == RedisStreams.WORKER
