from enum import Enum
from typing import Callable, TypeVar
import uuid
from pydantic import BaseModel, ConfigDict


class MessageType(str, Enum):
    """Message types."""
    TEST = "test"
    AGENT_JOB_REQUEST = "agent_job_request"
    JOB_EVENT = "job_event"
    AGENT_JOB_TASK_EXECUTION = "agent_job_task_execution"
    AGENT_JOB_PLAN_EXECUTION = "agent_job_plan_execution"
    AGENT_JOB_SYNTHESIZE_RESPONSE = "agent_job_synthesize_response"
    AGENT_JOB_COMPLETION = "agent_job_completion"
    
    SEMANTIC_QUERY_REQUEST = "semantic_query_request"
    SEMANTIC_QUERY_RESPONSE = "semantic_query_response"
    SEMANTIC_TASK_EXECUTION = "semantic_task_execution"
    SEMANTIC_QUERY_COMPLETION = "semantic_query_completion"
    AGENTIC_SEMANTIC_MODEL_JOB_REQUEST = "agentic_semantic_model_job_request"
    COPILOT_DASHBOARD_REQUEST = "copilot_dashboard_request"
    COPILOT_DASHBOARD_RESPONSE = "copilot_dashboard_response"
    COPILOT_DASHBOARD_COMPLETION = "copilot_dashboard_completion"
    SQL_JOB_REQUEST = "sql_job_request"
    

    def __str__(self) -> str:
        return self.value

class BaseMessagePayload(BaseModel):    
    model_config = ConfigDict(json_encoders={uuid.UUID: str})

    
    """Base class for message payloads."""
    __message_type__: str | None = None
    
    @property
    def message_type(self) -> MessageType:
        message_type = getattr(self, "__message_type__", None)
        if not message_type:
            raise NotImplementedError("Subclasses must define a __message_type__ attribute.")
        return MessageType(message_type)


PayloadT = TypeVar("PayloadT", bound=BaseMessagePayload)

_PAYLOAD_REGISTRY: dict[str, type[BaseMessagePayload]] = {}


def register_payload(message_type: str) -> Callable[[type[PayloadT]], type[PayloadT]]:
    def _decorator(cls: type[PayloadT]) -> type[PayloadT]:
        _PAYLOAD_REGISTRY[message_type] = cls
        cls.__message_type__ = message_type
        return cls

    return _decorator


def get_payload_model(message_type: str) -> type[BaseMessagePayload] | None:
    return _PAYLOAD_REGISTRY.get(message_type)

@register_payload("test")
class TestMessagePayload(BaseMessagePayload):
    message: str
