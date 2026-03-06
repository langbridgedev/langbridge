
from langbridge.packages.messaging.langbridge_messaging.broker.redis import RedisStreams
from langbridge.packages.messaging.langbridge_messaging.contracts.base import MessageType


STREAM_MAPPING = {
    MessageType.AGENT_JOB_REQUEST: RedisStreams.WORKER,
    MessageType.SEMANTIC_QUERY_REQUEST: RedisStreams.WORKER,
    MessageType.AGENTIC_SEMANTIC_MODEL_JOB_REQUEST: RedisStreams.WORKER,
    MessageType.COPILOT_DASHBOARD_REQUEST: RedisStreams.WORKER,
    MessageType.SQL_JOB_REQUEST: RedisStreams.WORKER,
    MessageType.DATASET_JOB_REQUEST: RedisStreams.WORKER,
    MessageType.CONNECTOR_SYNC_JOB_REQUEST: RedisStreams.WORKER,
    MessageType.JOB_EVENT: RedisStreams.API
}
