from __future__ import annotations

import uuid
from dataclasses import dataclass
from datetime import datetime, timezone

import pytest

from langbridge.apps.api.langbridge_api.services.edge_task_gateway_service import (
    EdgeTaskGatewayService,
)
from langbridge.packages.common.langbridge_common.db import agent as _agent  # noqa: F401
from langbridge.packages.common.langbridge_common.db import semantic as _semantic  # noqa: F401
from langbridge.packages.common.langbridge_common.contracts.runtime import (
    EdgeTaskAckRequest,
    EdgeTaskPullRequest,
)
from langbridge.packages.common.langbridge_common.db.runtime import EdgeTaskRecord
from langbridge.packages.messaging.langbridge_messaging.contracts.base import (
    BaseMessagePayload,
    MessageType,
    register_payload
)
from langbridge.packages.messaging.langbridge_messaging.contracts.messages import MessageEnvelope

@register_payload(MessageType.AGENT_JOB_REQUEST.value)
class TestMessagePayload(BaseMessagePayload):
    message: str


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _FakeRedis:
    def __init__(self) -> None:
        self.hashes: dict[str, dict[str, str]] = {}
        self.sorted_sets: dict[str, dict[str, float]] = {}
        self.stream_entries: dict[str, list[dict[str, str]]] = {}

    async def hset(self, key: str, mapping: dict[str, str]) -> int:
        bucket = self.hashes.setdefault(key, {})
        for field, value in mapping.items():
            bucket[field] = str(value)
        return len(mapping)

    async def hgetall(self, key: str) -> dict[str, str]:
        return dict(self.hashes.get(key, {}))

    async def zadd(self, key: str, mapping: dict[str, float]) -> int:
        bucket = self.sorted_sets.setdefault(key, {})
        for member, score in mapping.items():
            bucket[str(member)] = float(score)
        return len(mapping)

    async def zrangebyscore(self, key: str, min, max, start: int = 0, num: int | None = None):
        bucket = self.sorted_sets.get(key, {})
        min_val = float("-inf") if min == "-inf" else float(min)
        max_val = float("inf") if max == "+inf" else float(max)
        members = [member for member, score in sorted(bucket.items(), key=lambda item: item[1]) if min_val <= score <= max_val]
        if num is None:
            return members[start:]
        return members[start : start + num]

    async def zrem(self, key: str, *members: str) -> int:
        bucket = self.sorted_sets.setdefault(key, {})
        removed = 0
        for member in members:
            if member in bucket:
                del bucket[member]
                removed += 1
        return removed

    async def expire(self, key: str, ttl_seconds: int) -> bool:
        return True

    async def xadd(self, stream: str, fields: dict[str, str]) -> str:
        entries = self.stream_entries.setdefault(stream, [])
        entries.append(fields)
        return f"{len(entries)}-0"


@dataclass
class _FakeEdgeTaskRepo:
    by_id: dict[uuid.UUID, EdgeTaskRecord]

    def __init__(self) -> None:
        self.by_id = {}

    def add(self, record: EdgeTaskRecord) -> EdgeTaskRecord:
        if record.id is None:
            record.id = uuid.uuid4()
        self.by_id[record.id] = record
        return record

    async def flush(self) -> None:
        return

    async def get_by_id(self, id_):
        return self.by_id.get(id_)


@dataclass
class _FakeEdgeResultReceiptRepo:
    ids: set[tuple[uuid.UUID, uuid.UUID, str]]

    def __init__(self) -> None:
        self.ids = set()

    async def get_by_request_id(self, *, tenant_id, runtime_id, request_id):
        if (tenant_id, runtime_id, request_id) in self.ids:
            return object()
        return None

    async def create_receipt(self, *, tenant_id, runtime_id, request_id, task_id, payload_hash=None):
        self.ids.add((tenant_id, runtime_id, request_id))
        return object()


@pytest.mark.anyio
async def test_edge_pull_and_ack_flow() -> None:
    task_repo = _FakeEdgeTaskRepo()
    receipt_repo = _FakeEdgeResultReceiptRepo()
    service = EdgeTaskGatewayService(
        edge_task_repository=task_repo,
        edge_result_receipt_repository=receipt_repo,
    )
    fake_redis = _FakeRedis()
    service._redis = fake_redis  # type: ignore[attr-defined]

    tenant_id = uuid.uuid4()
    runtime_id = uuid.uuid4()
    envelope = MessageEnvelope(
        message_type=MessageType.AGENT_JOB_REQUEST,
        payload=TestMessagePayload(message="hello"),
    )
    task_id = await service.enqueue_for_runtime(
        tenant_id=tenant_id,
        runtime_id=runtime_id,
        envelope=envelope,
    )

    pulled = await service.pull_tasks(
        tenant_id=tenant_id,
        runtime_id=runtime_id,
        request=EdgeTaskPullRequest(max_tasks=1, long_poll_seconds=1, visibility_timeout_seconds=60),
    )
    assert len(pulled) == 1
    assert pulled[0].task_id == task_id
    assert pulled[0].envelope.message_type == MessageType.AGENT_JOB_REQUEST

    ack = await service.ack_task(
        tenant_id=tenant_id,
        runtime_id=runtime_id,
        request=EdgeTaskAckRequest(task_id=task_id, lease_id=pulled[0].lease_id),
    )
    assert ack.accepted is True
    assert ack.status == "acked"
