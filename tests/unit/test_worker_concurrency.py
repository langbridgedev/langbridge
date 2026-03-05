import asyncio

import pytest

from langbridge.apps.worker.langbridge_worker import main as worker_main
from langbridge.packages.messaging.langbridge_messaging.broker.base import (
    MessageReceipt,
    ReceivedMessage,
)
from langbridge.packages.messaging.langbridge_messaging.contracts.base import (
    BaseMessagePayload,
    MessageType,
    register_payload,
)
from langbridge.packages.messaging.langbridge_messaging.contracts.messages import (
    MessageEnvelope,
)


@register_payload(MessageType.AGENT_JOB_REQUEST.value)
class _TestMessagePayload(BaseMessagePayload):
    message: str


@pytest.fixture
def anyio_backend():
    return "asyncio"


class _DummySession:
    async def commit(self) -> None:
        return

    async def rollback(self) -> None:
        return

    async def close(self) -> None:
        return


class _FakeContainer:
    def __init__(self, broker) -> None:
        self._broker = broker

    def wire(self, *, packages):
        return

    def message_broker(self):
        return self._broker

    def async_session_factory(self):
        return _DummySession


class _FakeBroker:
    def __init__(self, messages):
        self._messages = messages
        self.consume_calls: list[tuple[int, int]] = []
        self.acked: list[str] = []
        self.nacked: list[tuple[str, str | None]] = []
        self.closed = False

    async def consume(self, *, timeout_ms: int, count: int):
        self.consume_calls.append((timeout_ms, count))
        return list(self._messages)

    async def ack(self, message: ReceivedMessage) -> None:
        self.acked.append(message.receipt.entry_id)

    async def nack(self, message: ReceivedMessage, *, error: str | None = None) -> None:
        self.nacked.append((message.receipt.entry_id, error))

    async def publish(self, message: MessageEnvelope, stream: str | None = None) -> str:
        return "1-0"

    async def close(self) -> None:
        self.closed = True


def _received_message(entry_id: str) -> ReceivedMessage:
    envelope = MessageEnvelope(
        message_type=MessageType.AGENT_JOB_REQUEST,
        payload=_TestMessagePayload(message=f"message-{entry_id}"),
    )
    receipt = MessageReceipt(
        stream="langbridge:worker_stream",
        group="worker",
        consumer="test-consumer",
        entry_id=entry_id,
    )
    return ReceivedMessage(envelope=envelope, receipt=receipt)


@pytest.mark.anyio
async def test_run_worker_processes_batch_concurrently(monkeypatch) -> None:
    active_messages = 0
    max_active_messages = 0
    active_lock = asyncio.Lock()
    two_messages_started = asyncio.Event()
    release_processing = asyncio.Event()

    class _FakeWorkerMessageHandler:
        def __init__(self, dependency_resolver) -> None:
            self._dependency_resolver = dependency_resolver

        async def handle_message(self, message):
            nonlocal active_messages, max_active_messages
            async with active_lock:
                active_messages += 1
                max_active_messages = max(max_active_messages, active_messages)
                if active_messages >= 2:
                    two_messages_started.set()
            await release_processing.wait()
            async with active_lock:
                active_messages -= 1
            return None

    messages = [_received_message("1-0"), _received_message("2-0")]
    fake_broker = _FakeBroker(messages=messages)
    fake_container = _FakeContainer(broker=fake_broker)

    monkeypatch.setenv("WORKER_RUN_ONCE", "true")
    monkeypatch.setenv("WORKER_BROKER", "redis")
    monkeypatch.setenv("WORKER_CONCURRENCY", "2")
    monkeypatch.setenv("WORKER_BATCH_SIZE", "2")
    monkeypatch.setattr(worker_main, "create_container", lambda: fake_container)
    monkeypatch.setattr(worker_main, "WorkerMessageDispatcher", _FakeWorkerMessageHandler)

    worker_task = asyncio.create_task(worker_main.run_worker(poll_interval=0))
    try:
        await asyncio.wait_for(two_messages_started.wait(), timeout=1.0)
    finally:
        release_processing.set()

    await asyncio.wait_for(worker_task, timeout=1.0)

    assert max_active_messages >= 2
    assert fake_broker.consume_calls[0][1] == 2
    assert sorted(fake_broker.acked) == ["1-0", "2-0"]
    assert fake_broker.nacked == []
    assert fake_broker.closed is True
