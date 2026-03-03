from __future__ import annotations

import json
import os
import uuid
from datetime import datetime, timedelta, timezone
from typing import Sequence

import httpx

from langbridge.packages.common.langbridge_common.contracts.runtime import (
    EdgeTaskAckRequest,
    EdgeTaskFailRequest,
    EdgeTaskPullRequest,
    EdgeTaskResultRequest,
    RuntimeCapabilitiesUpdateRequest,
    RuntimeHeartbeatRequest,
    RuntimeRegistrationRequest,
    RuntimeRegistrationResponse,
)
from langbridge.packages.messaging.langbridge_messaging.broker.base import (
    MessageBroker,
    MessageReceipt,
    ReceivedMessage,
)
from langbridge.packages.messaging.langbridge_messaging.contracts.messages import MessageEnvelope


class CustomerRuntimeBroker(MessageBroker):
    def __init__(
        self,
        *,
        api_base_url: str | None = None,
        access_token: str | None = None,
        registration_token: str | None = None,
    ) -> None:
        self._api_base_url = (api_base_url or os.environ.get("EDGE_API_BASE_URL") or "").rstrip("/")
        if not self._api_base_url:
            backend = os.environ.get("BACKEND_URL", "http://localhost:8000").rstrip("/")
            self._api_base_url = f"{backend}/api/v1"
        self._access_token = access_token or os.environ.get("EDGE_RUNTIME_ACCESS_TOKEN")
        self._registration_token = registration_token or os.environ.get("EDGE_REGISTRATION_TOKEN")
        self._token_expires_at: datetime | None = None
        self._runtime_id: str | None = os.environ.get("EDGE_RUNTIME_ID")
        self._lease_by_entry_id: dict[str, str] = {}
        self._client = httpx.AsyncClient(timeout=httpx.Timeout(65.0, connect=10.0))

    async def publish(self, message: MessageEnvelope, stream: str | None = None) -> str:
        await self._ensure_access_token()
        request = EdgeTaskResultRequest(
            request_id=str(uuid.uuid4()),
            envelopes=[message],
        )
        await self._request(
            "POST",
            "/edge/tasks/result",
            json=request.model_dump(mode="json"),
        )
        return "ok"

    async def consume(self, *, timeout_ms: int = 1000, count: int = 1) -> Sequence[ReceivedMessage]:
        await self._ensure_access_token()
        pull_request = EdgeTaskPullRequest(
            max_tasks=max(1, min(count, 10)),
            long_poll_seconds=max(1, min(60, int(timeout_ms / 1000) or 1)),
            visibility_timeout_seconds=max(30, int(os.environ.get("EDGE_VISIBILITY_TIMEOUT_SECONDS", "90"))),
        )
        response = await self._request(
            "POST",
            "/edge/tasks/pull",
            json=pull_request.model_dump(mode="json"),
        )
        payload = response.json()
        tasks = payload.get("tasks", [])
        messages: list[ReceivedMessage] = []
        for task in tasks:
            task_id = str(task.get("taskId") or task.get("task_id"))
            lease_id = str(task.get("leaseId") or task.get("lease_id"))
            envelope_payload = task.get("envelope")
            if not task_id or not lease_id or not envelope_payload:
                continue
            envelope = MessageEnvelope.model_validate(envelope_payload)
            self._lease_by_entry_id[task_id] = lease_id
            messages.append(
                ReceivedMessage(
                    envelope=envelope,
                    receipt=MessageReceipt(
                        stream="edge",
                        group="edge",
                        consumer=self._runtime_id or "customer-runtime",
                        entry_id=task_id,
                    ),
                )
            )
        return messages

    async def ack(self, message: ReceivedMessage) -> None:
        await self._ensure_access_token()
        task_id = message.receipt.entry_id
        lease_id = self._lease_by_entry_id.get(task_id)
        if lease_id is None:
            return
        request = EdgeTaskAckRequest(task_id=uuid.UUID(task_id), lease_id=lease_id)
        await self._request(
            "POST",
            "/edge/tasks/ack",
            json=request.model_dump(mode="json"),
        )
        self._lease_by_entry_id.pop(task_id, None)

    async def nack(self, message: ReceivedMessage, *, error: str | None = None) -> None:
        await self._ensure_access_token()
        task_id = message.receipt.entry_id
        lease_id = self._lease_by_entry_id.get(task_id)
        if lease_id is None:
            return
        request = EdgeTaskFailRequest(
            task_id=uuid.UUID(task_id),
            lease_id=lease_id,
            error=error or "worker_error",
            retry_delay_seconds=max(1, int(os.environ.get("EDGE_RETRY_DELAY_SECONDS", "5"))),
        )
        await self._request(
            "POST",
            "/edge/tasks/fail",
            json=request.model_dump(mode="json"),
        )
        self._lease_by_entry_id.pop(task_id, None)

    async def close(self) -> None:
        await self._client.aclose()

    async def _ensure_access_token(self) -> None:
        if self._access_token and self._token_expires_at is not None:
            self._token_expires_at = self._coerce_utc(self._token_expires_at)
            if self._token_expires_at > datetime.now(timezone.utc) + timedelta(seconds=60):
                return
            await self._heartbeat()
            return

        if self._access_token and self._token_expires_at is None:
            await self._heartbeat()
            return

        if not self._registration_token:
            raise RuntimeError(
                "Customer runtime broker requires EDGE_REGISTRATION_TOKEN or EDGE_RUNTIME_ACCESS_TOKEN."
            )
        await self._register()
        await self._update_capabilities()

    async def _register(self) -> None:
        raw_caps = os.environ.get("WORKER_RUNTIME_CAPABILITIES", "").strip()
        capabilities = self._parse_capabilities(raw_caps)
        request = RuntimeRegistrationRequest(
            registration_token=self._registration_token or "",
            display_name=os.environ.get("WORKER_RUNTIME_NAME"),
            tags=self._parse_tags(os.environ.get("WORKER_RUNTIME_TAGS", "")),
            capabilities=capabilities,
            metadata={
                "worker_version": os.environ.get("WORKER_VERSION", "v1"),
            },
        )
        response = await self._request(
            "POST",
            "/runtimes/register",
            json=request.model_dump(mode="json"),
            include_auth=False,
        )
        payload = RuntimeRegistrationResponse.model_validate(response.json())
        self._access_token = payload.access_token
        self._token_expires_at = self._coerce_utc(payload.expires_at)
        self._runtime_id = str(payload.ep_id)

    async def _heartbeat(self) -> None:
        if not self._access_token:
            return
        request = RuntimeHeartbeatRequest(metadata={"runtime_id": self._runtime_id})
        response = await self._request(
            "POST",
            "/runtimes/heartbeat",
            json=request.model_dump(mode="json"),
        )
        payload = response.json()
        token = payload.get("accessToken") or payload.get("access_token")
        expires_at_raw = payload.get("expiresAt") or payload.get("expires_at")
        if token:
            self._access_token = token
        if expires_at_raw:
            parsed = datetime.fromisoformat(str(expires_at_raw).replace("Z", "+00:00"))
            self._token_expires_at = self._coerce_utc(parsed)

    async def _update_capabilities(self) -> None:
        if not self._access_token:
            return
        raw_caps = os.environ.get("WORKER_RUNTIME_CAPABILITIES", "").strip()
        request = RuntimeCapabilitiesUpdateRequest(
            tags=self._parse_tags(os.environ.get("WORKER_RUNTIME_TAGS", "")),
            capabilities=self._parse_capabilities(raw_caps),
        )
        await self._request(
            "POST",
            "/runtimes/capabilities",
            json=request.model_dump(mode="json"),
        )

    def _auth_headers(self) -> dict[str, str]:
        if not self._access_token:
            return {}
        return {"Authorization": f"Bearer {self._access_token}"}

    async def _request(
        self,
        method: str,
        path: str,
        *,
        json: dict | None = None,
        include_auth: bool = True,
    ) -> httpx.Response:
        headers: dict[str, str] = {}
        if include_auth:
            headers.update(self._auth_headers())
        response = await self._client.request(
            method=method,
            url=f"{self._api_base_url}{path}",
            json=json,
            headers=headers,
        )
        response.raise_for_status()
        return response

    @staticmethod
    def _parse_tags(raw_tags: str) -> list[str]:
        return [tag.strip() for tag in raw_tags.split(",") if tag.strip()]

    @staticmethod
    def _parse_capabilities(raw_caps: str) -> dict:
        if not raw_caps:
            return {
                "message_types": [
                    "agent_job_request",
                    "semantic_query_request",
                    "agentic_semantic_model_job_request",
                    "copilot_dashboard_request",
                ]
            }
        try:
            parsed = json.loads(raw_caps)
        except json.JSONDecodeError as exc:
            raise RuntimeError("WORKER_RUNTIME_CAPABILITIES must be valid JSON.") from exc
        if not isinstance(parsed, dict):
            raise RuntimeError("WORKER_RUNTIME_CAPABILITIES must deserialize to an object.")
        return parsed

    @staticmethod
    def _coerce_utc(value: datetime) -> datetime:
        if value.tzinfo is None or value.tzinfo.utcoffset(value) is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)
