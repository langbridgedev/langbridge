from __future__ import annotations

import re
import uuid
from dataclasses import dataclass
from typing import Any, Iterable, Literal, Sequence

from langbridge.orchestrator.definitions.model import DataAccessPolicy
from langbridge.orchestrator.tools.sql_analyst.interfaces import (
    AnalystExecutionOutcome,
    AnalystOutcomeStage,
    AnalystOutcomeStatus,
    AnalystQueryRequest,
    AnalystQueryResponse,
    AnalystRecoveryAction,
)

_TOKEN_RE = re.compile(r"[A-Za-z0-9_]+")


def _normalize_text(value: Any) -> str:
    return re.sub(r"\s+", " ", str(value or "").strip().lower()).strip()


def _expand_match_variants(value: str) -> set[str]:
    normalized = _normalize_text(value)
    if not normalized:
        return set()
    variants = {
        normalized,
        normalized.replace("_", " "),
        normalized.replace("-", " "),
        normalized.replace(".", " "),
    }
    if " " in normalized:
        variants.add(normalized.replace(" ", "_"))
        variants.add(normalized.replace(" ", "-"))
    return {item.strip() for item in variants if item.strip()}


def _sorted_connector_ids(values: Iterable[uuid.UUID]) -> tuple[uuid.UUID, ...]:
    return tuple(sorted(set(values), key=lambda item: str(item)))


@dataclass(slots=True, frozen=True)
class ConnectorAuthorizationDecision:
    allowed: bool
    policy_rule: str
    policy_rationale: str
    connector_ids: tuple[uuid.UUID, ...] = ()
    denied_connector_ids: tuple[uuid.UUID, ...] = ()
    unknown_connector_ownership: bool = False


@dataclass(slots=True, frozen=True)
class AnalyticalDeniedAsset:
    asset_type: Literal["dataset", "semantic_model"]
    asset_id: str
    asset_name: str
    dataset_names: tuple[str, ...] = ()
    sql_aliases: tuple[str, ...] = ()
    connector_ids: tuple[uuid.UUID, ...] = ()
    denied_connector_ids: tuple[uuid.UUID, ...] = ()
    policy_rule: str = ""
    policy_rationale: str = ""
    unknown_connector_ownership: bool = False

    def matches_request(
        self,
        *,
        question: str,
        filters: dict[str, Any] | None,
    ) -> bool:
        combined_text = " ".join(
            part
            for part in (
                _normalize_text(question),
                _normalize_text(" ".join((filters or {}).keys())),
            )
            if part
        )
        if not combined_text:
            return False

        tokens = set(_TOKEN_RE.findall(combined_text))
        match_terms = {
            variant
            for raw_value in (self.asset_name, *self.dataset_names, *self.sql_aliases)
            for variant in _expand_match_variants(raw_value)
        }
        for term in sorted(match_terms, key=len, reverse=True):
            if " " in term and term in combined_text:
                return True
            if term in tokens and len(term) >= 3:
                return True
        return False

    def to_metadata(self) -> dict[str, Any]:
        return {
            "requested_asset_name": self.asset_name,
            "requested_asset_id": self.asset_id,
            "requested_asset_type": self.asset_type,
            "requested_dataset_names": list(self.dataset_names),
            "requested_sql_aliases": list(self.sql_aliases),
            "connector_ids": [str(item) for item in self.connector_ids],
            "denied_connector_ids": [str(item) for item in self.denied_connector_ids],
            "policy_rule": self.policy_rule,
            "policy_rationale": self.policy_rationale,
            "unknown_connector_ownership": self.unknown_connector_ownership,
        }


@dataclass(slots=True, frozen=True)
class AnalyticalAccessScope:
    policy_enforced: bool = False
    authorized_asset_count: int = 0
    denied_assets: tuple[AnalyticalDeniedAsset, ...] = ()

    @property
    def denied_asset_count(self) -> int:
        return len(self.denied_assets)

    @property
    def has_denied_assets(self) -> bool:
        return bool(self.denied_assets)

    @property
    def all_configured_assets_denied(self) -> bool:
        return self.has_denied_assets and self.authorized_asset_count <= 0

    def match_denied_request(
        self,
        *,
        question: str,
        filters: dict[str, Any] | None,
    ) -> AnalyticalDeniedAsset | None:
        for asset in self.denied_assets:
            if asset.matches_request(question=question, filters=filters):
                return asset
        return None

    def aggregate_denied_connector_ids(self) -> list[str]:
        connector_ids = {
            connector_id
            for asset in self.denied_assets
            for connector_id in (asset.denied_connector_ids or asset.connector_ids)
        }
        return [str(item) for item in sorted(connector_ids, key=lambda item: str(item))]

    def to_metadata(self) -> dict[str, Any]:
        return {
            "policy_enforced": self.policy_enforced,
            "authorized_assets_count": self.authorized_asset_count,
            "denied_assets_count": self.denied_asset_count,
            "denied_connector_ids": self.aggregate_denied_connector_ids(),
        }


class ConnectorAccessPolicyEvaluator:
    def __init__(self, policy: DataAccessPolicy | None) -> None:
        policy = policy or DataAccessPolicy()
        self._allowed = set(policy.allowed_connectors or [])
        self._denied = set(policy.denied_connectors or [])

    @property
    def has_restrictions(self) -> bool:
        return bool(self._allowed or self._denied)

    def evaluate_asset_connectors(
        self,
        *,
        connector_ids: Sequence[uuid.UUID | None],
        unknown_connector_ownership: bool = False,
    ) -> ConnectorAuthorizationDecision:
        resolved_connector_ids = _sorted_connector_ids(
            connector_id
            for connector_id in connector_ids
            if connector_id is not None
        )
        has_unknown_connector = unknown_connector_ownership or any(
            connector_id is None for connector_id in connector_ids
        )

        if self.has_restrictions and (has_unknown_connector or not resolved_connector_ids):
            return ConnectorAuthorizationDecision(
                allowed=False,
                policy_rule="unknown_connector_ownership",
                policy_rationale=(
                    "Connector ownership could not be resolved for this analytical asset, "
                    "so it is blocked under the current access policy."
                ),
                connector_ids=resolved_connector_ids,
                unknown_connector_ownership=True,
            )

        explicitly_denied = _sorted_connector_ids(
            connector_id
            for connector_id in resolved_connector_ids
            if connector_id in self._denied
        )
        if explicitly_denied:
            return ConnectorAuthorizationDecision(
                allowed=False,
                policy_rule="denied_connectors",
                policy_rationale=(
                    "One or more backing connectors are explicitly denied by the agent access policy."
                ),
                connector_ids=resolved_connector_ids,
                denied_connector_ids=explicitly_denied,
            )

        outside_allowed = _sorted_connector_ids(
            connector_id
            for connector_id in resolved_connector_ids
            if self._allowed and connector_id not in self._allowed
        )
        if outside_allowed:
            return ConnectorAuthorizationDecision(
                allowed=False,
                policy_rule="outside_allowed_connectors",
                policy_rationale=(
                    "The asset uses backing connectors outside the agent's allowed connector scope."
                ),
                connector_ids=resolved_connector_ids,
                denied_connector_ids=outside_allowed,
            )

        return ConnectorAuthorizationDecision(
            allowed=True,
            policy_rule="allowed",
            policy_rationale="The asset is within the configured connector access scope.",
            connector_ids=resolved_connector_ids,
        )

    def build_denied_asset(
        self,
        *,
        asset_type: Literal["dataset", "semantic_model"],
        asset_id: str,
        asset_name: str,
        dataset_names: Sequence[str],
        sql_aliases: Sequence[str],
        decision: ConnectorAuthorizationDecision,
    ) -> AnalyticalDeniedAsset:
        return AnalyticalDeniedAsset(
            asset_type=asset_type,
            asset_id=asset_id,
            asset_name=asset_name,
            dataset_names=tuple(item for item in dataset_names if str(item or "").strip()),
            sql_aliases=tuple(item for item in sql_aliases if str(item or "").strip()),
            connector_ids=decision.connector_ids,
            denied_connector_ids=decision.denied_connector_ids,
            policy_rule=decision.policy_rule,
            policy_rationale=decision.policy_rationale,
            unknown_connector_ownership=decision.unknown_connector_ownership,
        )


def _build_access_denied_message(
    *,
    denied_asset: AnalyticalDeniedAsset | None,
    access_scope: AnalyticalAccessScope,
) -> str:
    if denied_asset is None:
        return (
            "Access denied: none of this agent's configured analytical assets are available "
            "under the current connector access policy."
        )
    if denied_asset.policy_rule == "unknown_connector_ownership":
        return (
            f"Access denied: {denied_asset.asset_name} is blocked because its backing connector "
            "ownership could not be resolved under the current access policy."
        )
    if denied_asset.policy_rule == "denied_connectors":
        return (
            f"Access denied: {denied_asset.asset_name} uses a connector that is explicitly denied "
            "for this agent."
        )
    if denied_asset.policy_rule == "outside_allowed_connectors":
        return (
            f"Access denied: {denied_asset.asset_name} is outside this agent's allowed connector scope."
        )
    if access_scope.all_configured_assets_denied:
        return (
            "Access denied: the current agent cannot reach any of its configured analytical assets "
            "under the active connector policy."
        )
    return "Access denied: the requested analytical asset is outside this agent's access policy."


def _build_recovery_hint(access_scope: AnalyticalAccessScope) -> str:
    if access_scope.authorized_asset_count > 0:
        return "Retry with one of the agent's in-scope analytical assets."
    if access_scope.policy_enforced:
        return (
            "Ask an administrator to expand the agent's connector policy or update the configured analytical assets."
        )
    return "Adjust the request or update the agent access policy before retrying."


def build_access_denied_response(
    *,
    request: AnalystQueryRequest,
    access_scope: AnalyticalAccessScope,
    denied_asset: AnalyticalDeniedAsset | None = None,
    recovery_actions: Sequence[AnalystRecoveryAction] = (),
) -> AnalystQueryResponse:
    message = _build_access_denied_message(
        denied_asset=denied_asset,
        access_scope=access_scope,
    )
    metadata = {
        "question": request.question,
        "limit": request.limit,
        "filters": request.filters or {},
        "recovery_hint": _build_recovery_hint(access_scope),
        "policy_rationale": (
            denied_asset.policy_rationale
            if denied_asset is not None
            else "All configured analytical assets fall outside the current connector access policy."
        ),
        **access_scope.to_metadata(),
    }
    if denied_asset is not None:
        metadata.update(denied_asset.to_metadata())

    outcome = AnalystExecutionOutcome(
        status=AnalystOutcomeStatus.access_denied,
        stage=AnalystOutcomeStage.authorization,
        message=message,
        original_error="access_denied",
        recoverable=False,
        terminal=True,
        retry_attempted=False,
        rewrite_attempted=False,
        retry_count=0,
        selected_asset_id=denied_asset.asset_id if denied_asset else None,
        selected_asset_name=denied_asset.asset_name if denied_asset else None,
        selected_asset_type=denied_asset.asset_type if denied_asset else None,
        recovery_actions=list(recovery_actions),
        metadata=metadata,
    )
    asset_type = denied_asset.asset_type if denied_asset is not None else "dataset"
    asset_id = denied_asset.asset_id if denied_asset is not None else ""
    asset_name = denied_asset.asset_name if denied_asset is not None else ""
    return AnalystQueryResponse(
        analysis_path=asset_type,
        execution_mode="federated",
        asset_type=asset_type,
        asset_id=asset_id,
        asset_name=asset_name,
        sql_canonical="",
        sql_executable="",
        dialect="n/a",
        selected_datasets=[],
        result=None,
        error=message,
        execution_time_ms=None,
        outcome=outcome,
    )


__all__ = [
    "AnalyticalAccessScope",
    "AnalyticalDeniedAsset",
    "ConnectorAccessPolicyEvaluator",
    "ConnectorAuthorizationDecision",
    "build_access_denied_response",
]
