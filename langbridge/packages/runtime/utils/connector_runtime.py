from __future__ import annotations

import json
from typing import Any, Callable

from langbridge.packages.runtime.models import SecretReference

SecretResolver = Callable[[SecretReference], str]


def parse_connector_payload(raw_config: Any) -> dict[str, Any]:
    if isinstance(raw_config, dict):
        return dict(raw_config)
    if isinstance(raw_config, (str, bytes)):
        try:
            parsed = json.loads(raw_config)
        except Exception:
            return {}
        if isinstance(parsed, dict):
            return dict(parsed)
    return {}


def build_connector_runtime_payload(
    *,
    config_json: Any,
    connection_metadata: Any | None = None,
    secret_references: Any | None = None,
    secret_resolver: SecretResolver | None = None,
) -> dict[str, Any]:
    resolved_payload = parse_connector_payload(config_json)
    runtime_config = dict(resolved_payload.get("config") or {})

    if isinstance(connection_metadata, dict):
        metadata = dict(connection_metadata)
        extra = metadata.pop("extra", {})
        for key, value in metadata.items():
            if value is not None:
                runtime_config.setdefault(key, value)
        if isinstance(extra, dict):
            for key, value in extra.items():
                if value is not None:
                    runtime_config.setdefault(key, value)

    if isinstance(secret_references, dict):
        if secret_resolver is None and secret_references:
            raise ValueError("A secret resolver is required when secret references are configured.")
        for secret_name, raw_reference in secret_references.items():
            if secret_resolver is None:
                continue
            reference = (
                raw_reference
                if isinstance(raw_reference, SecretReference)
                else SecretReference.model_validate(raw_reference)
            )
            runtime_config[secret_name] = secret_resolver(reference)

    resolved_payload["config"] = runtime_config
    return resolved_payload
