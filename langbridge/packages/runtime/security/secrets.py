from __future__ import annotations

import json
import os
from pathlib import Path
from typing import Protocol

from langbridge.packages.common.langbridge_common.contracts.connectors import SecretReference


class SecretProvider(Protocol):
    def resolve(self, reference: SecretReference) -> str: ...


class EnvSecretProvider:
    def resolve(self, reference: SecretReference) -> str:
        raw = os.environ.get(reference.identifier)
        if raw is None:
            raise ValueError(f"Environment secret '{reference.identifier}' was not found.")
        if reference.key:
            try:
                payload = json.loads(raw)
            except json.JSONDecodeError as exc:
                raise ValueError(
                    f"Environment secret '{reference.identifier}' is not valid JSON."
                ) from exc
            if not isinstance(payload, dict) or reference.key not in payload:
                raise ValueError(
                    f"Environment secret '{reference.identifier}' does not contain key '{reference.key}'."
                )
            value = payload.get(reference.key)
            if value is None:
                raise ValueError(
                    f"Environment secret '{reference.identifier}' key '{reference.key}' is empty."
                )
            return str(value)
        return raw


class KubernetesSecretProvider:
    def __init__(self, secrets_mount_dir: str | None = None) -> None:
        self._base_dir = Path(
            secrets_mount_dir
            or os.environ.get("K8S_SECRETS_DIR", "/var/run/secrets/langbridge")
        )

    def resolve(self, reference: SecretReference) -> str:
        key = reference.key or "value"
        secret_path = self._base_dir / reference.identifier / key
        if not secret_path.exists():
            raise ValueError(f"Kubernetes secret file '{secret_path}' was not found.")
        return secret_path.read_text(encoding="utf-8").strip()


class SecretProviderRegistry:
    def __init__(self) -> None:
        self._providers: dict[str, SecretProvider] = {
            "env": EnvSecretProvider(),
            "kubernetes": KubernetesSecretProvider(),
        }

    def resolve(self, reference: SecretReference) -> str:
        provider = self._providers.get(reference.provider_type)
        if provider is None:
            raise ValueError(
                f"Secret provider '{reference.provider_type}' is not supported by this worker runtime."
            )
        return provider.resolve(reference)
