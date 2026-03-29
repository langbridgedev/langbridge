
from pathlib import Path

import pytest

from langbridge.runtime.models import SecretReference
from langbridge.runtime.security import SecretProviderRegistry


def test_env_secret_provider_resolves_plain_value(monkeypatch) -> None:
    monkeypatch.setenv("DB_PASSWORD", "super-secret")
    registry = SecretProviderRegistry()

    value = registry.resolve(
        SecretReference(provider_type="env", identifier="DB_PASSWORD")
    )
    assert value == "super-secret"


def test_kubernetes_secret_provider_resolves_file(tmp_path: Path, monkeypatch) -> None:
    secret_file = tmp_path / "warehouse-creds" / "password"
    secret_file.parent.mkdir(parents=True, exist_ok=True)
    secret_file.write_text("k8s-secret\n", encoding="utf-8")
    monkeypatch.setenv("K8S_SECRETS_DIR", str(tmp_path))
    registry = SecretProviderRegistry()

    value = registry.resolve(
        SecretReference(
            provider_type="kubernetes",
            identifier="warehouse-creds",
            key="password",
        )
    )
    assert value == "k8s-secret"


def test_secret_provider_registry_rejects_unknown_provider() -> None:
    registry = SecretProviderRegistry()
    with pytest.raises(ValueError):
        registry.resolve(
            SecretReference(provider_type="vault", identifier="secret/path")
        )
