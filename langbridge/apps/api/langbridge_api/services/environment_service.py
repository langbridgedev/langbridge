from dataclasses import asdict, dataclass
from datetime import datetime, timezone
import hashlib
import json
import logging
import uuid
from typing import Any

from langbridge.packages.common.langbridge_common.config import settings
from langbridge.packages.common.langbridge_common.db.environment import (
    OrganisationEnvironmentSetting,
)
from langbridge.packages.common.langbridge_common.repositories.environment_repository import (
    OrganizationEnvironmentSettingRepository,
)
from langbridge.packages.common.langbridge_common.utils.encryptor import (
    CipherRecord,
    ConfigCrypto,
    Keyring,
)


@dataclass(frozen=True)
class EnvironmentSettingCatalogItem:
    setting_key: str
    display_name: str
    description: str
    category: str
    scope: str = "organization"
    data_type: str = "string"
    options: list[str] | None = None
    placeholder: str | None = None
    multiline: bool = False
    default_value: str | None = None
    is_locked: bool = False
    is_inherited: bool = False
    helper_text: str | None = None
    is_advanced: bool = False


SETTINGS_CATALOG: tuple[EnvironmentSettingCatalogItem, ...] = (
    EnvironmentSettingCatalogItem(
        setting_key="support_email",
        display_name="Support email",
        description="Default support contact for this organization.",
        category="General",
        data_type="string",
        placeholder="support@company.com",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="feature_flag_new_dashboard",
        display_name="New dashboard experience",
        description="Enable the new dashboard UI for organization members.",
        category="General",
        data_type="boolean",
        options=["true", "false"],
        default_value="false",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="feature_flag_sql_ui",
        display_name="SQL workbench",
        description="Enable SQL workbench features for the organization.",
        category="General",
        data_type="boolean",
        options=["true", "false"],
        default_value="true",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="redaction_policy_pii",
        display_name="PII redaction policy",
        description="Default redaction mode applied to query results.",
        category="Security & Access",
        data_type="string",
        options=["off", "mask", "hash"],
        default_value="mask",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="execution_mode_default",
        display_name="Default execution mode",
        description="Default query execution target for the organization.",
        category="Execution & Runtime",
        data_type="string",
        options=["hosted", "customer_runtime"],
        default_value="hosted",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="runtime_default_instance",
        display_name="Default runtime instance",
        description="Preferred runtime instance identifier for execution routing.",
        category="Execution & Runtime",
        data_type="string",
        placeholder="runtime-instance-id",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="worker_concurrency",
        display_name="Worker concurrency",
        description="Maximum concurrent worker jobs for the organization.",
        category="Execution & Runtime",
        data_type="number",
        default_value="4",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="llm_enabled",
        display_name="LLM enabled",
        description="Allow LLM-assisted workflows for this organization.",
        category="AI / LLM",
        data_type="boolean",
        options=["true", "false"],
        default_value="false",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="llm_provider",
        display_name="Default LLM provider",
        description="Preferred provider for LLM-powered tasks.",
        category="AI / LLM",
        data_type="string",
        options=["openai", "azure", "openrouter", "anthropic"],
        default_value="openai",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="llm_default_model",
        display_name="Default LLM model",
        description="Model identifier used when no explicit model is selected.",
        category="AI / LLM",
        data_type="string",
        placeholder="gpt-4o-mini",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="feature_flag_agentic_model_builder",
        display_name="Agentic model builder",
        description="Enable agentic semantic model builder capabilities.",
        category="AI / LLM",
        data_type="boolean",
        options=["true", "false"],
        default_value="false",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="staging_db_connection",
        display_name="Staging DB connection",
        description="Connection string for staging data workflows.",
        category="Connectors",
        data_type="string",
        placeholder="postgres://user:password@host:5432/dbname",
        multiline=True,
        helper_text="Store secrets here instead of plaintext configuration files.",
        is_advanced=True,
    ),
    EnvironmentSettingCatalogItem(
        setting_key="default_semantic_vector_connector",
        display_name="Default semantic vector connector",
        description="Connector identifier used for semantic vector storage.",
        category="Connectors",
        data_type="string",
        placeholder="connector-id",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="connector_policy_allowed_sources",
        display_name="Allowed connector sources",
        description="Comma-separated list of allowed source ids for connectors.",
        category="Connectors",
        data_type="list",
        multiline=True,
        placeholder="source_a, source_b, source_c",
        is_advanced=True,
    ),
    EnvironmentSettingCatalogItem(
        setting_key="datasets.auto_generate_on_connection_add",
        display_name="Auto-generate datasets on connection add",
        description="Show guided dataset generation after creating a new connection.",
        category="Datasets",
        data_type="boolean",
        options=["true", "false"],
        default_value="false",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="datasets.auto_generate_mode",
        display_name="Default dataset generation mode",
        description="Preferred mode for post-connection dataset generation.",
        category="Datasets",
        data_type="string",
        options=["guided", "all", "skip"],
        default_value="guided",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="datasets.default_max_preview_rows",
        display_name="Default dataset preview rows",
        description="Default max preview rows for generated datasets.",
        category="Datasets",
        data_type="number",
        default_value="1000",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="datasets.default_max_export_rows",
        display_name="Default dataset export rows",
        description="Default max export rows for generated datasets.",
        category="Datasets",
        data_type="number",
        default_value="100000",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="query_max_rows",
        display_name="Query max rows",
        description="Maximum rows returned by a query preview.",
        category="Limits & Quotas",
        data_type="number",
        default_value="5000",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="query_max_bytes",
        display_name="Query max bytes",
        description="Maximum bytes scanned by a query before termination.",
        category="Limits & Quotas",
        data_type="number",
        default_value="104857600",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="query_timeout_seconds",
        display_name="Query timeout seconds",
        description="Maximum runtime for a single query execution.",
        category="Limits & Quotas",
        data_type="number",
        default_value="30",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="billing_budget_usd",
        display_name="Monthly budget (USD)",
        description="Soft monthly budget alert threshold.",
        category="Limits & Quotas",
        data_type="number",
        default_value="0",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="notifications_incident_email",
        display_name="Incident notification email",
        description="Email destination for operational incidents.",
        category="Notifications",
        data_type="string",
        placeholder="oncall@company.com",
    ),
    EnvironmentSettingCatalogItem(
        setting_key="audit_retention_days",
        display_name="Audit retention days",
        description="Retention period for audit and compliance artifacts.",
        category="Audit & Compliance",
        data_type="number",
        default_value="90",
    ),
)

class EnvironmentSettingKey:
    pass  # Placeholder for potential future use as a structured key registry

class EnvironmentService:
    _META_PREFIX = "__meta__:"

    def __init__(
        self,
        repository: OrganizationEnvironmentSettingRepository,
        crypto: ConfigCrypto | None = None,
    ) -> None:
        self._repository = repository
        self._logger = logging.getLogger(__name__)
        self._crypto = crypto or self._build_crypto()
        self._catalog = list(SETTINGS_CATALOG)
        self._catalog_by_key = {item.setting_key: item for item in self._catalog}

    def _build_crypto(self) -> ConfigCrypto:
        try:
            return ConfigCrypto(Keyring.from_env())
        except Exception as exc:  # noqa: BLE001 - fallback in local/dev
            self._logger.warning(
                "Falling back to derived local keyring for environment settings: %s",
                exc,
            )
            derived_key = hashlib.sha256(settings.SESSION_SECRET.encode("utf-8")).digest()
            return ConfigCrypto(Keyring({"local": derived_key}, "local"))

    def _aad(self, organization_id: uuid.UUID) -> bytes:
        return f"org:{organization_id}".encode("utf-8")

    def _encrypt_value(self, organization_id: uuid.UUID, value: Any) -> str:
        record = self._crypto.encrypt(value, aad=self._aad(organization_id))
        return record.to_json()

    def _decrypt_value(self, organization_id: uuid.UUID, ciphertext: str) -> str:
        record = CipherRecord.from_json(ciphertext)
        plaintext = self._crypto.decrypt(record, aad_override=self._aad(organization_id))
        return plaintext.decode("utf-8")

    def _meta_key(self, key: str) -> str:
        return f"{self._META_PREFIX}{key}"

    def _is_meta_key(self, key: str) -> bool:
        return key.startswith(self._META_PREFIX)

    def get_catalog(self) -> list[dict[str, Any]]:
        return [asdict(item) for item in self._catalog]

    def get_catalog_entry(self, setting_key: str) -> dict[str, Any] | None:
        item = self._catalog_by_key.get(setting_key)
        return asdict(item) if item else None

    async def _upsert_setting_record(
        self,
        organization_id: uuid.UUID,
        key: str,
        encrypted_value: str,
    ) -> None:
        existing = await self._repository.get_by_key(organization_id, key)
        if existing:
            existing.setting_value = encrypted_value
            return
        setting = OrganisationEnvironmentSetting(
            id=uuid.uuid4(),
            organization_id=organization_id,
            setting_key=key,
            setting_value=encrypted_value,
        )
        self._repository.add(setting)

    async def set_setting(
        self,
        organization_id: uuid.UUID,
        key: str,
        value: Any,
        *,
        updated_by: str | None = None,
    ) -> None:
        """
        Create or update an encrypted setting for the organization.
        Also stores audit-lite metadata under an internal sidecar key.
        """

        encrypted_value = self._encrypt_value(organization_id, value)
        await self._upsert_setting_record(organization_id, key, encrypted_value)

        meta_payload = {
            "last_updated_at": datetime.now(timezone.utc).isoformat(),
            "last_updated_by": updated_by,
        }
        encrypted_meta = self._encrypt_value(organization_id, meta_payload)
        await self._upsert_setting_record(
            organization_id,
            self._meta_key(key),
            encrypted_meta,
        )
        await self._repository.flush()

    async def get_setting(
        self,
        organization_id: uuid.UUID,
        key: str,
        default: Any | None = None,
    ) -> Any | None:
        """Retrieve and decrypt a single setting. Returns default when missing."""

        existing = await self._repository.get_by_key(organization_id, key)
        if not existing:
            return default
        return self._decrypt_value(organization_id, existing.setting_value)

    def _decode_metadata(self, value: str) -> dict[str, Any]:
        try:
            payload = json.loads(value)
            if isinstance(payload, dict):
                return payload
        except Exception:
            self._logger.debug("Unable to parse setting metadata payload.", exc_info=True)
        return {}

    async def list_settings_with_metadata(
        self,
        organization_id: uuid.UUID,
    ) -> dict[str, dict[str, Any]]:
        """
        Return explicit (non-meta) settings with audit-lite metadata.
        """
        rows = await self._repository.list_for_organization(organization_id)

        values: dict[str, str] = {}
        metadata: dict[str, dict[str, Any]] = {}
        for setting in rows:
            decrypted = self._decrypt_value(organization_id, setting.setting_value)
            if self._is_meta_key(setting.setting_key):
                source_key = setting.setting_key[len(self._META_PREFIX) :]
                metadata[source_key] = self._decode_metadata(decrypted)
                continue
            values[setting.setting_key] = decrypted

        hydrated: dict[str, dict[str, Any]] = {}
        for key, setting_value in values.items():
            meta = metadata.get(key, {})
            hydrated[key] = {
                "setting_value": setting_value,
                "last_updated_by": meta.get("last_updated_by"),
                "last_updated_at": meta.get("last_updated_at"),
            }
        return hydrated

    async def list_settings(self, organization_id: uuid.UUID) -> dict[str, Any]:
        """
        Return explicit settings for an organization as a plain dict (decrypted).
        """
        hydrated = await self.list_settings_with_metadata(organization_id)
        return {
            key: payload["setting_value"]
            for key, payload in hydrated.items()
        }

    async def delete_setting(self, organization_id: uuid.UUID, key: str) -> None:
        """Delete a setting and its sidecar metadata key, when present."""

        existing = await self._repository.get_by_key(organization_id, key)
        existing_meta = await self._repository.get_by_key(organization_id, self._meta_key(key))
        if existing is None and existing_meta is None:
            return
        if existing is not None:
            await self._repository.delete(existing)
        if existing_meta is not None:
            await self._repository.delete(existing_meta)
        await self._repository.flush()

    def get_available_keys(self) -> list[str]:
        """Return all known keys from the settings catalog."""
        return [item.setting_key for item in self._catalog]
