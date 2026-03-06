from __future__ import annotations

from typing import Any, Mapping

from langbridge.packages.common.langbridge_common.errors.connector_errors import (
    AuthError,
    ConnectorError,
)
from langbridge.packages.connectors.langbridge_connectors.api._http_api_connector import (
    ApiResourceDefinition,
    HttpApiConnector,
    flatten_api_records,
)
from langbridge.packages.connectors.langbridge_connectors.api.config import (
    ConnectorRuntimeType,
)
from langbridge.packages.connectors.langbridge_connectors.api.connector import (
    ApiExtractResult,
    ApiResource,
)

from .config import SALESFORCE_SUPPORTED_RESOURCES, SalesforceConnectorConfig

_RESOURCE_OBJECTS = {
    "accounts": "Account",
    "contacts": "Contact",
    "leads": "Lead",
    "opportunities": "Opportunity",
}

_RESOURCE_FIELDS = {
    "accounts": [
        "Id",
        "Name",
        "Type",
        "Industry",
        "BillingCity",
        "BillingCountry",
        "CreatedDate",
        "LastModifiedDate",
        "SystemModstamp",
    ],
    "contacts": [
        "Id",
        "FirstName",
        "LastName",
        "Email",
        "Phone",
        "AccountId",
        "LeadSource",
        "CreatedDate",
        "LastModifiedDate",
        "SystemModstamp",
    ],
    "leads": [
        "Id",
        "FirstName",
        "LastName",
        "Company",
        "Status",
        "Email",
        "Phone",
        "LeadSource",
        "CreatedDate",
        "LastModifiedDate",
        "SystemModstamp",
    ],
    "opportunities": [
        "Id",
        "Name",
        "StageName",
        "Amount",
        "CloseDate",
        "AccountId",
        "Type",
        "LeadSource",
        "CreatedDate",
        "LastModifiedDate",
        "SystemModstamp",
    ],
}


class SalesforceApiConnector(HttpApiConnector):
    RUNTIME_TYPE = ConnectorRuntimeType.SALESFORCE
    SUPPORTED_RESOURCES = SALESFORCE_SUPPORTED_RESOURCES
    RESOURCE_DEFINITIONS = {
        "accounts": ApiResourceDefinition(
            resource=ApiResource(
                name="accounts",
                label="Accounts",
                primary_key="Id",
                cursor_field="nextRecordsUrl",
                incremental_cursor_field="SystemModstamp",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="",
        ),
        "contacts": ApiResourceDefinition(
            resource=ApiResource(
                name="contacts",
                label="Contacts",
                primary_key="Id",
                cursor_field="nextRecordsUrl",
                incremental_cursor_field="SystemModstamp",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="",
        ),
        "leads": ApiResourceDefinition(
            resource=ApiResource(
                name="leads",
                label="Leads",
                primary_key="Id",
                cursor_field="nextRecordsUrl",
                incremental_cursor_field="SystemModstamp",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="",
        ),
        "opportunities": ApiResourceDefinition(
            resource=ApiResource(
                name="opportunities",
                label="Opportunities",
                primary_key="Id",
                cursor_field="nextRecordsUrl",
                incremental_cursor_field="SystemModstamp",
                supports_incremental=True,
                default_sync_mode="INCREMENTAL",
            ),
            path="",
        ),
    }

    def __init__(self, config: SalesforceConnectorConfig, logger=None, **kwargs: Any) -> None:
        super().__init__(config=config, logger=logger, **kwargs)
        self._access_token: str | None = None
        self._instance_url: str | None = None

    def _base_url(self) -> str:
        return (self._instance_url or self.config.instance_url).rstrip("/")

    async def test_connection(self) -> None:
        await self._authenticated_json(
            "GET",
            f"/services/data/{self.config.api_version}/",
        )

    async def extract_resource(
        self,
        resource_name: str,
        *,
        since: str | None = None,
        cursor: str | None = None,
        limit: int | None = None,
    ) -> ApiExtractResult:
        definition = self._require_resource(resource_name)
        page_size = self._clamp_limit(limit, default=200, maximum=2000)

        if cursor:
            payload = await self._authenticated_json("GET", cursor)
        else:
            payload = await self._authenticated_json(
                "GET",
                f"/services/data/{self.config.api_version}/query",
                params={"q": self._soql_query(resource_name, page_size)},
            )

        if not isinstance(payload, dict):
            raise ConnectorError("Salesforce response was not a valid JSON object.")
        records = payload.get("records", [])
        if not isinstance(records, list):
            records = []
        normalized_records = [
            _remove_attributes(record)
            for record in records
            if isinstance(record, Mapping)
        ]
        flattened_records, child_records = flatten_api_records(
            resource_name=definition.resource.name,
            records=normalized_records,
            primary_key=definition.resource.primary_key,
        )
        raw_cursor = payload.get("nextRecordsUrl")
        next_cursor = str(raw_cursor).strip() or None if raw_cursor else None
        return ApiExtractResult(
            resource=definition.resource.name,
            status="success",
            records=flattened_records,
            next_cursor=next_cursor,
            checkpoint_cursor=_max_salesforce_cursor(flattened_records, "SystemModstamp"),
            child_records=child_records,
        )

    async def _authenticated_json(
        self,
        method: str,
        path_or_url: str,
        *,
        params: Mapping[str, Any] | None = None,
        data: Mapping[str, Any] | None = None,
    ) -> Any:
        try:
            payload, _ = await self._request_json(
                method,
                path_or_url,
                headers=await self._authorization_headers(),
                params=params,
                data=data,
            )
            return payload
        except AuthError:
            self._access_token = None
            payload, _ = await self._request_json(
                method,
                path_or_url,
                headers=await self._authorization_headers(),
                params=params,
                data=data,
            )
            return payload

    async def _authorization_headers(self) -> dict[str, str]:
        return {"Authorization": f"Bearer {await self._access_token_value()}"}

    async def _access_token_value(self) -> str:
        if self._access_token:
            return self._access_token

        payload, _ = await self._request_json(
            "POST",
            f"{self.config.instance_url.rstrip('/')}/services/oauth2/token",
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            data={
                "grant_type": "refresh_token",
                "client_id": self.config.client_id,
                "client_secret": self.config.client_secret,
                "refresh_token": self.config.refresh_token,
            },
        )
        if not isinstance(payload, dict):
            raise AuthError("Salesforce token endpoint returned an invalid response.")

        access_token = str(payload.get("access_token") or "").strip()
        instance_url = str(payload.get("instance_url") or self.config.instance_url).strip()
        if not access_token:
            raise AuthError("Salesforce token endpoint did not return an access token.")
        if not instance_url:
            raise AuthError("Salesforce token endpoint did not return an instance URL.")

        self._access_token = access_token
        self._instance_url = instance_url
        return access_token

    def _soql_query(self, resource_name: str, limit: int) -> str:
        object_name = _RESOURCE_OBJECTS.get(resource_name)
        fields = _RESOURCE_FIELDS.get(resource_name)
        if object_name is None or fields is None:
            raise ConnectorError(f"Unsupported Salesforce resource '{resource_name}'.")
        field_sql = ", ".join(fields)
        return f"SELECT {field_sql} FROM {object_name} ORDER BY SystemModstamp ASC LIMIT {limit}"


def _remove_attributes(value: Any) -> Any:
    if isinstance(value, list):
        return [_remove_attributes(item) for item in value]
    if isinstance(value, Mapping):
        return {
            str(key): _remove_attributes(item)
            for key, item in value.items()
            if str(key) != "attributes"
        }
    return value


def _max_salesforce_cursor(rows: list[dict[str, Any]], field_name: str) -> str | None:
    values = [str(row.get(field_name) or "").strip() for row in rows if str(row.get(field_name) or "").strip()]
    if not values:
        return None
    return max(values)
