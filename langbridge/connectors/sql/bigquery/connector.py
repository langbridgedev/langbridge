import json
import logging
from pathlib import Path
from typing import Any, Dict, Optional

from langbridge.connectors.base.config import ConnectorRuntimeType
from langbridge.connectors.base.connector import SqlConnector
from langbridge.connectors.base.metadata import ColumnMetadata, ForeignKeyMetadata, TableMetadata
from langbridge.connectors.base.errors import ConnectorError

from .config import BigQueryConnectorConfig

try:  # pragma: no cover - optional dependency
    from google.cloud import bigquery  # type: ignore
    from google.oauth2 import service_account  # type: ignore
except ImportError:  # pragma: no cover - optional dependency
    bigquery = None  # type: ignore
    service_account = None  # type: ignore


class BigQueryConnector(SqlConnector):
    """
    Google BigQuery connector implementation.
    """

    RUNTIME_TYPE = ConnectorRuntimeType.BIGQUERY
    SQLGLOT_DIALECT = "bigquery"

    def __init__(
        self,
        config: BigQueryConnectorConfig,
        logger: Optional[logging.Logger] = None,
    ) -> None:
        super().__init__(config=config, logger=logger)
        self._config = config

    def _build_credentials(self):
        if service_account is None:
            raise ConnectorError(
                "google-cloud-bigquery is required for BigQuery support."
            )
        raw = self._config.credentials_json
        payload: Dict[str, Any]
        try:
            payload = json.loads(raw)
        except json.JSONDecodeError:
            path = Path(raw).expanduser()
            if not path.exists():
                raise ConnectorError("Invalid BigQuery credentials JSON payload.")
            payload = json.loads(path.read_text())
        return service_account.Credentials.from_service_account_info(payload)

    def _client(self):
        if bigquery is None:
            raise ConnectorError(
                "google-cloud-bigquery is required for BigQuery support."
            )
        credentials = self._build_credentials()
        return bigquery.Client(
            project=self._config.project_id,
            credentials=credentials,
            location=self._config.location,
        )

    async def test_connection(self) -> None:
        try:
            client = self._client()
            job = client.query("SELECT 1")
            list(job.result())
        except Exception as exc:
            self.logger.error("Connection test failed: %s", exc)
            raise ConnectorError(f"Unable to connect to BigQuery: {exc}") from exc

    async def fetch_schemas(self) -> list[str]:
        try:
            client = self._client()
            datasets = list(client.list_datasets(self._config.project_id))
            if datasets:
                return [dataset.dataset_id for dataset in datasets]
            if self._config.dataset:
                return [self._config.dataset]
            return []
        except Exception as exc:
            self.logger.error("Failed to fetch schemas: %s", exc)
            raise ConnectorError(f"Unable to fetch schemas from BigQuery: {exc}") from exc

    async def fetch_tables(self, schema: str) -> list[str]:
        try:
            client = self._client()
            dataset_ref = bigquery.DatasetReference(self._config.project_id, schema)  # type: ignore[union-attr]
            tables = list(client.list_tables(dataset_ref))
            return [table.table_id for table in tables]
        except Exception as exc:
            self.logger.error("Failed to fetch tables: %s", exc)
            raise ConnectorError(f"Unable to fetch tables from BigQuery: {exc}") from exc

    async def fetch_columns(self, schema: str, table: str) -> list[ColumnMetadata]:
        try:
            client = self._client()
            table_ref = f"{self._config.project_id}.{schema}.{table}"
            table_obj = client.get_table(table_ref)
            columns = []
            for field in table_obj.schema:
                columns.append(
                    ColumnMetadata(
                        name=field.name,
                        data_type=str(field.field_type),
                        is_nullable=field.mode != "REQUIRED",
                    )
                )
            return columns
        except Exception as exc:
            self.logger.error("Failed to fetch columns: %s", exc)
            raise ConnectorError(f"Unable to fetch columns from BigQuery: {exc}") from exc

    async def fetch_table_metadata(self, schema: str, table: str) -> TableMetadata:
        columns = await self.fetch_columns(schema, table)
        return TableMetadata(schema=schema, name=table, columns=columns)

    async def fetch_foreign_keys(self, schema: str, table: str) -> list[ForeignKeyMetadata]:
        return []

    async def _execute_select(
        self,
        sql: str,
        params: Dict[str, Any],
        *,
        timeout_s: Optional[int] = 30,
    ) -> tuple[list[str], list[tuple]]:
        if params:
            self.logger.warning("BigQuery connector ignores query parameters.")
        try:
            client = self._client()
            job = client.query(sql)
            result = job.result(timeout=timeout_s)
            columns = [field.name for field in result.schema]
            rows = [tuple(row) for row in result]
            return columns, rows
        except Exception as exc:
            self.logger.error("SQL execution failed: %s", exc)
            raise ConnectorError(f"SQL execution failed on BigQuery: {exc}") from exc
