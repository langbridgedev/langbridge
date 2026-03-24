from typing import Literal

from pydantic import computed_field
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", "langbridge/.env"),
        env_ignore_empty=True,
        extra="ignore",
    )

    ENVIRONMENT: Literal["local", "staging", "development", "production"] = "local"

    POSTGRES_SERVER: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = ""
    LOCAL_DB: str = "local.db"
    SQLALCHEMY_POOL_SIZE: int = 10
    SQLALCHEMY_MAX_OVERFLOW: int = 10
    SQLALCHEMY_POOL_TIMEOUT: int = 60

    OTEL_SDK_DISABLED: bool = True

    API_HTTP_CA_BUNDLE: str = ""
    API_HTTP_SKIP_TLS_VERIFY: bool = False
    SHOPIFY_APP_CLIENT_ID: str = ""
    SHOPIFY_APP_CLIENT_SECRET: str = ""

    REDIS_DEAD_LETTER_STREAM: str = "langbridge:dead-letter"
    REDIS_WORKER_STREAM: str = "langbridge:worker_stream"
    REDIS_WORKER_CONSUMER_GROUP: str = "langbridge-worker"
    REDIS_API_STREAM: str = "langbridge:api_stream"

    SQL_FEDERATION_ENABLED: bool = True
    SQL_DEFAULT_MAX_PREVIEW_ROWS: int = 1000
    SQL_DEFAULT_MAX_EXPORT_ROWS: int = 25000
    SQL_POLICY_MAX_PREVIEW_ROWS_UPPER_BOUND: int = 50000
    SQL_POLICY_MAX_EXPORT_ROWS_UPPER_BOUND: int = 500000

    DATASET_FILE_LOCAL_DIR: str = ".cache/datasets"

    FEDERATION_ARTIFACT_DIR: str = ".cache/federation"
    FEDERATION_BROADCAST_THRESHOLD_BYTES: int = 64 * 1024 * 1024
    FEDERATION_PARTITION_COUNT: int = 8
    FEDERATION_STAGE_MAX_RETRIES: int = 4
    FEDERATION_STAGE_PARALLELISM: int = 4

    @computed_field  # type: ignore[prop-decorator]
    @property
    def IS_LOCAL(self) -> bool:
        return self.ENVIRONMENT == "local"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        if self.ENVIRONMENT == "local":
            return f"sqlite:///./{self.LOCAL_DB}"
        return (
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}"
            f"@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"
        )

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SQLALCHEMY_ASYNC_DATABASE_URI(self) -> str:
        uri = self.SQLALCHEMY_DATABASE_URI
        if uri.startswith("sqlite"):
            return uri.replace("sqlite", "sqlite+aiosqlite", 1)
        if uri.startswith("postgresql"):
            return uri.replace("postgresql", "postgresql+asyncpg", 1)
        return uri


settings = Settings()  # type: ignore
