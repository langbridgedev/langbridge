import secrets
from typing import Any, Literal

from pydantic import (
    HttpUrl,
    computed_field,
)
from pydantic_settings import BaseSettings, SettingsConfigDict

def parse_cors(v: Any) -> list[str] | str:
    if isinstance(v, str) and not v.startswith("["):
        return [i.strip() for i in v.split(",")]
    elif isinstance(v, list | str):
        return v
    raise ValueError(v)


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=(".env", "langbridge/.env"), env_ignore_empty=True, extra="ignore"
    )
    API_V1_STR: str = "/api/v1"
    SECRET_KEY: str = secrets.token_urlsafe(32)
    DOMAIN: str = "localhost"
    ENVIRONMENT: Literal["local", "staging", "development", "production"] = "local"
    
    IS_LOCAL: bool = ENVIRONMENT == "local"

    UVICORN_RELOAD: bool = False
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def server_host(self) -> str:
        if self.ENVIRONMENT in ("local", "development"):
            return f"http://{self.DOMAIN}"
        return f"https://{self.DOMAIN}"
    
    CORS_ENABLED: bool = True
    
    @computed_field  # type: ignore[prop-decorator]
    @property
    def SQLALCHEMY_DATABASE_URI(self) -> str:
        if self.ENVIRONMENT == "local":
            return f"sqlite:///./{self.LOCAL_DB}"
        else:
            return f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_SERVER}:{self.POSTGRES_PORT}/{self.POSTGRES_DB}"

    @computed_field  # type: ignore[prop-decorator]
    @property
    def SQLALCHEMY_ASYNC_DATABASE_URI(self) -> str:
        uri = self.SQLALCHEMY_DATABASE_URI
        if uri.startswith("sqlite+"):
            return uri
        if uri.startswith("sqlite"):
            return uri.replace("sqlite", "sqlite+aiosqlite", 1)
        if uri.startswith("postgresql+"):
            return uri
        if uri.startswith("postgresql"):
            return uri.replace("postgresql", "postgresql+asyncpg", 1)
        return uri

    PROJECT_NAME: str = "FastAPI app"
    SENTRY_DSN: HttpUrl | None = None
    POSTGRES_SERVER: str = "localhost"
    POSTGRES_PORT: int = 5432
    POSTGRES_USER: str = ""
    POSTGRES_PASSWORD: str = ""
    POSTGRES_DB: str = ""
    LOCAL_DB: str = "local.db"
    SQLALCHEMY_POOL_SIZE: int = 5
    SQLALCHEMY_MAX_OVERFLOW: int = 10
    SQLALCHEMY_POOL_TIMEOUT: int = 30

    SHOPIFY_APP_CLIENT_ID: str = ""
    SHOPIFY_APP_CLIENT_SECRET: str = ""
    
    REDIS_HOST: str = "localhost"
    REDIS_PORT: int = 6379
    REDIS_CHANNEL: str = "events"
    REDIS_STREAM: str = "langbridge:events"
    REDIS_CONSUMER_GROUP: str = "langbridge-workers"
    REDIS_CONSUMER_NAME: str = ""
    REDIS_DEAD_LETTER_STREAM: str = "langbridge:dead-letter"
    REDIS_WORKER_STREAM: str = "langbridge:worker_stream"
    REDIS_WORKER_CONSUMER_GROUP: str = "langbridge-worker"
    REDIS_API_STREAM: str = "langbridge:api_stream"
    REDIS_API_CONSUMER_GROUP: str = "langbridge-api"
    EDGE_REDIS_PREFIX: str = "langbridge:edge"
    
    AGENT_MEMORY_EXCHANGE_SIZE: int = 100
    
    SERVICE_USER_SECRET: str = ""
    DEFAULT_EXPIRES_DAYS: int = 365
    
    STORAGE_SETTING: Literal["local", "azure"] = "local"
    DASHBOARD_SNAPSHOT_STORAGE_BACKEND: Literal["local", "azure_blob", "s3"] = "local"
    DASHBOARD_SNAPSHOT_LOCAL_DIR: str = ".cache/dashboard_snapshots"
    DATASET_FILE_ENABLED: bool = True
    DATASET_LOCAL_STORAGE_DIR: str = ".cache/datasets"
    SQL_FEATURE_ENABLED: bool = True
    SQL_AI_HELPER_ENABLED: bool = True
    SQL_FEDERATION_ENABLED: bool = True
    SQL_DEFAULT_MAX_PREVIEW_ROWS: int = 1000
    SQL_DEFAULT_MAX_EXPORT_ROWS: int = 25000
    SQL_DEFAULT_MAX_RUNTIME_SECONDS: int = 30
    SQL_DEFAULT_MAX_CONCURRENCY: int = 3
    SQL_DEFAULT_ALLOW_DML: bool = False
    SQL_DEFAULT_ALLOW_FEDERATION: bool = True
    SQL_POLICY_MAX_PREVIEW_ROWS_UPPER_BOUND: int = 50000
    SQL_POLICY_MAX_EXPORT_ROWS_UPPER_BOUND: int = 500000
    SQL_POLICY_MAX_RUNTIME_SECONDS_UPPER_BOUND: int = 600
    SQL_POLICY_MAX_CONCURRENCY_UPPER_BOUND: int = 20
    SQL_ARTIFACT_LOCAL_DIR: str = ".cache/sql_artifacts"
    DATASET_FILE_ENABLED: bool = True
    DATASET_FILE_LOCAL_DIR: str = ".cache/datasets"

    BACKEND_URL: str = "http://localhost:8000"
    FRONTEND_URL: str = "http://localhost:3000"

    JWT_SECRET: str
    JWT_ALG: str = "HS256"
    JWT_EXPIRES_MIN: int = 60 * 24 * 30 # 30 days
    EDGE_RUNTIME_JWT_SECRET: str = ""
    EDGE_RUNTIME_TOKEN_TTL_SECONDS: int = 900
    EDGE_RUNTIME_REGISTRATION_TOKEN_TTL_MINUTES: int = 60
    DEFAULT_EXECUTION_MODE: Literal["hosted", "customer_runtime"] = "hosted"
    COOKIE_NAME: str = "langbridge_token"
    COOKIE_SECURE: bool = False  # set True in production with HTTPS

    GITHUB_CLIENT_ID: str
    GITHUB_CLIENT_SECRET: str
    GITHUB_SCOPE: str = "read:user user:email"
    GITHUB_AUTHORIZE_URL: str = "https://github.com/login/oauth/authorize"
    GITHUB_ACCESS_TOKEN_URL: str = "https://github.com/login/oauth/access_token"
    GITHUB_API_BASE_URL: str = "https://api.github.com/"

    GOOGLE_CLIENT_ID: str = ""
    GOOGLE_CLIENT_SECRET: str = ""
    GOOGLE_AUTHORIZE_URL: str = "https://accounts.google.com/o/oauth2/v2/auth"
    GOOGLE_ACCESS_TOKEN_URL: str = "https://oauth2.googleapis.com/token"
    GOOGLE_API_BASE_URL: str = "https://www.googleapis.com/oauth2/v2/"

    SESSION_SECRET: str = "supersecretkey"
    
    CONFIG_KEYRING: str = "default"  # JSON-encoded dict of base64 keys, e.g. '{"key-id-1": "base64key1",
    CONFIG_ACTIVE_KEY: str = "default"  # key ID of the active key in the keyring
    FEDERATION_ARTIFACT_DIR: str = ".cache/federation"
    FEDERATION_BROADCAST_THRESHOLD_BYTES: int = 64 * 1024 * 1024
    FEDERATION_PARTITION_COUNT: int = 8
    FEDERATION_STAGE_MAX_RETRIES: int = 4
    FEDERATION_STAGE_PARALLELISM: int = 4

settings = Settings()  # type: ignore
