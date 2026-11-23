from functools import lru_cache

from pydantic import Field, SecretStr
from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(
        env_file=".env",
        env_file_encoding="utf-8",
        extra="ignore",
    )

    app_name: str = "Memory Vault"
    debug: bool = False

    postgres_host: str = "localhost"
    postgres_port: int = 5432
    postgres_db: str = "memory_vault"
    postgres_user: str = "memory_vault"
    postgres_password: SecretStr

    telegram_bot_token: SecretStr

    outbox_worker_enabled: bool = False
    outbox_batch_size: int = 10
    outbox_lease_duration_seconds: int = 60
    outbox_heartbeat_interval_seconds: float | None = None
    outbox_poll_interval: float = 1.0
    outbox_handler_timeout_seconds: float | None = None
    outbox_shutdown_timeout_seconds: float = Field(default=30.0, gt=0)


@lru_cache
def get_settings() -> Settings:
    return Settings()  # type: ignore[call-arg]
