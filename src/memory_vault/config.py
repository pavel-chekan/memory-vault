from functools import lru_cache

from pydantic import SecretStr
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


@lru_cache
def get_settings() -> Settings:
    return Settings()
