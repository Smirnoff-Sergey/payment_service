from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbit_url: str = "amqp://guest:guest@localhost:5672/"
    api_key: str = "change-me"

    # Outbox relay
    outbox_batch_size: int = 100
    outbox_poll_interval: float = 0.5

    # Consumer: retries with exponential backoff (base * 2 ** (attempt - 1))
    max_retries: int = 3
    retry_base_delay: float = 1.0

    # Payment gateway emulation
    gateway_min_delay: float = 2.0
    gateway_max_delay: float = 5.0
    gateway_success_rate: float = 0.9

    webhook_timeout: float = 5.0


@lru_cache
def get_settings() -> Settings:
    return Settings()
