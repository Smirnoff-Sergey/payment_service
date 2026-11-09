from datetime import timedelta
from functools import lru_cache
from pathlib import Path
from typing import Annotated

from pydantic import SecretStr, field_validator
from pydantic_settings import BaseSettings, NoDecode, SettingsConfigDict


DOCKER_SECRETS_DIR = Path("/run/secrets")


class Settings(BaseSettings):
    # Secrets come from Docker secrets (/run/secrets/<field name>); environment
    # variables and .env still take priority, which is handy for local runs.
    # The directory is optional: pydantic warns about a missing secrets_dir.
    model_config = SettingsConfigDict(
        env_file=".env",
        extra="ignore",
        secrets_dir=DOCKER_SECRETS_DIR if DOCKER_SECRETS_DIR.is_dir() else None,
    )

    database_url: str = "postgresql+asyncpg://payments:payments@localhost:5432/payments"
    rabbit_url: str = "amqp://guest:guest@localhost:5672/"
    # No defaults: the service must not start with a guessable key.
    api_key: SecretStr
    webhook_secret: SecretStr

    # API: an Idempotency-Key is reserved for this long, then it can be reused.
    idempotency_key_ttl_hours: float = 24.0

    # Outbox relay
    outbox_batch_size: int = 100
    outbox_poll_interval: float = 0.5
    # Housekeeping done by the relay: published events and expired idempotency keys.
    outbox_retention_hours: float = 24.0
    cleanup_interval: float = 60.0
    cleanup_batch_size: int = 1000

    # Consumer: retries with exponential backoff (base * 2 ** (attempt - 1))
    max_retries: int = 3
    retry_base_delay: float = 1.0

    # Payment gateway emulation
    gateway_min_delay: float = 2.0
    gateway_max_delay: float = 5.0
    gateway_success_rate: float = 0.9

    webhook_timeout: float = 5.0
    # Hosts exempt from the SSRF guard (comma-separated), e.g. the demo webhook-mock.
    webhook_allowed_hosts: Annotated[frozenset[str], NoDecode] = frozenset()

    @field_validator("webhook_allowed_hosts", mode="before")
    @classmethod
    def _split_hosts(cls, value: object) -> object:
        if isinstance(value, str):
            return frozenset(host.strip().lower() for host in value.split(",") if host.strip())
        return value

    @property
    def idempotency_key_ttl(self) -> timedelta:
        return timedelta(hours=self.idempotency_key_ttl_hours)

    @property
    def outbox_retention(self) -> timedelta:
        return timedelta(hours=self.outbox_retention_hours)


@lru_cache
def get_settings() -> Settings:
    return Settings()
