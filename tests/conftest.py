"""Test fixtures.

Integration tests need a real PostgreSQL: DATABASE_URL must point to a disposable
database (it is dropped and re-created). In docker compose: `docker compose run --rm tests`.
"""

import asyncio
import os
from collections.abc import AsyncIterator
from decimal import Decimal

import asyncpg
import pytest
from alembic import command
from alembic.config import Config
from httpx import ASGITransport, AsyncClient
from sqlalchemy import make_url, text
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker
from sqlalchemy.pool import NullPool

from app.api.main import app
from app.config import get_settings
from app.db import create_engine, create_session_factory, get_session
from app.schemas import PaymentCreate

API_KEY = "test-api-key"
os.environ["API_KEY"] = API_KEY
get_settings.cache_clear()


async def _recreate_database(url: str) -> None:
    db_url = make_url(url)
    conn = await asyncpg.connect(
        user=db_url.username,
        password=db_url.password,
        host=db_url.host,
        port=db_url.port or 5432,
        database="postgres",
    )
    try:
        await conn.execute(f'DROP DATABASE IF EXISTS "{db_url.database}" WITH (FORCE)')
        await conn.execute(f'CREATE DATABASE "{db_url.database}"')
    finally:
        await conn.close()


@pytest.fixture(scope="session")
def database_url() -> str:
    url = os.environ["DATABASE_URL"]
    asyncio.run(_recreate_database(url))
    config = Config("alembic.ini")
    config.attributes["database_url"] = url
    command.upgrade(config, "head")
    return url


@pytest.fixture
async def engine(database_url: str) -> AsyncIterator[AsyncEngine]:
    engine = create_engine(database_url, poolclass=NullPool)
    async with engine.begin() as conn:
        await conn.execute(text("TRUNCATE payments, outbox"))
    yield engine
    await engine.dispose()


@pytest.fixture
def sessions(engine: AsyncEngine) -> async_sessionmaker[AsyncSession]:
    return create_session_factory(engine)


@pytest.fixture
async def client(sessions: async_sessionmaker[AsyncSession]) -> AsyncIterator[AsyncClient]:
    async def override_session() -> AsyncIterator[AsyncSession]:
        async with sessions() as session:
            yield session

    app.dependency_overrides[get_session] = override_session
    async with AsyncClient(
        transport=ASGITransport(app=app), base_url="http://test", headers={"X-API-Key": API_KEY}
    ) as client:
        yield client
    app.dependency_overrides.clear()


@pytest.fixture
def payment_data() -> PaymentCreate:
    return PaymentCreate(
        amount=Decimal("100.50"),
        currency="RUB",
        description="Order #42",
        metadata={"order_id": 42},
        webhook_url="http://webhook.test/hook",
    )
