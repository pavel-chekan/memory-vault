import asyncio
import os
from collections.abc import AsyncIterator

import asyncpg
import pytest
import pytest_asyncio
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import (
    AsyncEngine,
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)
from sqlalchemy.pool import NullPool

from memory_vault.config import Settings, get_settings
from memory_vault.database import create_database_url
from memory_vault.models import Base

TEST_DATABASE_ENV = "POSTGRES_TEST_DB"
DEFAULT_TEST_DATABASE = "memory_vault_test"


def _test_database_name(settings: Settings) -> str:
    database = os.getenv(TEST_DATABASE_ENV, DEFAULT_TEST_DATABASE)

    if database == settings.postgres_db:
        raise RuntimeError("Test database must not be the application database")

    return database


def _test_database_url(settings: Settings, database: str) -> URL:
    return create_database_url(settings).set(database=database)


async def _ensure_database(settings: Settings, database: str) -> None:
    connection = await asyncpg.connect(
        host=settings.postgres_host,
        port=settings.postgres_port,
        user=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        database=settings.postgres_db,
    )

    try:
        exists = await connection.fetchval(
            "SELECT 1 FROM pg_database WHERE datname = $1",
            database,
        )

        if exists is None:
            await connection.execute(f'CREATE DATABASE "{database}"')
    finally:
        await connection.close()


async def _reset_schema(url: URL) -> None:
    engine = create_async_engine(url, poolclass=NullPool)

    try:
        async with engine.begin() as connection:
            await connection.run_sync(Base.metadata.drop_all)
            await connection.run_sync(Base.metadata.create_all)
    finally:
        await engine.dispose()


@pytest.fixture(scope="session")
def test_database_url() -> URL:
    settings = get_settings()
    database = _test_database_name(settings)

    asyncio.run(_ensure_database(settings, database))

    url = _test_database_url(settings, database)
    asyncio.run(_reset_schema(url))

    return url


@pytest_asyncio.fixture
async def engine(test_database_url: URL) -> AsyncIterator[AsyncEngine]:
    engine = create_async_engine(test_database_url, poolclass=NullPool)

    try:
        yield engine
    finally:
        await engine.dispose()


@pytest_asyncio.fixture
async def session_factory(
    engine: AsyncEngine,
) -> AsyncIterator[async_sessionmaker[AsyncSession]]:
    async with engine.connect() as connection:
        transaction = await connection.begin()

        try:
            yield async_sessionmaker(
                bind=connection,
                class_=AsyncSession,
                expire_on_commit=False,
                join_transaction_mode="create_savepoint",
            )
        finally:
            await transaction.rollback()
