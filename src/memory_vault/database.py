from collections.abc import AsyncIterator

from fastapi import Request
from sqlalchemy import URL
from sqlalchemy.ext.asyncio import (
    AsyncSession,
)

from memory_vault.config import Settings


def create_database_url(settings: Settings) -> URL:
    return URL.create(
        drivername="postgresql+asyncpg",
        username=settings.postgres_user,
        password=settings.postgres_password.get_secret_value(),
        host=settings.postgres_host,
        port=settings.postgres_port,
        database=settings.postgres_db,
    )


async def get_session(request: Request) -> AsyncIterator[AsyncSession]:
    session_factory = request.app.state.session_factory

    async with session_factory() as session:
        yield session
