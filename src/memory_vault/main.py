from contextlib import asynccontextmanager
from typing import Annotated

from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from memory_vault.api.routes.users import router as users_router
from memory_vault.config import get_settings
from memory_vault.database import create_database_url, get_session


@asynccontextmanager
async def lifespan(app: FastAPI):
    settings = get_settings()

    engine = create_async_engine(
        create_database_url(settings),
        pool_pre_ping=True,
    )

    app.state.session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    try:
        yield
    finally:
        await engine.dispose()


app = FastAPI(
    title=get_settings().app_name,
    debug=get_settings().debug,
    lifespan=lifespan,
)

app.include_router(users_router)


@app.get("/health")
async def health():
    return {"status": "ok"}


@app.get("/health/db")
async def database_health(
    session: Annotated[AsyncSession, Depends(get_session)],
):
    result = await session.execute(text("SELECT 1"))

    return {
        "database": "ok",
        "result": result.scalar_one(),
    }
