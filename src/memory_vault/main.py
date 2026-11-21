import asyncio
from contextlib import asynccontextmanager, suppress
from typing import Annotated, AsyncIterator

from aiogram import Bot
from fastapi import Depends, FastAPI
from sqlalchemy import text
from sqlalchemy.ext.asyncio import (
    AsyncSession,
    async_sessionmaker,
    create_async_engine,
)

from memory_vault.api.routes.users import router as users_router
from memory_vault.bot.dispatcher import create_dispatcher
from memory_vault.config import get_settings
from memory_vault.database import create_database_url, get_session


@asynccontextmanager
async def lifespan(app: FastAPI) -> AsyncIterator[None]:
    settings = get_settings()

    engine = create_async_engine(
        create_database_url(settings),
        pool_pre_ping=True,
    )

    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    app.state.session_factory = session_factory

    bot = Bot(token=settings.telegram_bot_token.get_secret_value())
    dispatcher = create_dispatcher(session_factory)

    polling_task = asyncio.create_task(
        dispatcher.start_polling(
            bot,
            handle_signals=False,
            close_bot_session=False,
        )
    )

    try:
        yield
    finally:
        try:
            if not polling_task.done():
                try:
                    await dispatcher.stop_polling()
                except RuntimeError:
                    polling_task.cancel()
        finally:
            try:
                with suppress(asyncio.CancelledError):
                    await polling_task
            finally:
                try:
                    await bot.session.close()
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
