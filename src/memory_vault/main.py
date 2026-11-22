import asyncio
import logging
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager, suppress
from typing import Annotated

from aiogram import Bot, Dispatcher
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
from memory_vault.workers.handlers import create_outbox_handlers
from memory_vault.workers.outbox_worker import OutboxWorker

logger = logging.getLogger(__name__)


async def _wait_for_task(task: asyncio.Task[None], wait_timeout: float) -> bool:
    """Return whether ``task`` finished within ``wait_timeout``.

    ``asyncio.wait`` is used instead of ``asyncio.wait_for`` on purpose:
    ``wait_for`` waits for the awaited task's cancellation to complete, so a
    handler that absorbs ``CancelledError`` can keep it blocked past the
    timeout. ``wait`` returns on the deadline regardless of the task.
    """
    try:
        done, _ = await asyncio.wait({task}, timeout=wait_timeout)
    except asyncio.CancelledError:
        task.cancel()
        raise

    return bool(done)


def _log_task_result(task: asyncio.Task[None]) -> None:
    try:
        task.result()
    except asyncio.CancelledError:
        pass
    except Exception:
        logger.exception("Outbox worker stopped with an error")


async def _stop_outbox_worker(
    worker: OutboxWorker | None,
    task: asyncio.Task[None] | None,
    shutdown_timeout: float,
) -> None:
    if worker is None or task is None:
        return

    worker.stop()

    if await _wait_for_task(task, shutdown_timeout):
        _log_task_result(task)
        return

    logger.warning(
        "Outbox worker did not stop within %s seconds; cancelling it",
        shutdown_timeout,
    )
    task.cancel()

    if await _wait_for_task(task, shutdown_timeout):
        _log_task_result(task)
        return

    logger.error(
        "Outbox worker did not stop %s seconds after cancellation; "
        "abandoning it (a handler is not cancellation-cooperative)",
        shutdown_timeout,
    )


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

    bot: Bot | None = None
    dispatcher: Dispatcher | None = None
    polling_task: asyncio.Task[None] | None = None
    outbox_worker: OutboxWorker | None = None
    outbox_task: asyncio.Task[None] | None = None

    # Everything that can fail at startup lives inside the try/finally, so a
    # misconfigured worker (or any other startup error) still disposes the
    # engine and closes the bot session.
    try:
        if settings.outbox_worker_enabled:
            outbox_worker = OutboxWorker(
                session_factory,
                handlers=create_outbox_handlers(),
                batch_size=settings.outbox_batch_size,
                lease_duration_seconds=settings.outbox_lease_duration_seconds,
                heartbeat_interval_seconds=settings.outbox_heartbeat_interval_seconds,
                handler_timeout_seconds=settings.outbox_handler_timeout_seconds,
                poll_interval=settings.outbox_poll_interval,
            )

        bot = Bot(token=settings.telegram_bot_token.get_secret_value())
        dispatcher = create_dispatcher(session_factory)

        polling_task = asyncio.create_task(
            dispatcher.start_polling(
                bot,
                handle_signals=False,
                close_bot_session=False,
            )
        )

        if outbox_worker is not None:
            outbox_task = asyncio.create_task(
                outbox_worker.run(),
                name="outbox-worker",
            )

        yield
    finally:
        try:
            await _stop_outbox_worker(
                outbox_worker,
                outbox_task,
                settings.outbox_shutdown_timeout_seconds,
            )
        finally:
            try:
                if dispatcher is not None and polling_task is not None and not polling_task.done():
                    try:
                        await dispatcher.stop_polling()
                    except RuntimeError:
                        polling_task.cancel()
            finally:
                try:
                    if polling_task is not None:
                        with suppress(asyncio.CancelledError):
                            await polling_task
                finally:
                    try:
                        if bot is not None:
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
async def health() -> dict[str, str]:
    return {"status": "ok"}


@app.get("/health/db")
async def database_health(
    session: Annotated[AsyncSession, Depends(get_session)],
) -> dict[str, str | int]:
    result = await session.execute(text("SELECT 1"))

    return {
        "database": "ok",
        "result": result.scalar_one(),
    }
