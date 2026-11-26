from aiogram import Dispatcher
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from memory_vault.bot.handlers import messages, start


def create_dispatcher(
    session_factory: async_sessionmaker[AsyncSession],
) -> Dispatcher:
    dispatcher = Dispatcher()
    dispatcher["session_factory"] = session_factory
    dispatcher.include_router(start.router)
    dispatcher.include_router(messages.router)

    return dispatcher
