from aiogram import Router
from aiogram.filters import CommandStart
from aiogram.types import Message
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from memory_vault.schemas.users import TelegramUserCreate
from memory_vault.services.users import UserService

router = Router(name="start")


@router.message(CommandStart())
async def handle_start(
    message: Message,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    if message.from_user is None:
        return

    data = TelegramUserCreate(
        telegram_user_id=message.from_user.id,
        username=message.from_user.username,
        first_name=message.from_user.first_name,
        last_name=message.from_user.last_name,
        language_code=message.from_user.language_code,
    )

    async with session_factory() as session:
        user = await UserService(session).register(data)

    await message.answer(f"Welcome, {user.first_name}!")
