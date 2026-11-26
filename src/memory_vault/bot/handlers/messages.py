from aiogram import F, Router
from aiogram.types import Message, Update
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from memory_vault.schemas.messages import TelegramMessageCreate
from memory_vault.schemas.users import TelegramUserCreate
from memory_vault.services.saved_messages import SavedMessageService

router = Router(name="messages")


@router.message(F.text & ~F.text.startswith("/"))
async def handle_text_message(
    message: Message,
    event_update: Update,
    session_factory: async_sessionmaker[AsyncSession],
) -> None:
    """Adapt a Telegram text message into a ``SavedMessageService`` call.

    The handler only maps Telegram objects to the application DTO and awaits
    the service inside the handler's own turn: it does not call an LLM and does
    not spawn background tasks. Duplicate deliveries are a silent no-op because
    the service is idempotent on ``telegram_update_id``.
    """
    sender = message.from_user
    text = message.text

    if sender is None or text is None:
        return

    data = TelegramMessageCreate(
        telegram_update_id=event_update.update_id,
        telegram_chat_id=message.chat.id,
        telegram_message_id=message.message_id,
        text=text,
        message_date=message.date,
        user=TelegramUserCreate(
            telegram_user_id=sender.id,
            username=sender.username,
            first_name=sender.first_name,
            last_name=sender.last_name,
            language_code=sender.language_code,
        ),
    )

    async with session_factory() as session:
        await SavedMessageService(session).save_incoming_message(data)
