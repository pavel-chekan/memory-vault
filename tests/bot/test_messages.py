from collections.abc import AsyncIterator
from datetime import UTC, datetime

import pytest
import pytest_asyncio
from aiogram import Bot
from aiogram.enums import ChatType
from aiogram.types import Chat, Message, Update, User
from sqlalchemy import select
from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from memory_vault.bot.dispatcher import create_dispatcher
from memory_vault.bot.handlers.messages import handle_text_message
from memory_vault.models.outbox_event import OutboxEvent
from memory_vault.models.saved_message import SavedMessage
from memory_vault.models.user import User as DbUser
from memory_vault.schemas.messages import TelegramMessageCreate
from memory_vault.services.saved_messages import (
    MESSAGE_RECEIVED_EVENT_TYPE,
    SAVED_MESSAGE_AGGREGATE_TYPE,
    SavedMessageService,
)

SessionFactory = async_sessionmaker[AsyncSession]

MESSAGE_DATE = datetime(2026, 11, 26, 10, 0, tzinfo=UTC)
BOT_TOKEN = "42:" + "A" * 35


@pytest_asyncio.fixture
async def bot() -> AsyncIterator[Bot]:
    bot = Bot(token=BOT_TOKEN)

    try:
        yield bot
    finally:
        await bot.session.close()


def _message(
    *,
    update_id: int = 991001,
    chat_id: int = 1701,
    message_id: int = 71,
    user_id: int = 8701,
    text: str | None = "remember this",
    has_sender: bool = True,
) -> tuple[Message, Update]:
    sender = User(
        id=user_id,
        is_bot=False,
        first_name="Alice",
        username="alice",
        language_code="en",
    )
    message = Message(
        message_id=message_id,
        date=MESSAGE_DATE,
        chat=Chat(id=chat_id, type=ChatType.PRIVATE),
        from_user=sender if has_sender else None,
        text=text,
    )

    return message, Update(update_id=update_id, message=message)


async def test_handler_maps_telegram_update_to_service_dto(
    session_factory: SessionFactory,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[TelegramMessageCreate] = []

    async def fake_save(
        self: SavedMessageService,
        data: TelegramMessageCreate,
    ) -> None:
        captured.append(data)

    monkeypatch.setattr(SavedMessageService, "save_incoming_message", fake_save)

    message, update = _message()

    await handle_text_message(
        message=message,
        event_update=update,
        session_factory=session_factory,
    )

    assert len(captured) == 1
    data = captured[0]
    assert data.telegram_update_id == 991001
    assert data.telegram_chat_id == 1701
    assert data.telegram_message_id == 71
    assert data.text == "remember this"
    assert data.message_date == MESSAGE_DATE
    assert data.user.telegram_user_id == 8701
    assert data.user.username == "alice"
    assert data.user.first_name == "Alice"
    assert data.user.last_name is None
    assert data.user.language_code == "en"


async def test_handler_persists_message_and_outbox_event(
    session_factory: SessionFactory,
) -> None:
    message, update = _message(
        update_id=991101,
        chat_id=1711,
        message_id=81,
        user_id=8711,
    )

    await handle_text_message(
        message=message,
        event_update=update,
        session_factory=session_factory,
    )

    async with session_factory() as session:
        stored = await session.scalar(
            select(SavedMessage).where(SavedMessage.telegram_update_id == 991101)
        )
        assert stored is not None
        event = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == stored.id)
        )
        db_user = await session.scalar(select(DbUser).where(DbUser.telegram_user_id == 8711))

    assert stored.text == "remember this"
    assert stored.telegram_chat_id == 1711
    assert stored.telegram_message_id == 81
    assert stored.message_date == MESSAGE_DATE
    assert db_user is not None
    assert stored.user_id == db_user.id

    assert event is not None
    assert event.event_type == MESSAGE_RECEIVED_EVENT_TYPE
    assert event.aggregate_type == SAVED_MESSAGE_AGGREGATE_TYPE
    assert event.aggregate_id == stored.id
    assert event.payload == {"saved_message_id": str(stored.id)}


@pytest.mark.parametrize(
    ("has_sender", "text"),
    [
        (False, "remember this"),
        (True, None),
    ],
)
async def test_handler_ignores_messages_without_sender_or_text(
    session_factory: SessionFactory,
    monkeypatch: pytest.MonkeyPatch,
    has_sender: bool,
    text: str | None,
) -> None:
    called = False

    async def fake_save(
        self: SavedMessageService,
        data: TelegramMessageCreate,
    ) -> None:
        nonlocal called
        called = True

    monkeypatch.setattr(SavedMessageService, "save_incoming_message", fake_save)

    message, update = _message(has_sender=has_sender, text=text)

    await handle_text_message(
        message=message,
        event_update=update,
        session_factory=session_factory,
    )

    assert called is False


async def test_dispatcher_routes_text_and_ignores_commands(
    session_factory: SessionFactory,
    bot: Bot,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    captured: list[TelegramMessageCreate] = []

    async def fake_save(
        self: SavedMessageService,
        data: TelegramMessageCreate,
    ) -> None:
        captured.append(data)

    monkeypatch.setattr(SavedMessageService, "save_incoming_message", fake_save)

    dispatcher = create_dispatcher(session_factory)
    _, text_update = _message(
        update_id=991201,
        chat_id=1721,
        message_id=91,
        user_id=8721,
    )
    _, command_update = _message(
        update_id=991301,
        chat_id=1731,
        message_id=101,
        user_id=8731,
        text="/help",
    )

    await dispatcher.feed_update(bot, text_update)
    await dispatcher.feed_update(bot, command_update)

    assert len(captured) == 1
    assert captured[0].telegram_update_id == 991201
    assert captured[0].telegram_chat_id == 1721
