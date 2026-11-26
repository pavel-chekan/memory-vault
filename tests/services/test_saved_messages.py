import asyncio
import uuid
from datetime import UTC, datetime
from typing import Any

import pytest
from pydantic import ValidationError
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from memory_vault.models.outbox_event import OutboxEvent, OutboxEventStatus
from memory_vault.models.saved_message import SavedMessage
from memory_vault.models.user import User
from memory_vault.repositories.outbox import OutboxRepository
from memory_vault.schemas.messages import TelegramMessageCreate
from memory_vault.schemas.users import TelegramUserCreate
from memory_vault.services.saved_messages import (
    MESSAGE_RECEIVED_EVENT_TYPE,
    SAVED_MESSAGE_AGGREGATE_TYPE,
    SavedMessageService,
)
from memory_vault.services.users import UserService

SessionFactory = async_sessionmaker[AsyncSession]


def _message_data(**overrides: Any) -> TelegramMessageCreate:
    values: dict[str, Any] = {
        "telegram_update_id": 990001,
        "telegram_chat_id": 1001,
        "telegram_message_id": 1,
        "text": "remember this",
        "message_date": datetime(2026, 11, 26, 9, 0, tzinfo=UTC),
        "user": TelegramUserCreate(telegram_user_id=8001, first_name="Alice"),
    }
    values.update(overrides)

    return TelegramMessageCreate(**values)


async def _save(
    session_factory: SessionFactory,
    data: TelegramMessageCreate,
) -> SavedMessage | None:
    async with session_factory() as session:
        return await SavedMessageService(session).save_incoming_message(data)


async def _count_messages(
    session_factory: SessionFactory,
    *,
    telegram_update_id: int,
) -> int:
    async with session_factory() as session:
        total = await session.scalar(
            select(func.count())
            .select_from(SavedMessage)
            .where(SavedMessage.telegram_update_id == telegram_update_id)
        )

    return int(total or 0)


async def _count_events(
    session_factory: SessionFactory,
    *,
    aggregate_id: uuid.UUID,
) -> int:
    async with session_factory() as session:
        total = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(OutboxEvent.aggregate_id == aggregate_id)
        )

    return int(total or 0)


async def _count_received_events(session_factory: SessionFactory) -> int:
    async with session_factory() as session:
        total = await session.scalar(
            select(func.count())
            .select_from(OutboxEvent)
            .where(OutboxEvent.event_type == MESSAGE_RECEIVED_EVENT_TYPE)
        )

    return int(total or 0)


async def _count_users(
    session_factory: SessionFactory,
    *,
    telegram_user_id: int,
) -> int:
    async with session_factory() as session:
        total = await session.scalar(
            select(func.count()).select_from(User).where(User.telegram_user_id == telegram_user_id)
        )

    return int(total or 0)


async def _cleanup(
    session_factory: SessionFactory,
    *,
    telegram_update_id: int,
    telegram_user_id: int,
) -> None:
    async with session_factory() as session:
        message_ids = list(
            (
                await session.scalars(
                    select(SavedMessage.id).where(
                        SavedMessage.telegram_update_id == telegram_update_id
                    )
                )
            ).all()
        )

        if message_ids:
            await session.execute(
                delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(message_ids))
            )

        await session.execute(
            delete(SavedMessage).where(SavedMessage.telegram_update_id == telegram_update_id)
        )
        await session.execute(delete(User).where(User.telegram_user_id == telegram_user_id))
        await session.commit()


async def test_saves_message_and_creates_outbox_event(
    session_factory: SessionFactory,
) -> None:
    data = _message_data()

    message = await _save(session_factory, data)

    assert message is not None

    async with session_factory() as session:
        stored = await session.get(SavedMessage, message.id)
        user = await session.scalar(
            select(User).where(User.telegram_user_id == data.user.telegram_user_id)
        )
        event = await session.scalar(
            select(OutboxEvent).where(OutboxEvent.aggregate_id == message.id)
        )

    assert stored is not None
    assert user is not None
    assert stored.user_id == user.id
    assert stored.telegram_chat_id == data.telegram_chat_id
    assert stored.telegram_message_id == data.telegram_message_id
    assert stored.telegram_update_id == data.telegram_update_id
    assert stored.text == data.text
    assert stored.message_date == data.message_date
    assert stored.created_at is not None

    assert event is not None
    assert event.event_type == MESSAGE_RECEIVED_EVENT_TYPE
    assert event.aggregate_type == SAVED_MESSAGE_AGGREGATE_TYPE
    assert event.aggregate_id == message.id
    assert event.payload == {"saved_message_id": str(message.id)}
    assert event.status == OutboxEventStatus.PENDING


async def test_creates_unknown_sender_in_same_transaction(
    session_factory: SessionFactory,
) -> None:
    data = _message_data(
        telegram_update_id=990101,
        telegram_chat_id=1101,
        telegram_message_id=11,
        user=TelegramUserCreate(telegram_user_id=8101, first_name="Bob"),
    )

    message = await _save(session_factory, data)

    assert message is not None

    async with session_factory() as session:
        user = await session.scalar(select(User).where(User.telegram_user_id == 8101))

    assert user is not None
    assert message.user_id == user.id


async def test_existing_sender_is_reused(
    session_factory: SessionFactory,
) -> None:
    sender = TelegramUserCreate(
        telegram_user_id=8102,
        username="gina",
        first_name="Gina",
    )

    async with session_factory() as session:
        existing = await UserService(session).register(sender)

    message = await _save(
        session_factory,
        _message_data(
            telegram_update_id=990102,
            telegram_chat_id=1102,
            telegram_message_id=12,
            user=sender,
        ),
    )

    assert message is not None
    assert message.user_id == existing.id
    assert await _count_users(session_factory, telegram_user_id=8102) == 1


async def test_duplicate_update_creates_nothing(
    session_factory: SessionFactory,
) -> None:
    data = _message_data(
        telegram_update_id=990201,
        telegram_chat_id=1201,
        telegram_message_id=21,
        user=TelegramUserCreate(telegram_user_id=8201, first_name="Carol"),
    )

    first = await _save(session_factory, data)
    second = await _save(session_factory, data)

    assert first is not None
    assert second is None
    assert await _count_messages(session_factory, telegram_update_id=990201) == 1
    assert await _count_events(session_factory, aggregate_id=first.id) == 1


async def test_concurrent_duplicate_deliveries_create_single_message_and_event(
    engine: AsyncEngine,
) -> None:
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    data = _message_data(
        telegram_update_id=990301,
        telegram_chat_id=1301,
        telegram_message_id=31,
        user=TelegramUserCreate(telegram_user_id=8301, first_name="Dave"),
    )

    try:
        first, second = await asyncio.gather(
            _save(session_factory, data),
            _save(session_factory, data),
            return_exceptions=True,
        )

        assert not isinstance(first, BaseException)
        assert not isinstance(second, BaseException)
        assert (first is None) != (second is None)

        winner = first if first is not None else second
        assert winner is not None

        assert await _count_messages(session_factory, telegram_update_id=990301) == 1
        assert await _count_events(session_factory, aggregate_id=winner.id) == 1
    finally:
        await _cleanup(
            session_factory,
            telegram_update_id=990301,
            telegram_user_id=8301,
        )


async def test_event_failure_rolls_back_message(
    engine: AsyncEngine,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    data = _message_data(
        telegram_update_id=990401,
        telegram_chat_id=1401,
        telegram_message_id=41,
        user=TelegramUserCreate(telegram_user_id=8401, first_name="Eve"),
    )

    async def _failing_create(*_args: Any, **_kwargs: Any) -> OutboxEvent:
        raise RuntimeError("outbox unavailable")

    monkeypatch.setattr(OutboxRepository, "create", _failing_create)

    try:
        events_before = await _count_received_events(session_factory)

        with pytest.raises(RuntimeError, match="outbox unavailable"):
            await _save(session_factory, data)

        assert await _count_messages(session_factory, telegram_update_id=990401) == 0
        assert await _count_received_events(session_factory) == events_before
        assert await _count_users(session_factory, telegram_user_id=8401) == 0
    finally:
        await _cleanup(
            session_factory,
            telegram_update_id=990401,
            telegram_user_id=8401,
        )


async def test_duplicate_chat_message_pair_with_new_update_is_rejected(
    session_factory: SessionFactory,
) -> None:
    user = TelegramUserCreate(telegram_user_id=8501, first_name="Frank")

    first = await _save(
        session_factory,
        _message_data(
            telegram_update_id=990501,
            telegram_chat_id=1501,
            telegram_message_id=51,
            user=user,
        ),
    )

    assert first is not None
    events_before = await _count_received_events(session_factory)

    with pytest.raises(IntegrityError):
        await _save(
            session_factory,
            _message_data(
                telegram_update_id=990502,
                telegram_chat_id=1501,
                telegram_message_id=51,
                user=user,
            ),
        )

    assert await _count_messages(session_factory, telegram_update_id=990501) == 1
    assert await _count_messages(session_factory, telegram_update_id=990502) == 0
    assert await _count_events(session_factory, aggregate_id=first.id) == 1
    assert await _count_received_events(session_factory) == events_before


def test_naive_message_date_is_rejected() -> None:
    with pytest.raises(ValidationError):
        _message_data(message_date=datetime(2026, 11, 26, 9, 0))


@pytest.mark.parametrize(
    "overrides",
    [
        {"telegram_update_id": 0},
        {"telegram_update_id": -1},
        {"telegram_message_id": 0},
        {"telegram_message_id": -1},
        {"text": "x" * 4097},
    ],
)
def test_invalid_message_fields_are_rejected(
    overrides: dict[str, Any],
) -> None:
    with pytest.raises(ValidationError):
        _message_data(**overrides)


def test_empty_text_is_rejected() -> None:
    with pytest.raises(ValidationError):
        _message_data(text="")


def test_negative_group_chat_id_is_accepted() -> None:
    data = _message_data(telegram_chat_id=-1001234567890)

    assert data.telegram_chat_id == -1001234567890
