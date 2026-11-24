import asyncio
import uuid
from datetime import UTC, datetime

import pytest
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from memory_vault.models.saved_message import SavedMessage
from memory_vault.models.user import User
from memory_vault.schemas.users import TelegramUserCreate
from memory_vault.services.users import UserService

SessionFactory = async_sessionmaker[AsyncSession]


def _message_date() -> datetime:
    return datetime.now(UTC)


async def _register_user(
    session_factory: SessionFactory,
    telegram_user_id: int,
) -> User:
    async with session_factory() as session:
        return await UserService(session).register(
            TelegramUserCreate(
                telegram_user_id=telegram_user_id,
                first_name="Test",
            )
        )


def _message(
    *,
    user_id: uuid.UUID,
    telegram_chat_id: int,
    telegram_message_id: int,
    telegram_update_id: int,
    text: str = "hello",
    message_date: datetime | None = None,
) -> SavedMessage:
    return SavedMessage(
        user_id=user_id,
        telegram_chat_id=telegram_chat_id,
        telegram_message_id=telegram_message_id,
        telegram_update_id=telegram_update_id,
        text=text,
        message_date=message_date if message_date is not None else _message_date(),
    )


async def _insert_message(
    session_factory: SessionFactory,
    message: SavedMessage,
) -> SavedMessage:
    async with session_factory() as session:
        session.add(message)

        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            raise

        await session.refresh(message)

    return message


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


async def test_create_saved_message_persists_all_fields(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 7001)
    message_date = datetime(2026, 11, 24, 10, 30, tzinfo=UTC)

    message = await _insert_message(
        session_factory,
        _message(
            user_id=user.id,
            telegram_chat_id=-1001234567890,
            telegram_message_id=42,
            telegram_update_id=999001,
            text="remember this",
            message_date=message_date,
        ),
    )

    async with session_factory() as session:
        stored = await session.get(SavedMessage, message.id)

    assert stored is not None
    assert isinstance(stored.id, uuid.UUID)
    assert stored.user_id == user.id
    assert stored.telegram_chat_id == -1001234567890
    assert stored.telegram_message_id == 42
    assert stored.telegram_update_id == 999001
    assert stored.text == "remember this"
    assert stored.message_date == message_date
    assert stored.created_at is not None


async def test_duplicate_telegram_update_id_is_rejected(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 7002)

    await _insert_message(
        session_factory,
        _message(
            user_id=user.id,
            telegram_chat_id=1001,
            telegram_message_id=1001,
            telegram_update_id=999002,
        ),
    )

    async with session_factory() as session:
        session.add(
            _message(
                user_id=user.id,
                telegram_chat_id=1001,
                telegram_message_id=1002,
                telegram_update_id=999002,
            )
        )

        with pytest.raises(IntegrityError):
            await session.commit()

    assert await _count_messages(session_factory, telegram_update_id=999002) == 1


async def test_duplicate_chat_message_pair_is_rejected(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 7003)

    await _insert_message(
        session_factory,
        _message(
            user_id=user.id,
            telegram_chat_id=1002,
            telegram_message_id=2001,
            telegram_update_id=999003,
        ),
    )

    async with session_factory() as session:
        session.add(
            _message(
                user_id=user.id,
                telegram_chat_id=1002,
                telegram_message_id=2001,
                telegram_update_id=999004,
            )
        )

        with pytest.raises(IntegrityError):
            await session.commit()

    async with session_factory() as session:
        total = await session.scalar(
            select(func.count())
            .select_from(SavedMessage)
            .where(
                SavedMessage.telegram_chat_id == 1002,
                SavedMessage.telegram_message_id == 2001,
            )
        )

    assert int(total or 0) == 1


async def test_concurrent_duplicate_update_inserts_single_row(
    engine: AsyncEngine,
) -> None:
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    telegram_user_id = 7004
    telegram_update_id = 999005

    try:
        user = await _register_user(session_factory, telegram_user_id)

        first, second = await asyncio.gather(
            _insert_message(
                session_factory,
                _message(
                    user_id=user.id,
                    telegram_chat_id=1003,
                    telegram_message_id=3001,
                    telegram_update_id=telegram_update_id,
                ),
            ),
            _insert_message(
                session_factory,
                _message(
                    user_id=user.id,
                    telegram_chat_id=1003,
                    telegram_message_id=3002,
                    telegram_update_id=telegram_update_id,
                ),
            ),
            return_exceptions=True,
        )

        results = [first, second]
        successes = [result for result in results if isinstance(result, SavedMessage)]
        failures = [result for result in results if isinstance(result, BaseException)]

        assert len(successes) == 1
        assert len(failures) == 1
        assert isinstance(failures[0], IntegrityError)

        assert await _count_messages(
            session_factory,
            telegram_update_id=telegram_update_id,
        ) == 1
    finally:
        async with session_factory() as session:
            await session.execute(
                delete(SavedMessage).where(
                    SavedMessage.telegram_update_id == telegram_update_id
                )
            )
            await session.execute(
                delete(User).where(User.telegram_user_id == telegram_user_id)
            )
            await session.commit()


async def test_unknown_user_id_is_rejected(
    session_factory: SessionFactory,
) -> None:
    async with session_factory() as session:
        session.add(
            _message(
                user_id=uuid.uuid4(),
                telegram_chat_id=1004,
                telegram_message_id=4001,
                telegram_update_id=999006,
            )
        )

        with pytest.raises(IntegrityError):
            await session.commit()

    assert await _count_messages(session_factory, telegram_update_id=999006) == 0


async def test_deleting_user_cascades_messages(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 7005)

    await _insert_message(
        session_factory,
        _message(
            user_id=user.id,
            telegram_chat_id=1005,
            telegram_message_id=5001,
            telegram_update_id=999007,
        ),
    )

    async with session_factory() as session:
        stored_user = await session.get(User, user.id)

        assert stored_user is not None

        await session.delete(stored_user)
        await session.commit()

    assert await _count_messages(session_factory, telegram_update_id=999007) == 0


async def test_text_is_required(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 7006)

    async with session_factory() as session:
        session.add(
            _message(
                user_id=user.id,
                telegram_chat_id=1006,
                telegram_message_id=6001,
                telegram_update_id=999008,
                text=None,  # type: ignore[arg-type]
            )
        )

        with pytest.raises(IntegrityError):
            await session.commit()

    assert await _count_messages(session_factory, telegram_update_id=999008) == 0
