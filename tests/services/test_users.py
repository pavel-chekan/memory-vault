import asyncio
import uuid
from typing import Any

from sqlalchemy import delete, func, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from memory_vault.models.user import User
from memory_vault.schemas.users import TelegramUserCreate
from memory_vault.services.users import UserService

SessionFactory = async_sessionmaker[AsyncSession]


async def _register(
    session_factory: SessionFactory,
    **data: Any,
) -> User:
    async with session_factory() as session:
        return await UserService(session).register(TelegramUserCreate(**data))


async def _load_users(session_factory: SessionFactory) -> list[User]:
    async with session_factory() as session:
        result = await session.execute(select(User).order_by(User.telegram_user_id))

        return list(result.scalars().all())


async def test_register_new_user(
    session_factory: SessionFactory,
) -> None:
    user = await _register(
        session_factory,
        telegram_user_id=1001,
        username="alice",
        first_name="Alice",
        last_name="Smith",
        language_code="en",
    )

    assert isinstance(user.id, uuid.UUID)
    assert user.telegram_user_id == 1001
    assert user.username == "alice"
    assert user.first_name == "Alice"
    assert user.last_name == "Smith"
    assert user.language_code == "en"
    assert user.created_at is not None
    assert user.updated_at is not None

    users = await _load_users(session_factory)

    assert len(users) == 1
    assert users[0].id == user.id
    assert users[0].telegram_user_id == 1001
    assert users[0].username == "alice"
    assert users[0].first_name == "Alice"
    assert users[0].last_name == "Smith"
    assert users[0].language_code == "en"


async def test_register_existing_user_updates_data(
    session_factory: SessionFactory,
) -> None:
    original = await _register(
        session_factory,
        telegram_user_id=2002,
        username="bob",
        first_name="Bob",
        last_name="Old",
        language_code="en",
    )

    updated = await _register(
        session_factory,
        telegram_user_id=2002,
        username="robert",
        first_name="Robert",
        last_name="New",
        language_code="de",
    )

    assert updated.id == original.id
    assert updated.telegram_user_id == 2002
    assert updated.username == "robert"
    assert updated.first_name == "Robert"
    assert updated.last_name == "New"
    assert updated.language_code == "de"

    users = await _load_users(session_factory)

    assert len(users) == 1
    assert users[0].id == original.id
    assert users[0].telegram_user_id == 2002
    assert users[0].username == "robert"
    assert users[0].first_name == "Robert"
    assert users[0].last_name == "New"
    assert users[0].language_code == "de"


async def test_register_two_users_with_different_telegram_ids(
    session_factory: SessionFactory,
) -> None:
    first = await _register(
        session_factory,
        telegram_user_id=3003,
        username="carol",
        first_name="Carol",
        last_name=None,
        language_code="en",
    )

    second = await _register(
        session_factory,
        telegram_user_id=3004,
        username="dave",
        first_name="Dave",
        last_name="Jones",
        language_code="ru",
    )

    assert first.id != second.id
    assert first.telegram_user_id != second.telegram_user_id

    users = await _load_users(session_factory)

    assert len(users) == 2

    by_telegram_id = {user.telegram_user_id: user for user in users}

    assert set(by_telegram_id) == {3003, 3004}

    assert by_telegram_id[3003].id == first.id
    assert by_telegram_id[3003].username == "carol"
    assert by_telegram_id[3003].first_name == "Carol"
    assert by_telegram_id[3003].last_name is None
    assert by_telegram_id[3003].language_code == "en"

    assert by_telegram_id[3004].id == second.id
    assert by_telegram_id[3004].username == "dave"
    assert by_telegram_id[3004].first_name == "Dave"
    assert by_telegram_id[3004].last_name == "Jones"
    assert by_telegram_id[3004].language_code == "ru"


async def test_register_concurrently_creates_single_user(
    engine: AsyncEngine,
) -> None:
    telegram_user_id = 4004
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    try:
        first, second = await asyncio.gather(
            _register(
                session_factory,
                telegram_user_id=telegram_user_id,
                username="first",
                first_name="First",
                last_name="Request",
                language_code="en",
            ),
            _register(
                session_factory,
                telegram_user_id=telegram_user_id,
                username="second",
                first_name="Second",
                last_name="Request",
                language_code="de",
            ),
            return_exceptions=True,
        )

        assert not isinstance(first, BaseException), first
        assert not isinstance(second, BaseException), second

        assert first.id == second.id
        assert first.telegram_user_id == telegram_user_id
        assert second.telegram_user_id == telegram_user_id

        async with session_factory() as session:
            users = list(
                (
                    await session.execute(
                        select(User).where(User.telegram_user_id == telegram_user_id)
                    )
                )
                .scalars()
                .all()
            )

            total = await session.scalar(
                select(func.count())
                .select_from(User)
                .where(User.telegram_user_id == telegram_user_id)
            )

            assert total == 1
        assert total == 1
        assert len(users) == 1
        assert users[0].id == first.id
        assert (users[0].username, users[0].first_name) in {
            ("first", "First"),
            ("second", "Second"),
        }
    finally:
        async with session_factory() as session:
            await session.execute(delete(User).where(User.telegram_user_id == telegram_user_id))
            await session.commit()
