import asyncio
import hashlib
import re
import uuid
from datetime import UTC, datetime

import pytest
import sqlalchemy as sa
from sqlalchemy import delete, func, select
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from memory_vault.models.knowledge_item import (
    KnowledgeItem,
    KnowledgeItemStatus,
    KnowledgeSourceType,
)
from memory_vault.models.saved_message import SavedMessage
from memory_vault.models.user import User
from memory_vault.schemas.users import TelegramUserCreate
from memory_vault.services.users import UserService

SessionFactory = async_sessionmaker[AsyncSession]


def _knowledge_item(
    *,
    user_id: uuid.UUID,
    source_id: uuid.UUID,
    title: str | None = "knowledge title",
    content: str | None = "knowledge content",
    content_hash: str | None = None,
    status: KnowledgeItemStatus | None = None,
    source_type: KnowledgeSourceType | None = None,
) -> KnowledgeItem:
    return KnowledgeItem(
        user_id=user_id,
        source_id=source_id,
        title=title,
        content=content,
        content_hash=(
            content_hash
            if content_hash is not None
            else _hash(content if content is not None else "knowledge content")
        ),
        status=(status if status is not None else KnowledgeItemStatus.PENDING),
        source_type=(
            source_type if source_type is not None else KnowledgeSourceType.TELEGRAM_MESSAGE
        ),
    )


def _hash(content: str) -> str:
    return hashlib.sha256(content.encode("utf-8")).hexdigest()


def _source(
    *,
    user_id: uuid.UUID,
    telegram_update_id: int,
    telegram_chat_id: int,
    telegram_message_id: int,
    text: str = "source message",
    message_date: datetime | None = None,
) -> SavedMessage:
    return SavedMessage(
        user_id=user_id,
        telegram_chat_id=telegram_chat_id,
        telegram_message_id=telegram_message_id,
        telegram_update_id=telegram_update_id,
        text=text,
        message_date=message_date if message_date is not None else datetime.now(UTC),
    )


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


async def _insert_source(
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


async def _insert_knowledge_item(
    session_factory: SessionFactory,
    item: KnowledgeItem,
) -> KnowledgeItem:
    async with session_factory() as session:
        session.add(item)
        try:
            await session.commit()
        except IntegrityError:
            await session.rollback()
            raise
        await session.refresh(item)
    return item


async def _count_knowledge_items(
    session_factory: SessionFactory,
    *,
    source_id: uuid.UUID,
) -> int:
    async with session_factory() as session:
        total = await session.scalar(
            select(func.count())
            .select_from(KnowledgeItem)
            .where(KnowledgeItem.source_id == source_id)
        )
    return int(total or 0)


def _known_pairs() -> tuple[int, int, int]:
    return 8_000_001, 8_001_001, -100_800_001


async def test_all_fields_persist_and_round_trip(
    session_factory: SessionFactory,
) -> None:
    telegram_user_id, telegram_update_id, telegram_chat_id = _known_pairs()
    user = await _register_user(session_factory, telegram_user_id)
    message_date = datetime(2026, 11, 24, 10, 30, tzinfo=UTC)

    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=telegram_update_id,
            telegram_chat_id=telegram_chat_id,
            telegram_message_id=42,
            text="source message",
            message_date=message_date,
        ),
    )

    content = "normalize this text"
    item = await _insert_knowledge_item(
        session_factory,
        _knowledge_item(
            user_id=user.id,
            source_id=source.id,
            title="full knowledge item title",
            content=content,
            content_hash=_hash(content),
            status=KnowledgeItemStatus.PROCESSED,
            source_type=KnowledgeSourceType.TELEGRAM_MESSAGE,
        ),
    )

    async with session_factory() as session:
        stored = await session.get(KnowledgeItem, item.id)

    assert stored is not None
    assert isinstance(stored.id, uuid.UUID)
    assert stored.user_id == user.id
    assert stored.source_id == source.id
    assert stored.source_type == KnowledgeSourceType.TELEGRAM_MESSAGE
    assert stored.title == "full knowledge item title"
    assert stored.content == "normalize this text"
    assert stored.content_hash == _hash("normalize this text")
    assert stored.status == KnowledgeItemStatus.PROCESSED
    assert stored.created_at is not None
    assert stored.created_at.tzinfo is not None
    assert stored.updated_at is not None
    assert stored.updated_at.tzinfo is not None


async def test_defaults_applied_when_status_and_source_type_omitted(
    session_factory: SessionFactory,
) -> None:
    telegram_user_id, telegram_update_id, _ = _known_pairs()
    telegram_user_id = 8_000_002
    telegram_update_id = 8_001_002
    user = await _register_user(session_factory, telegram_user_id)

    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=telegram_update_id,
            telegram_chat_id=-100_800_002,
            telegram_message_id=42,
        ),
    )

    item = await _insert_knowledge_item(
        session_factory,
        KnowledgeItem(
            user_id=user.id,
            source_id=source.id,
            title="defaults title",
            content="defaults content",
            content_hash=_hash("defaults content"),
        ),
    )

    async with session_factory() as session:
        stored = await session.get(KnowledgeItem, item.id)

    assert stored is not None
    assert stored.status == KnowledgeItemStatus.PENDING
    assert stored.source_type == KnowledgeSourceType.TELEGRAM_MESSAGE


async def test_content_hash_round_trips_as_sha256_hexdigest(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_003)
    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=8_001_003,
            telegram_chat_id=-100_800_003,
            telegram_message_id=42,
        ),
    )

    content = "utf-8 content with unicode привет"
    item = await _insert_knowledge_item(
        session_factory,
        _knowledge_item(
            user_id=user.id,
            source_id=source.id,
            content=content,
            content_hash=_hash(content),
        ),
    )

    assert item.content_hash == hashlib.sha256(content.encode("utf-8")).hexdigest()
    assert len(item.content_hash) == 64
    assert re.fullmatch(r"[0-9a-f]{64}", item.content_hash) is not None


async def test_duplicate_source_pair_is_rejected(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_004)
    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=8_001_004,
            telegram_chat_id=-100_800_004,
            telegram_message_id=42,
        ),
    )

    await _insert_knowledge_item(
        session_factory,
        _knowledge_item(
            user_id=user.id,
            source_id=source.id,
            title="first title",
            content="first content",
            content_hash=_hash("first content"),
        ),
    )

    async with session_factory() as session:
        session.add(
            _knowledge_item(
                user_id=user.id,
                source_id=source.id,
                title="second title",
                content="second content",
                content_hash=_hash("second content"),
            )
        )

        with pytest.raises(IntegrityError):
            await session.commit()

    assert await _count_knowledge_items(session_factory, source_id=source.id) == 1


async def test_concurrent_duplicate_source_inserts_single_row(
    engine: AsyncEngine,
) -> None:
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    telegram_user_id = 8_000_005
    telegram_update_id = 8_001_005
    chat_id = -100_800_005
    user = None
    source = None
    source_id: uuid.UUID | None = None

    try:
        # Setup via the raw engine session, so the knowledge item inserts are
        # independent of the final cleanup.
        async with session_factory() as session:
            user = await UserService(session).register(
                TelegramUserCreate(
                    telegram_user_id=telegram_user_id,
                    first_name="Test",
                )
            )
            message = _source(
                user_id=user.id,
                telegram_update_id=telegram_update_id,
                telegram_chat_id=chat_id,
                telegram_message_id=42,
            )
            session.add(message)
            await session.commit()
            user = user
            source = message
            source_id = message.id

        first, second = await asyncio.gather(
            _insert_knowledge_item(
                session_factory,
                _knowledge_item(
                    user_id=user.id,
                    source_id=source_id,
                    title="first",
                    content="first content",
                    content_hash=_hash("first content"),
                ),
            ),
            _insert_knowledge_item(
                session_factory,
                _knowledge_item(
                    user_id=user.id,
                    source_id=source_id,
                    title="second",
                    content="second content",
                    content_hash=_hash("second content"),
                ),
            ),
            return_exceptions=True,
        )

        results = [first, second]
        successes = [result for result in results if isinstance(result, KnowledgeItem)]
        failures = [result for result in results if isinstance(result, BaseException)]

        assert len(successes) == 1
        assert len(failures) == 1
        assert isinstance(failures[0], IntegrityError)

        assert await _count_knowledge_items(session_factory, source_id=source_id) == 1
    finally:
        async with session_factory() as session:
            if source_id is not None:
                await session.execute(
                    delete(KnowledgeItem).where(KnowledgeItem.source_id == source_id)
                )
            if source is not None:
                await session.execute(
                    delete(SavedMessage).where(
                        SavedMessage.telegram_update_id == telegram_update_id
                    )
                )
            if user is not None:
                await session.execute(delete(User).where(User.telegram_user_id == telegram_user_id))
            await session.commit()


async def test_unique_constraint_covers_source_type_and_source_id(
    session_factory: SessionFactory,
) -> None:
    table = KnowledgeItem.__table__

    constraint = next(
        (c for c in table.constraints if c.name == "uq_knowledge_items_source_type_source_id"),
        None,
    )

    assert constraint is not None
    assert isinstance(constraint, sa.UniqueConstraint)

    columns = constraint.columns
    column_names = [column.name for column in columns]

    assert column_names == ["source_type", "source_id"]


async def test_unknown_source_id_is_rejected(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_007)

    async with session_factory() as session:
        session.add(
            _knowledge_item(
                user_id=user.id,
                source_id=uuid.uuid4(),
                title="unknown source",
                content="unknown source content",
                content_hash=_hash("unknown source content"),
            )
        )

        with pytest.raises(IntegrityError):
            await session.commit()


async def test_unknown_user_id_is_rejected(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_016)
    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=8_001_016,
            telegram_chat_id=-100_800_016,
            telegram_message_id=42,
        ),
    )

    async with session_factory() as session:
        session.add(
            _knowledge_item(
                user_id=uuid.uuid4(),
                source_id=source.id,
                title="unknown user",
                content="unknown user content",
                content_hash=_hash("unknown user content"),
            )
        )

        with pytest.raises(IntegrityError):
            await session.commit()

    assert await _count_knowledge_items(session_factory, source_id=source.id) == 0


async def test_deleting_saved_message_cascades_knowledge_items(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_008)
    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=8_001_008,
            telegram_chat_id=-100_800_008,
            telegram_message_id=42,
        ),
    )

    await _insert_knowledge_item(
        session_factory,
        _knowledge_item(
            user_id=user.id,
            source_id=source.id,
            title="cascade title",
            content="cascade content",
            content_hash=_hash("cascade content"),
        ),
    )

    async with session_factory() as session:
        stored_source = await session.get(SavedMessage, source.id)

        assert stored_source is not None

        await session.delete(stored_source)
        await session.commit()

    assert await _count_knowledge_items(session_factory, source_id=source.id) == 0


async def test_deleting_user_cascades_knowledge_items(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_009)
    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=8_001_009,
            telegram_chat_id=-100_800_009,
            telegram_message_id=42,
        ),
    )

    await _insert_knowledge_item(
        session_factory,
        _knowledge_item(
            user_id=user.id,
            source_id=source.id,
            title="cascade user title",
            content="cascade user content",
            content_hash=_hash("cascade user content"),
        ),
    )

    async with session_factory() as session:
        stored_user = await session.get(User, user.id)

        assert stored_user is not None

        await session.delete(stored_user)
        await session.commit()

    async with session_factory() as session:
        knowledge_count = await session.scalar(
            select(func.count())
            .select_from(KnowledgeItem)
            .where(KnowledgeItem.source_id == source.id)
        )
        message_count = await session.scalar(
            select(func.count())
            .select_from(SavedMessage)
            .where(SavedMessage.telegram_update_id == 8_001_009)
        )

    assert int(knowledge_count or 0) == 0
    assert int(message_count or 0) == 0


async def test_title_none_is_accepted(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_010)
    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=8_001_010,
            telegram_chat_id=-100_800_010,
            telegram_message_id=42,
        ),
    )

    item = await _insert_knowledge_item(
        session_factory,
        _knowledge_item(
            user_id=user.id,
            source_id=source.id,
            title=None,
            content="title none content",
            content_hash=_hash("title none content"),
        ),
    )

    async with session_factory() as session:
        stored = await session.get(KnowledgeItem, item.id)

    assert stored is not None
    assert stored.title is None


async def test_content_is_required(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_011)
    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=8_001_011,
            telegram_chat_id=-100_800_011,
            telegram_message_id=42,
        ),
    )

    async with session_factory() as session:
        session.add(
            KnowledgeItem(
                user_id=user.id,
                source_id=source.id,
                title="content missing title",
                content=None,  # type: ignore[arg-type]
                content_hash=_hash("content missing content"),
            )
        )

        with pytest.raises(IntegrityError):
            await session.commit()


async def test_unknown_status_is_rejected(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_012)
    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=8_001_012,
            telegram_chat_id=-100_800_012,
            telegram_message_id=42,
        ),
    )

    async with session_factory() as session:
        with pytest.raises(IntegrityError):
            await session.execute(
                sa.text(
                    "INSERT INTO knowledge_items (id, user_id, source_type, "
                    "source_id, content, content_hash, status) "
                    "VALUES ('9a8e4f4d-4d4e-4c8f-8f0c-f8e1a9d4e2f0', "
                    ":user_id, 'TELEGRAM_MESSAGE', :source_id, "
                    ":content, :content_hash, 'BOGUS')"
                ),
                {
                    "user_id": str(user.id),
                    "source_id": str(source.id),
                    "content": "bogus status content",
                    "content_hash": _hash("bogus status content"),
                },
            )

        await session.rollback()


async def test_unknown_source_type_is_rejected(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_013)
    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=8_001_013,
            telegram_chat_id=-100_800_013,
            telegram_message_id=42,
        ),
    )

    async with session_factory() as session:
        with pytest.raises(IntegrityError):
            await session.execute(
                sa.text(
                    "INSERT INTO knowledge_items (id, user_id, source_type, "
                    "source_id, content, content_hash, status) "
                    "VALUES ('9a8e4f4d-4d4e-4c8f-8f0c-f8e1a9d4e2f1', "
                    ":user_id, 'BOGUS', :source_id, "
                    ":content, :content_hash, 'PENDING')"
                ),
                {
                    "user_id": str(user.id),
                    "source_id": str(source.id),
                    "content": "bogus source type content",
                    "content_hash": _hash("bogus source type content"),
                },
            )

        await session.rollback()


async def test_server_defaults_apply_on_raw_insert(
    session_factory: SessionFactory,
) -> None:
    user = await _register_user(session_factory, 8_000_017)
    source = await _insert_source(
        session_factory,
        _source(
            user_id=user.id,
            telegram_update_id=8_001_017,
            telegram_chat_id=-100_800_017,
            telegram_message_id=42,
        ),
    )
    item_id = uuid.uuid4()

    async with session_factory() as session:
        await session.execute(
            sa.text(
                "INSERT INTO knowledge_items (id, user_id, source_id, content, "
                "content_hash) VALUES (:id, :user_id, :source_id, :content, "
                ":content_hash)"
            ),
            {
                "id": str(item_id),
                "user_id": str(user.id),
                "source_id": str(source.id),
                "content": "server defaults content",
                "content_hash": _hash("server defaults content"),
            },
        )

        result = await session.execute(
            sa.text("SELECT status, source_type FROM knowledge_items WHERE id = :id"),
            {"id": str(item_id)},
        )
        row = result.first()

        assert row is not None
        assert row.status == "PENDING"
        assert row.source_type == "TELEGRAM_MESSAGE"

        await session.rollback()
