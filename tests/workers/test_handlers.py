import asyncio
import logging
import uuid
from collections.abc import AsyncIterator
from datetime import UTC, datetime
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import delete, select
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from memory_vault.models.outbox_event import OutboxEvent, OutboxEventStatus
from memory_vault.models.saved_message import SavedMessage
from memory_vault.models.user import User
from memory_vault.repositories.saved_messages import SavedMessageRepository
from memory_vault.schemas.messages import TelegramMessageCreate
from memory_vault.schemas.users import TelegramUserCreate
from memory_vault.services.outbox import OutboxService
from memory_vault.services.saved_messages import (
    MESSAGE_RECEIVED_EVENT_TYPE,
    SAVED_MESSAGE_AGGREGATE_TYPE,
    SavedMessageService,
)
from memory_vault.workers.handlers import create_outbox_handlers
from memory_vault.workers.message_received import create_message_received_handler
from memory_vault.workers.outbox_worker import OutboxWorker

SessionFactory = async_sessionmaker[AsyncSession]
HANDLER_LOGGER = "memory_vault.workers.message_received"


def _unique_telegram_id() -> int:
    return uuid.uuid4().int % (2**62)


class HandlerHarness:
    """Real-database helper for the outbox handler tests.

    The ``session_factory`` fixture binds every session to one connection, but
    the worker's heartbeat and the handler open sessions concurrently, so these
    tests use a ``NullPool`` engine instead.
    """

    def __init__(self, session_factory: SessionFactory) -> None:
        self.session_factory = session_factory
        self.event_ids: list[uuid.UUID] = []
        self.message_ids: list[uuid.UUID] = []
        self.user_ids: list[uuid.UUID] = []

    async def create_event(
        self,
        *,
        aggregate_id: uuid.UUID,
        payload: dict[str, Any],
    ) -> OutboxEvent:
        async with self.session_factory() as session:
            event = await OutboxService(session).create_event(
                event_type=MESSAGE_RECEIVED_EVENT_TYPE,
                aggregate_type=SAVED_MESSAGE_AGGREGATE_TYPE,
                aggregate_id=aggregate_id,
                payload=payload,
            )

        self.event_ids.append(event.id)

        return event

    async def save_message(self, *, text: str) -> SavedMessage:
        telegram_id = _unique_telegram_id()
        data = TelegramMessageCreate(
            telegram_update_id=telegram_id,
            telegram_chat_id=telegram_id,
            telegram_message_id=telegram_id,
            text=text,
            message_date=datetime(2026, 11, 27, 9, 0, tzinfo=UTC),
            user=TelegramUserCreate(
                telegram_user_id=_unique_telegram_id(),
                first_name="Alice",
            ),
        )

        async with self.session_factory() as session:
            message = await SavedMessageService(session).save_incoming_message(data)

        assert message is not None

        self.message_ids.append(message.id)
        self.user_ids.append(message.user_id)

        return message

    async def event_for(self, aggregate_id: uuid.UUID) -> OutboxEvent:
        async with self.session_factory() as session:
            event = await session.scalar(
                select(OutboxEvent).where(OutboxEvent.aggregate_id == aggregate_id)
            )

        assert event is not None

        return event

    async def get(self, event_id: uuid.UUID) -> OutboxEvent | None:
        async with self.session_factory() as session:
            return await session.get(OutboxEvent, event_id)

    async def cleanup(self) -> None:
        async with self.session_factory() as session:
            if self.event_ids:
                await session.execute(delete(OutboxEvent).where(OutboxEvent.id.in_(self.event_ids)))

            if self.message_ids:
                await session.execute(
                    delete(OutboxEvent).where(OutboxEvent.aggregate_id.in_(self.message_ids))
                )
                await session.execute(
                    delete(SavedMessage).where(SavedMessage.id.in_(self.message_ids))
                )

            if self.user_ids:
                await session.execute(delete(User).where(User.id.in_(self.user_ids)))

            await session.commit()


@pytest_asyncio.fixture
async def handlers(engine: AsyncEngine) -> AsyncIterator[HandlerHarness]:
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    harness = HandlerHarness(session_factory)

    try:
        yield harness
    finally:
        await harness.cleanup()


def _worker(
    harness: HandlerHarness,
    handlers: dict[str, Any],
    *,
    max_attempts: int = 1,
) -> OutboxWorker:
    return OutboxWorker(
        harness.session_factory,
        handlers=handlers,
        worker_id="handler-worker",
        batch_size=10,
        lease_duration_seconds=60,
        heartbeat_interval_seconds=0.05,
        max_attempts=max_attempts,
        poll_interval=1.0,
    )


async def test_registry_registers_message_received_handler(
    handlers: HandlerHarness,
) -> None:
    registry = create_outbox_handlers(handlers.session_factory)

    assert set(registry) == {MESSAGE_RECEIVED_EVENT_TYPE}
    assert callable(registry[MESSAGE_RECEIVED_EVENT_TYPE])


async def test_worker_processes_saved_message_and_logs_text(
    handlers: HandlerHarness,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=HANDLER_LOGGER)
    message = await handlers.save_message(text="remember the milk")
    event = await handlers.event_for(message.id)

    worker = _worker(handlers, create_outbox_handlers(handlers.session_factory))

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    stored = await handlers.get(event.id)

    assert processed >= 1
    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSED
    assert stored.processed_at is not None
    assert stored.last_error is None

    records = [record for record in caplog.records if record.name == HANDLER_LOGGER]

    assert len(records) == 1
    assert str(message.id) in records[0].getMessage()
    assert f"text={'remember the milk'!r}" in records[0].getMessage()


async def test_handler_invocation_is_idempotent(
    handlers: HandlerHarness,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.INFO, logger=HANDLER_LOGGER)
    message = await handlers.save_message(text="only once")
    event = await handlers.event_for(message.id)
    handler = create_message_received_handler(handlers.session_factory)

    await handler(event)
    await handler(event)

    records = [record for record in caplog.records if record.name == HANDLER_LOGGER]
    stored = await handlers.get(event.id)

    assert len(records) == 2
    assert stored is not None
    # The handler must not mutate the event; the worker owns completion.
    assert stored.status == OutboxEventStatus.PENDING


async def test_missing_saved_message_is_logged_and_completed(
    handlers: HandlerHarness,
    caplog: pytest.LogCaptureFixture,
) -> None:
    caplog.set_level(logging.WARNING, logger=HANDLER_LOGGER)
    message_id = uuid.uuid4()
    event = await handlers.create_event(
        aggregate_id=message_id,
        payload={"saved_message_id": str(message_id)},
    )

    worker = _worker(handlers, create_outbox_handlers(handlers.session_factory))

    await asyncio.wait_for(worker.run_once(), timeout=5)

    stored = await handlers.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSED
    assert stored.last_error is None
    assert any(
        record.levelno == logging.WARNING and str(message_id) in record.getMessage()
        for record in caplog.records
    )


@pytest.mark.parametrize(
    "payload",
    [
        pytest.param({}, id="missing"),
        pytest.param({"saved_message_id": 123}, id="non-string"),
        pytest.param({"saved_message_id": "not-a-uuid"}, id="invalid-uuid"),
    ],
)
async def test_malformed_payload_fails_event(
    handlers: HandlerHarness,
    payload: dict[str, Any],
) -> None:
    event = await handlers.create_event(
        aggregate_id=uuid.uuid4(),
        payload=payload,
    )

    worker = _worker(handlers, create_outbox_handlers(handlers.session_factory))

    await asyncio.wait_for(worker.run_once(), timeout=5)

    stored = await handlers.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.FAILED
    assert stored.last_error is not None
    assert "saved_message_id" in stored.last_error


async def test_payload_and_aggregate_mismatch_fails_event(
    handlers: HandlerHarness,
) -> None:
    message_id = uuid.uuid4()
    event = await handlers.create_event(
        aggregate_id=uuid.uuid4(),
        payload={"saved_message_id": str(message_id)},
    )

    worker = _worker(handlers, create_outbox_handlers(handlers.session_factory))

    await asyncio.wait_for(worker.run_once(), timeout=5)

    stored = await handlers.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.FAILED
    assert stored.last_error is not None
    assert "does not match" in stored.last_error


async def test_handler_propagates_cancellation(
    handlers: HandlerHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    message_id = uuid.uuid4()
    event = await handlers.create_event(
        aggregate_id=message_id,
        payload={"saved_message_id": str(message_id)},
    )

    async def cancelled(
        self: SavedMessageRepository,
        message_id: uuid.UUID,
    ) -> SavedMessage | None:
        raise asyncio.CancelledError

    monkeypatch.setattr(SavedMessageRepository, "get_by_id", cancelled)
    handler = create_message_received_handler(handlers.session_factory)

    with pytest.raises(asyncio.CancelledError):
        await handler(event)
