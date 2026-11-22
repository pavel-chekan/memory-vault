import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import delete, func, update
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from memory_vault.models.outbox_event import OutboxEvent, OutboxEventStatus
from memory_vault.services.outbox import OutboxService
from memory_vault.workers.outbox_worker import OutboxWorker

SessionFactory = async_sessionmaker[AsyncSession]


class OutboxHarness:
    """Small helper around the real database used by worker tests.

    Worker tests cannot use the ``session_factory`` fixture: the heartbeat and
    the handler open sessions concurrently, and that fixture binds every session
    to one connection. A NullPool engine gives each session its own connection.
    """

    def __init__(self, session_factory: SessionFactory) -> None:
        self.session_factory = session_factory
        self.event_ids: list[uuid.UUID] = []

    async def create_event(self, event_type: str = "test.event") -> OutboxEvent:
        async with self.session_factory() as session:
            event = await OutboxService(session).create_event(
                event_type=event_type,
                aggregate_type="test",
                aggregate_id=uuid.uuid4(),
                payload={},
            )

        self.event_ids.append(event.id)

        return event

    async def claim(
        self,
        worker_id: str,
        *,
        batch_size: int = 1,
    ) -> list[OutboxEvent]:
        async with self.session_factory() as session:
            return await OutboxService(session).claim_batch(
                worker_id=worker_id,
                batch_size=batch_size,
                lease_duration_seconds=60,
            )

    async def get(self, event_id: uuid.UUID) -> OutboxEvent | None:
        async with self.session_factory() as session:
            return await session.get(OutboxEvent, event_id)

    async def expire_lease(self, event_id: uuid.UUID) -> None:
        async with self.session_factory() as session:
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id == event_id)
                .values(
                    lease_expires_at=func.now() - func.make_interval(0, 0, 0, 0, 0, 0, 1),
                )
            )
            await session.commit()

    async def cleanup(self) -> None:
        if not self.event_ids:
            return

        async with self.session_factory() as session:
            await session.execute(delete(OutboxEvent).where(OutboxEvent.id.in_(self.event_ids)))
            await session.commit()


@pytest_asyncio.fixture
async def outbox(engine: AsyncEngine) -> AsyncIterator[OutboxHarness]:
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    harness = OutboxHarness(session_factory)

    try:
        yield harness
    finally:
        await harness.cleanup()


def _worker(
    outbox: OutboxHarness,
    handler: Any,
    *,
    worker_id: str = "worker-a",
    heartbeat_interval_seconds: float = 0.05,
) -> OutboxWorker:
    return OutboxWorker(
        outbox.session_factory,
        handlers={"test.event": handler},
        worker_id=worker_id,
        lease_duration_seconds=60,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
    )


async def test_heartbeat_renews_lease_while_handler_runs(
    outbox: OutboxHarness,
) -> None:
    event = await outbox.create_event()
    observed_expiries: list[datetime | None] = []

    async def handler(_: OutboxEvent) -> None:
        initial = await outbox.get(event.id)
        observed_expiries.append(initial.lease_expires_at if initial else None)

        await asyncio.sleep(0.15)

        current = await outbox.get(event.id)
        observed_expiries.append(current.lease_expires_at if current else None)

        await asyncio.sleep(0.1)

    worker = _worker(outbox, handler)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1
    assert len(observed_expiries) == 2
    assert observed_expiries[0] is not None
    assert observed_expiries[1] is not None
    assert observed_expiries[1] > observed_expiries[0]

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSED
    assert stored.lease_token is None
    assert stored.lease_expires_at is None


async def test_heartbeat_stops_after_handler_completes(
    outbox: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await outbox.create_event()

    renewals = 0
    original_renew = OutboxService.renew_lease

    async def counting_renew(self: OutboxService, *args: Any, **kwargs: Any) -> bool:
        nonlocal renewals
        renewals += 1
        return await original_renew(self, *args, **kwargs)

    monkeypatch.setattr(OutboxService, "renew_lease", counting_renew)

    async def handler(_: OutboxEvent) -> None:
        await asyncio.sleep(0.16)

    worker = _worker(outbox, handler)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1
    assert renewals >= 1

    renewals_at_completion = renewals

    # Wait well past a heartbeat interval: a leaked heartbeat would renew again.
    await asyncio.sleep(0.2)

    assert renewals == renewals_at_completion


async def test_no_renewal_when_handler_finishes_before_first_heartbeat(
    outbox: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    await outbox.create_event()

    renewals = 0
    original_renew = OutboxService.renew_lease

    async def counting_renew(self: OutboxService, *args: Any, **kwargs: Any) -> bool:
        nonlocal renewals
        renewals += 1
        return await original_renew(self, *args, **kwargs)

    monkeypatch.setattr(OutboxService, "renew_lease", counting_renew)

    async def handler(_: OutboxEvent) -> None:
        return None

    worker = _worker(outbox, handler)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1
    assert renewals == 0


async def test_lease_loss_cancels_handler(
    outbox: OutboxHarness,
) -> None:
    event = await outbox.create_event()

    handler_started = asyncio.Event()
    handler_cancelled = asyncio.Event()

    async def handler(_: OutboxEvent) -> None:
        handler_started.set()

        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise

    worker = _worker(outbox, handler)
    run_task = asyncio.create_task(worker.run_once())

    await asyncio.wait_for(handler_started.wait(), timeout=5)

    # The lease expires and another worker reclaims the event with a fresh token.
    await outbox.expire_lease(event.id)
    reclaimed = await outbox.claim("worker-b")

    assert len(reclaimed) == 1

    processed = await asyncio.wait_for(run_task, timeout=5)

    assert processed == 1
    assert handler_cancelled.is_set()

    # Worker A must not complete or fail an event it no longer owns.
    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-b"
    assert stored.lease_token == reclaimed[0].lease_token
    assert stored.attempt_count == 2
    assert stored.last_error is None


async def test_heartbeat_error_fails_event_and_cancels_handler(
    outbox: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = await outbox.create_event()

    handler_started = asyncio.Event()
    handler_cancelled = asyncio.Event()

    async def handler(_: OutboxEvent) -> None:
        handler_started.set()

        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise

    async def broken_renew(self: OutboxService, *args: Any, **kwargs: Any) -> bool:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(OutboxService, "renew_lease", broken_renew)

    worker = _worker(outbox, handler)
    run_task = asyncio.create_task(worker.run_once())

    await asyncio.wait_for(handler_started.wait(), timeout=5)

    processed = await asyncio.wait_for(run_task, timeout=5)

    assert processed == 1
    assert handler_cancelled.is_set()

    # A renewal error is reported to the main task, which releases the lease and
    # schedules the event for a retry instead of silently continuing.
    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PENDING
    assert stored.locked_by is None
    assert stored.lease_token is None
    assert stored.lease_expires_at is None
    assert stored.attempt_count == 1
    assert stored.last_error is not None
    assert "renew" in stored.last_error.lower()


@pytest.mark.parametrize("interval", [0.0, -1.0, 60.0])
async def test_invalid_heartbeat_interval_is_rejected(
    engine: AsyncEngine,
    interval: float,
) -> None:
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    with pytest.raises(ValueError):
        OutboxWorker(
            session_factory,
            handlers={},
            lease_duration_seconds=60,
            heartbeat_interval_seconds=interval,
        )


async def test_default_heartbeat_interval_is_one_third_of_lease(
    engine: AsyncEngine,
) -> None:
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )

    worker = OutboxWorker(
        session_factory,
        handlers={},
        lease_duration_seconds=60,
    )

    assert worker.heartbeat_interval_seconds == 20.0
