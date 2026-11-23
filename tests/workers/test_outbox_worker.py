import asyncio
import uuid
from collections.abc import AsyncIterator
from datetime import datetime
from typing import Any

import pytest
import pytest_asyncio
from sqlalchemy import delete, func, update
from sqlalchemy.exc import OperationalError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from memory_vault.models.outbox_event import OutboxEvent, OutboxEventStatus
from memory_vault.services.outbox import DEFAULT_MAX_ATTEMPTS, OutboxService
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

    async def make_available(self, event_id: uuid.UUID) -> None:
        """Skip the retry backoff so the next claim happens immediately."""
        async with self.session_factory() as session:
            await session.execute(
                update(OutboxEvent)
                .where(OutboxEvent.id == event_id)
                .values(available_at=func.now())
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
    batch_size: int = 1,
    handler_timeout_seconds: float | None = None,
    max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    poll_interval: float = 1.0,
) -> OutboxWorker:
    return OutboxWorker(
        outbox.session_factory,
        handlers={"test.event": handler},
        worker_id=worker_id,
        batch_size=batch_size,
        lease_duration_seconds=60,
        heartbeat_interval_seconds=heartbeat_interval_seconds,
        handler_timeout_seconds=handler_timeout_seconds,
        max_attempts=max_attempts,
        poll_interval=poll_interval,
    )


def _database_unavailable(
    monkeypatch: pytest.MonkeyPatch,
    *,
    renew_started: asyncio.Event | None = None,
) -> dict[str, int]:
    """Make every service-level write fail as an unreachable database would.

    ``renew_lease`` is replaced with a shim that marks ``renew_started`` before
    raising, so the caller can synchronize on the heartbeat attempt. ``complete``
    and ``fail`` are replaced with call-through-counting shims that raise, so the
    caller can also assert which writes the worker attempted.

    Returns a mapping with the attempted ``complete``/``fail`` call counts.
    """
    calls = {"complete": 0, "fail": 0}

    async def broken_renew(self: OutboxService, *args: Any, **kwargs: Any) -> bool:
        if renew_started is not None:
            renew_started.set()

        raise OperationalError(
            "UPDATE outbox_events SET lease_expires_at = now() + interval '60 seconds'",
            {},
            Exception("connection to server was lost"),
        )

    async def broken_complete(self: OutboxService, *args: Any, **kwargs: Any) -> Any:
        calls["complete"] += 1

        raise OperationalError(
            "UPDATE outbox_events SET status = 'PROCESSED'",
            {},
            Exception("connection to server was lost"),
        )

    async def broken_fail(self: OutboxService, *args: Any, **kwargs: Any) -> Any:
        calls["fail"] += 1

        raise OperationalError(
            "UPDATE outbox_events SET status = 'PENDING'",
            {},
            Exception("connection to server was lost"),
        )

    monkeypatch.setattr(OutboxService, "renew_lease", broken_renew)
    monkeypatch.setattr(OutboxService, "complete", broken_complete)
    monkeypatch.setattr(OutboxService, "fail", broken_fail)

    return calls


OUTBOX_TASK_PREFIXES = ("outbox-event-", "outbox-heartbeat-", "outbox-handler-")


def _outbox_task_names() -> list[str]:
    return [
        task.get_name()
        for task in asyncio.all_tasks()
        if task.get_name().startswith(OUTBOX_TASK_PREFIXES)
    ]


async def test_heartbeat_renews_lease_while_handler_runs(
    outbox: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = await outbox.create_event()
    observed_expiries: list[datetime | None] = []
    renewed = asyncio.Event()

    original_renew = OutboxService.renew_lease

    async def counting_renew(self: OutboxService, *args: Any, **kwargs: Any) -> bool:
        result = await original_renew(self, *args, **kwargs)

        if result:
            renewed.set()

        return result

    monkeypatch.setattr(OutboxService, "renew_lease", counting_renew)

    async def handler(_: OutboxEvent) -> None:
        initial = await outbox.get(event.id)
        observed_expiries.append(initial.lease_expires_at if initial else None)

        # Deterministic sync: wait for a real renewal instead of sleeping
        # a fixed duration that only happens to cover one heartbeat.
        await asyncio.wait_for(renewed.wait(), timeout=5)

        current = await outbox.get(event.id)
        observed_expiries.append(current.lease_expires_at if current else None)

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

    complete_calls = 0
    original_complete = OutboxService.complete

    async def counting_complete(
        self: OutboxService,
        *args: Any,
        **kwargs: Any,
    ) -> OutboxEvent | None:
        nonlocal complete_calls

        complete_calls += 1

        return await original_complete(self, *args, **kwargs)

    monkeypatch.setattr(OutboxService, "complete", counting_complete)

    # Block the heartbeat in renew_lease until worker-b has taken over the
    # event. This makes the renewal-observation of the lost ownership a fixed
    # precondition of the test instead of a race with the reclaim.
    reclaimed_by_worker_b = asyncio.Event()
    original_renew = OutboxService.renew_lease

    async def renew_after_reclaim(
        self: OutboxService,
        *args: Any,
        **kwargs: Any,
    ) -> bool:
        await reclaimed_by_worker_b.wait()

        return await original_renew(self, *args, **kwargs)

    monkeypatch.setattr(OutboxService, "renew_lease", renew_after_reclaim)

    worker = _worker(outbox, handler)
    run_task = asyncio.create_task(worker.run_once())

    await asyncio.wait_for(handler_started.wait(), timeout=5)

    # The lease expires and another worker reclaims the event with a fresh token.
    await outbox.expire_lease(event.id)
    reclaimed = await outbox.claim("worker-b")

    # Release the blocked heartbeat; it must observe the lost ownership and
    # cancel the handler.
    reclaimed_by_worker_b.set()

    assert len(reclaimed) == 1

    processed = await asyncio.wait_for(run_task, timeout=5)

    assert processed == 1
    assert handler_cancelled.is_set()

    # Worker A must not complete or fail an event it no longer owns.
    assert complete_calls == 0

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
    renew_started = asyncio.Event()

    async def handler(_: OutboxEvent) -> None:
        handler_started.set()

        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise

    async def broken_renew(self: OutboxService, *args: Any, **kwargs: Any) -> bool:
        renew_started.set()

        raise OperationalError(
            "UPDATE outbox_events SET lease_expires_at = now() + interval '60 seconds'",
            {},
            Exception("connection to server was lost"),
        )

    monkeypatch.setattr(OutboxService, "renew_lease", broken_renew)

    complete_calls = 0
    original_complete = OutboxService.complete

    async def counting_complete(
        self: OutboxService,
        *args: Any,
        **kwargs: Any,
    ) -> OutboxEvent | None:
        nonlocal complete_calls

        complete_calls += 1

        return await original_complete(self, *args, **kwargs)

    monkeypatch.setattr(OutboxService, "complete", counting_complete)

    fail_calls = 0
    original_fail = OutboxService.fail

    async def counting_fail(
        self: OutboxService,
        *args: Any,
        **kwargs: Any,
    ) -> OutboxEvent | None:
        nonlocal fail_calls

        fail_calls += 1

        return await original_fail(self, *args, **kwargs)

    monkeypatch.setattr(OutboxService, "fail", counting_fail)

    # max_attempts=3 keeps this test on the retryable fail() branch instead of
    # the terminal mark_failed() branch.
    worker = _worker(outbox, handler, max_attempts=3)
    run_task = asyncio.create_task(worker.run_once())

    await asyncio.wait_for(handler_started.wait(), timeout=5)
    await asyncio.wait_for(renew_started.wait(), timeout=5)

    processed = await asyncio.wait_for(run_task, timeout=5)

    assert processed == 1
    assert handler_cancelled.is_set()

    # A heartbeat failure cancels the handler and must reach the retry path:
    # complete() is never called and fail() is called exactly once.
    assert complete_calls == 0
    assert fail_calls == 1

    # A heartbeat failure cancels the handler. The worker then attempts to
    # release the lease and schedule the event for a retry.
    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PENDING
    assert stored.locked_by is None
    assert stored.lease_token is None
    assert stored.lease_expires_at is None
    assert stored.attempt_count == 1
    assert stored.last_error is not None
    assert "renew" in stored.last_error.lower()


async def test_heartbeat_failure_with_database_unavailable_leaves_event_processing(
    outbox: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = await outbox.create_event()

    handler_started = asyncio.Event()
    handler_cancelled = asyncio.Event()
    renew_started = asyncio.Event()

    async def handler(_: OutboxEvent) -> None:
        handler_started.set()

        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise

    calls = _database_unavailable(monkeypatch, renew_started=renew_started)

    # max_attempts=3 keeps this test on the retryable fail() branch. The
    # failure cannot be persisted because the database is unreachable, so the
    # worker must swallow it and leave the event PROCESSING to expire.
    worker = _worker(outbox, handler, max_attempts=3)
    run_task = asyncio.create_task(worker.run_once())

    await asyncio.wait_for(handler_started.wait(), timeout=5)

    # The claim already committed before the handler started, and the broken
    # renew writes nothing, so the lease is expected to be exactly as claimed.
    original_lease = await outbox.get(event.id)

    assert original_lease is not None
    assert original_lease.lease_token is not None
    assert original_lease.lease_expires_at is not None

    await asyncio.wait_for(renew_started.wait(), timeout=5)

    processed = await asyncio.wait_for(run_task, timeout=5)

    assert processed == 1
    assert handler_cancelled.is_set()
    assert calls["complete"] == 0
    assert calls["fail"] == 1
    assert _outbox_task_names() == []

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-a"
    assert stored.lease_token == original_lease.lease_token
    assert stored.lease_expires_at == original_lease.lease_expires_at
    assert stored.attempt_count == 1
    assert stored.last_error is None

    # The lease is not released by the failure: it simply expires, after which
    # another worker can reclaim the event.
    await outbox.expire_lease(event.id)
    reclaimed = await outbox.claim("worker-b")

    assert len(reclaimed) == 1
    assert reclaimed[0].id == event.id
    assert reclaimed[0].attempt_count == 2


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


async def test_handler_timeout_fails_event(
    outbox: OutboxHarness,
) -> None:
    event = await outbox.create_event()
    handler_cancelled = asyncio.Event()

    async def handler(_: OutboxEvent) -> None:
        try:
            await asyncio.sleep(10)
        except asyncio.CancelledError:
            handler_cancelled.set()
            raise

    worker = _worker(outbox, handler, handler_timeout_seconds=0.1)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1
    assert handler_cancelled.is_set()

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PENDING
    assert stored.lease_token is None
    assert stored.attempt_count == 1
    assert stored.last_error is not None
    assert "timed out" in stored.last_error.lower()


async def test_handler_raised_timeout_error_is_not_reported_as_worker_timeout(
    outbox: OutboxHarness,
) -> None:
    event = await outbox.create_event()

    async def handler(_: OutboxEvent) -> None:
        raise TimeoutError("upstream connect timeout")

    # No worker timeout is configured, so the handler's own TimeoutError must
    # be reported verbatim instead of as the worker's handler timeout.
    worker = _worker(outbox, handler, handler_timeout_seconds=None)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PENDING
    assert stored.last_error == "upstream connect timeout"


async def test_handler_timeout_error_is_distinguished_from_worker_timeout(
    outbox: OutboxHarness,
) -> None:
    event = await outbox.create_event()

    async def handler(_: OutboxEvent) -> None:
        # Fails long before the worker's timeout expires.
        raise TimeoutError("upstream connect timeout")

    worker = _worker(outbox, handler, handler_timeout_seconds=5)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PENDING
    assert stored.last_error == "upstream connect timeout"
    assert "timed out" not in stored.last_error.lower()


async def test_stop_during_long_handler_is_graceful(
    outbox: OutboxHarness,
) -> None:
    event = await outbox.create_event()
    handler_started = asyncio.Event()

    async def handler(_: OutboxEvent) -> None:
        handler_started.set()
        await asyncio.sleep(0.2)

    worker = _worker(outbox, handler, poll_interval=0.05)
    run_task = asyncio.create_task(worker.run())

    await asyncio.wait_for(handler_started.wait(), timeout=5)

    worker.stop()
    await asyncio.wait_for(run_task, timeout=5)

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSED
    assert _outbox_task_names() == []


async def test_external_cancel_cancels_handler_and_heartbeat(
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

    complete_calls = 0
    original_complete = OutboxService.complete

    async def counting_complete(
        self: OutboxService,
        *args: Any,
        **kwargs: Any,
    ) -> OutboxEvent | None:
        nonlocal complete_calls

        complete_calls += 1

        return await original_complete(self, *args, **kwargs)

    monkeypatch.setattr(OutboxService, "complete", counting_complete)

    fail_calls = 0
    original_fail = OutboxService.fail

    async def counting_fail(
        self: OutboxService,
        *args: Any,
        **kwargs: Any,
    ) -> OutboxEvent | None:
        nonlocal fail_calls

        fail_calls += 1

        return await original_fail(self, *args, **kwargs)

    monkeypatch.setattr(OutboxService, "fail", counting_fail)

    # Signal that the heartbeat task has reached its first iteration, so the
    # cancel below is guaranteed to hit a live heartbeat, not a not-yet-started one.
    heartbeat_started = asyncio.Event()
    original_heartbeat = OutboxWorker._heartbeat

    async def signaling_heartbeat(
        self: OutboxWorker,
        *args: Any,
        **kwargs: Any,
    ) -> None:
        heartbeat_started.set()

        await original_heartbeat(self, *args, **kwargs)

    monkeypatch.setattr(OutboxWorker, "_heartbeat", signaling_heartbeat)

    # heartbeat_interval_seconds=30 (>= test duration) guarantees that no
    # legitimate renewal can change the lease, so any change is a bug.
    worker = _worker(outbox, handler, heartbeat_interval_seconds=30)
    run_task = asyncio.create_task(worker.run_once())

    await asyncio.wait_for(handler_started.wait(), timeout=5)
    await asyncio.wait_for(heartbeat_started.wait(), timeout=5)

    lease_before = await outbox.get(event.id)

    assert lease_before is not None

    run_task.cancel()

    with pytest.raises(asyncio.CancelledError):
        await run_task

    assert handler_cancelled.is_set()
    assert _outbox_task_names() == []

    # A hard cancel propagates out of run_once: it is not turned into a
    # failure write, so complete()/fail() must never be called.
    assert complete_calls == 0
    assert fail_calls == 0

    # The lease is neither released nor renewed: it is left untouched to expire.
    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == lease_before.locked_by == "worker-a"
    assert stored.lease_token == lease_before.lease_token
    assert stored.lease_expires_at == lease_before.lease_expires_at

    # A hard cancel does not release the lease; it simply expires, after which
    # another worker can reclaim the event.
    await outbox.expire_lease(event.id)
    reclaimed = await outbox.claim("worker-b")

    assert len(reclaimed) == 1
    assert reclaimed[0].id == event.id
    assert reclaimed[0].attempt_count == 2


async def test_no_orphan_tasks_after_run_once(
    outbox: OutboxHarness,
) -> None:
    await outbox.create_event()

    async def handler(_: OutboxEvent) -> None:
        await asyncio.sleep(0.1)

    worker = _worker(outbox, handler)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1
    assert _outbox_task_names() == []


async def test_batch_is_processed_concurrently(
    outbox: OutboxHarness,
) -> None:
    for _ in range(3):
        await outbox.create_event()

    running = 0
    max_running = 0
    started = 0
    all_started = asyncio.Event()

    async def handler(_: OutboxEvent) -> None:
        nonlocal running, max_running, started

        running += 1
        started += 1
        max_running = max(max_running, running)

        if started == 3:
            all_started.set()

        try:
            # Deadlocks (until the timeout) if the batch is processed sequentially.
            await asyncio.wait_for(all_started.wait(), timeout=2)
        finally:
            running -= 1

    worker = _worker(outbox, handler, batch_size=3)

    processed = await asyncio.wait_for(worker.run_once(), timeout=10)

    assert processed == 3
    assert max_running == 3


async def test_stop_interrupts_poll_sleep(
    outbox: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def no_events(self: OutboxWorker) -> list[OutboxEvent]:
        return []

    monkeypatch.setattr(OutboxWorker, "_claim_batch", no_events)

    worker = OutboxWorker(
        outbox.session_factory,
        handlers={},
        worker_id="worker-a",
        lease_duration_seconds=60,
        heartbeat_interval_seconds=0.05,
        poll_interval=30.0,
    )
    run_task = asyncio.create_task(worker.run())

    # Let the loop reach the poll wait before stopping it.
    await asyncio.sleep(0.1)

    worker.stop()
    await asyncio.wait_for(run_task, timeout=2)


async def test_claim_error_propagates_without_task_leak(
    outbox: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def broken_claim(self: OutboxService, *args: Any, **kwargs: Any) -> list[OutboxEvent]:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(OutboxService, "claim_batch", broken_claim)

    async def handler(_: OutboxEvent) -> None:
        return None

    worker = _worker(outbox, handler)

    with pytest.raises(RuntimeError, match="database unavailable"):
        await worker.run_once()

    assert _outbox_task_names() == []


async def test_complete_without_valid_lease_is_not_raised(
    outbox: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = await outbox.create_event()

    async def lost_lease(self: OutboxService, *args: Any, **kwargs: Any) -> OutboxEvent | None:
        return None

    monkeypatch.setattr(OutboxService, "complete", lost_lease)

    async def handler(_: OutboxEvent) -> None:
        return None

    worker = _worker(outbox, handler)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING


async def test_fail_without_valid_lease_is_not_raised(
    outbox: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = await outbox.create_event()

    async def lost_lease(self: OutboxService, *args: Any, **kwargs: Any) -> OutboxEvent | None:
        return None

    monkeypatch.setattr(OutboxService, "handle_failure", lost_lease)

    async def handler(_: OutboxEvent) -> None:
        raise RuntimeError("handler boom")

    worker = _worker(outbox, handler)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING


async def test_complete_database_error_does_not_crash_worker(
    outbox: OutboxHarness,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    event = await outbox.create_event()

    async def broken_complete(self: OutboxService, *args: Any, **kwargs: Any) -> OutboxEvent | None:
        raise RuntimeError("database unavailable")

    monkeypatch.setattr(OutboxService, "complete", broken_complete)

    async def handler(_: OutboxEvent) -> None:
        return None

    worker = _worker(outbox, handler)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING


async def test_self_cancelled_handler_is_failed_not_completed(
    outbox: OutboxHarness,
) -> None:
    event = await outbox.create_event()

    async def handler(_: OutboxEvent) -> None:
        raise asyncio.CancelledError("handler self-cancelled")

    worker = _worker(outbox, handler)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PENDING
    assert stored.lease_token is None
    assert stored.attempt_count == 1
    assert stored.last_error is not None
    assert "cancelled" in stored.last_error.lower()


async def test_claim_works_with_expire_on_commit_factory(
    outbox: OutboxHarness,
    engine: AsyncEngine,
) -> None:
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=True,
    )
    event = await outbox.create_event()

    async def handler(_: OutboxEvent) -> None:
        return None

    worker = OutboxWorker(
        session_factory,
        handlers={"test.event": handler},
        worker_id="worker-a",
        lease_duration_seconds=60,
        heartbeat_interval_seconds=0.05,
    )

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSED


async def test_successful_processing_transitions_pending_to_processing_to_processed(
    outbox: OutboxHarness,
) -> None:
    event = await outbox.create_event()
    observed_statuses: list[OutboxEventStatus] = []
    handler_started = asyncio.Event()
    handler_release = asyncio.Event()

    async def handler(_: OutboxEvent) -> None:
        current = await outbox.get(event.id)

        assert current is not None
        observed_statuses.append(current.status)

        handler_started.set()
        await handler_release.wait()

    worker = _worker(outbox, handler)
    run_task = asyncio.create_task(worker.run_once())

    await asyncio.wait_for(handler_started.wait(), timeout=5)

    in_flight = await outbox.get(event.id)

    assert in_flight is not None
    assert in_flight.status == OutboxEventStatus.PROCESSING

    handler_release.set()
    processed = await asyncio.wait_for(run_task, timeout=5)

    assert processed == 1
    assert observed_statuses == [OutboxEventStatus.PROCESSING]

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSED
    assert stored.processed_at is not None
    assert stored.locked_by is None
    assert stored.lease_token is None
    assert stored.lease_expires_at is None


async def test_handler_error_returns_event_to_pending(
    outbox: OutboxHarness,
) -> None:
    event = await outbox.create_event()

    async def handler(_: OutboxEvent) -> None:
        raise RuntimeError("handler boom")

    worker = _worker(outbox, handler)

    processed = await asyncio.wait_for(worker.run_once(), timeout=5)

    assert processed == 1

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PENDING
    assert stored.attempt_count == 1
    assert stored.locked_by is None
    assert stored.lease_token is None
    assert stored.lease_expires_at is None
    assert stored.last_error == "handler boom"


async def test_event_becomes_failed_after_max_attempts(
    outbox: OutboxHarness,
) -> None:
    event = await outbox.create_event()

    async def handler(_: OutboxEvent) -> None:
        raise RuntimeError("handler boom")

    worker = _worker(outbox, handler, max_attempts=2)

    # First failure: still retryable.
    await asyncio.wait_for(worker.run_once(), timeout=5)

    first = await outbox.get(event.id)

    assert first is not None
    assert first.status == OutboxEventStatus.PENDING
    assert first.attempt_count == 1

    # Skip the retry backoff so the second attempt happens immediately.
    await outbox.make_available(event.id)

    # Second failure reaches max_attempts and is terminal.
    await asyncio.wait_for(worker.run_once(), timeout=5)

    stored = await outbox.get(event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.FAILED
    assert stored.attempt_count == 2
    assert stored.locked_by is None
    assert stored.lease_token is None
    assert stored.lease_expires_at is None
    assert stored.last_error == "handler boom"


async def test_graceful_shutdown_finishes_current_batch(
    outbox: OutboxHarness,
) -> None:
    events = [await outbox.create_event() for _ in range(3)]
    all_started = asyncio.Event()
    started = 0
    finished = 0

    async def handler(_: OutboxEvent) -> None:
        nonlocal started, finished

        started += 1

        if started == len(events):
            all_started.set()

        await asyncio.sleep(0.1)
        finished += 1

    worker = _worker(outbox, handler, batch_size=len(events), poll_interval=0.05)
    run_task = asyncio.create_task(worker.run())

    await asyncio.wait_for(all_started.wait(), timeout=5)

    # The whole in-flight batch must drain before run() returns.
    worker.stop()
    await asyncio.wait_for(run_task, timeout=5)

    assert finished == len(events)

    for event in events:
        stored = await outbox.get(event.id)

        assert stored is not None
        assert stored.status == OutboxEventStatus.PROCESSED
        assert stored.locked_by is None
        assert stored.lease_token is None


async def _noop_handler(_: OutboxEvent) -> None:
    return None


async def test_stop_requested_is_false_until_stop(
    outbox: OutboxHarness,
) -> None:
    worker = _worker(outbox, _noop_handler)

    assert worker.stop_requested is False

    worker.stop()

    assert worker.stop_requested is True


async def test_wait_for_stop_returns_immediately_when_stop_is_requested(
    outbox: OutboxHarness,
) -> None:
    worker = _worker(outbox, _noop_handler)
    worker.stop()

    start = asyncio.get_running_loop().time()

    await asyncio.wait_for(worker.wait_for_stop(), timeout=5)

    assert asyncio.get_running_loop().time() - start < 0.5


async def test_wait_for_stop_blocks_until_stop_is_requested(
    outbox: OutboxHarness,
) -> None:
    worker = _worker(outbox, _noop_handler)
    wait_task = asyncio.create_task(worker.wait_for_stop())

    await asyncio.sleep(0)

    assert not wait_task.done()

    worker.stop()

    await asyncio.wait_for(wait_task, timeout=5)


async def test_wait_for_stop_times_out_when_stop_is_not_requested(
    outbox: OutboxHarness,
) -> None:
    worker = _worker(outbox, _noop_handler)

    start = asyncio.get_running_loop().time()

    with pytest.raises(TimeoutError):
        await asyncio.wait_for(worker.wait_for_stop(), timeout=0.05)

    assert asyncio.get_running_loop().time() - start >= 0.05
