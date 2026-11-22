import asyncio
import uuid
from typing import Any

import pytest
from sqlalchemy import delete, func, text, update
from sqlalchemy.exc import IntegrityError
from sqlalchemy.ext.asyncio import AsyncEngine, AsyncSession, async_sessionmaker

from memory_vault.models.outbox_event import OutboxEvent, OutboxEventStatus
from memory_vault.repositories.outbox import OutboxRepository
from memory_vault.services.outbox import DEFAULT_MAX_ATTEMPTS, OutboxService

SessionFactory = async_sessionmaker[AsyncSession]


async def _create_event(
    session_factory: SessionFactory,
    *,
    event_type: str = "test.event",
    payload: dict[str, Any] | None = None,
) -> OutboxEvent:
    async with session_factory() as session:
        return await OutboxService(session).create_event(
            event_type=event_type,
            aggregate_type="test",
            aggregate_id=uuid.uuid4(),
            payload=payload if payload is not None else {},
        )


async def _claim(
    session_factory: SessionFactory,
    worker_id: str,
    *,
    batch_size: int = 10,
    lease_duration_seconds: int = 60,
) -> list[OutboxEvent]:
    async with session_factory() as session:
        return await OutboxService(session).claim_batch(
            worker_id=worker_id,
            batch_size=batch_size,
            lease_duration_seconds=lease_duration_seconds,
        )


async def _get_event(
    session_factory: SessionFactory,
    event_id: uuid.UUID,
) -> OutboxEvent | None:
    async with session_factory() as session:
        return await session.get(OutboxEvent, event_id)


async def _renew_lease(
    session_factory: SessionFactory,
    event_id: uuid.UUID,
    worker_id: str,
    lease_token: uuid.UUID,
    lease_duration_seconds: int = 60,
) -> bool:
    async with session_factory() as session:
        return await OutboxService(session).renew_lease(
            event_id,
            worker_id,
            lease_token,
            lease_duration_seconds,
        )


async def _complete(
    session_factory: SessionFactory,
    event_id: uuid.UUID,
    worker_id: str,
    lease_token: uuid.UUID,
) -> OutboxEvent | None:
    async with session_factory() as session:
        return await OutboxService(session).complete(
            event_id,
            worker_id,
            lease_token,
        )


async def _fail(
    session_factory: SessionFactory,
    event_id: uuid.UUID,
    worker_id: str,
    lease_token: uuid.UUID,
    error: str,
) -> OutboxEvent | None:
    async with session_factory() as session:
        return await OutboxService(session).fail(
            event_id,
            worker_id,
            lease_token,
            error,
        )


async def _mark_failed(
    session_factory: SessionFactory,
    event_id: uuid.UUID,
    worker_id: str,
    lease_token: uuid.UUID,
    error: str,
) -> OutboxEvent | None:
    async with session_factory() as session:
        return await OutboxService(session).mark_failed(
            event_id,
            worker_id,
            lease_token,
            error,
        )


async def _handle_failure(
    session_factory: SessionFactory,
    event: OutboxEvent,
    worker_id: str,
    lease_token: uuid.UUID,
    error: str,
) -> OutboxEvent | None:
    async with session_factory() as session:
        return await OutboxService(session).handle_failure(
            event,
            worker_id=worker_id,
            lease_token=lease_token,
            error=error,
        )


async def _make_available(
    session_factory: SessionFactory,
    event_id: uuid.UUID,
) -> None:
    async with session_factory() as session:
        await session.execute(
            update(OutboxEvent).where(OutboxEvent.id == event_id).values(available_at=func.now())
        )
        await session.commit()


async def _expire_lease(
    session_factory: SessionFactory,
    event_id: uuid.UUID,
) -> None:
    async with session_factory() as session:
        await session.execute(
            update(OutboxEvent)
            .where(OutboxEvent.id == event_id)
            .values(
                lease_expires_at=func.now() - func.make_interval(0, 0, 0, 0, 0, 0, 1),
            )
        )
        await session.commit()


async def test_create_event(
    session_factory: SessionFactory,
) -> None:
    payload = {"user_id": str(uuid.uuid4())}

    event = await _create_event(
        session_factory,
        event_type="user.created",
        payload=payload,
    )

    assert isinstance(event.id, uuid.UUID)
    assert event.event_type == "user.created"
    assert event.aggregate_type == "test"
    assert event.payload == payload
    assert isinstance(event.status, OutboxEventStatus)
    assert event.status == OutboxEventStatus.PENDING
    assert event.attempt_count == 0
    assert event.locked_by is None
    assert event.lease_token is None
    assert event.lease_expires_at is None
    assert event.processed_at is None
    assert event.last_error is None

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.id == event.id
    assert stored.payload == payload
    assert isinstance(stored.status, OutboxEventStatus)
    assert stored.status == OutboxEventStatus.PENDING


async def test_claim_single_event(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(session_factory, "worker-a", batch_size=1)

    assert len(claimed) == 1
    assert claimed[0].id == event.id

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert isinstance(stored.status, OutboxEventStatus)
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-a"
    assert isinstance(stored.lease_token, uuid.UUID)
    assert stored.attempt_count == 1
    assert stored.lease_expires_at is not None


async def test_lease_token_generated_per_claim(
    session_factory: SessionFactory,
) -> None:
    first = await _create_event(session_factory)
    second = await _create_event(session_factory)

    assert first.lease_token is None
    assert second.lease_token is None

    claimed = await _claim(session_factory, "worker-a", batch_size=2)

    assert len(claimed) == 2

    tokens = {event.id: event.lease_token for event in claimed}

    assert all(isinstance(token, uuid.UUID) for token in tokens.values())
    assert len(set(tokens.values())) == 2

    await _expire_lease(session_factory, first.id)

    reclaimed = await _claim(session_factory, "worker-b", batch_size=1)

    assert len(reclaimed) == 1
    assert reclaimed[0].id == first.id
    assert reclaimed[0].lease_token != tokens[first.id]


async def test_raw_insert_processing_without_lease_is_rejected(
    session_factory: SessionFactory,
) -> None:
    async with session_factory() as session:
        with pytest.raises(IntegrityError) as exc_info:
            await session.execute(
                text(
                    """
                    INSERT INTO outbox_events (
                        event_type,
                        aggregate_type,
                        aggregate_id,
                        payload,
                        status
                    )
                    VALUES (
                        'test',
                        'message',
                        gen_random_uuid(),
                        '{}',
                        'PROCESSING'
                    )
                    """
                )
            )
            await session.commit()

    assert "state_consistency" in str(exc_info.value)

    async with session_factory() as session:
        total = await session.scalar(
            text("SELECT count(*) FROM outbox_events WHERE event_type = 'test'")
        )

    assert total == 0


async def test_lease_token_column_has_no_default(
    session_factory: SessionFactory,
) -> None:
    async with session_factory() as session:
        column_default = await session.scalar(
            text(
                """
                SELECT column_default
                FROM information_schema.columns
                WHERE table_name = 'outbox_events'
                  AND column_name = 'lease_token'
                """
            )
        )

    assert column_default is None


async def test_second_worker_does_not_claim_locked_event(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    first_claim = await _claim(session_factory, "worker-a", batch_size=1)
    second_claim = await _claim(session_factory, "worker-b", batch_size=1)

    assert [claimed.id for claimed in first_claim] == [event.id]
    assert second_claim == []

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-a"
    assert stored.lease_token == first_claim[0].lease_token
    assert stored.attempt_count == 1


async def test_expired_lease_allows_reclaim(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    first_claim = await _claim(session_factory, "worker-a", batch_size=1)
    await _expire_lease(session_factory, event.id)

    reclaimed = await _claim(session_factory, "worker-b", batch_size=1)

    assert len(reclaimed) == 1
    assert reclaimed[0].id == event.id
    assert reclaimed[0].lease_token != first_claim[0].lease_token

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-b"
    assert stored.lease_token == reclaimed[0].lease_token
    assert stored.attempt_count == 2


async def test_expired_lease_cannot_complete_or_fail(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(session_factory, "worker-a", batch_size=1)
    lease_token = claimed[0].lease_token

    assert lease_token is not None

    await _expire_lease(session_factory, event.id)

    # The lease is still held by worker-a (no one reclaimed it yet), but it has
    # expired, so the lease semantics say worker-a must not finish the job.
    assert await _complete(session_factory, event.id, "worker-a", lease_token) is None
    assert (
        await _fail(
            session_factory,
            event.id,
            "worker-a",
            lease_token,
            "too late",
        )
        is None
    )
    assert (
        await _mark_failed(
            session_factory,
            event.id,
            "worker-a",
            lease_token,
            "too late",
        )
        is None
    )

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-a"
    assert stored.lease_token == lease_token
    assert stored.last_error is None


async def test_renew_lease_extends_active_lease(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(
        session_factory,
        "worker-a",
        batch_size=1,
        lease_duration_seconds=60,
    )
    lease_token = claimed[0].lease_token

    assert lease_token is not None

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.lease_expires_at is not None

    original_expiry = stored.lease_expires_at

    renewed = await _renew_lease(
        session_factory,
        event.id,
        "worker-a",
        lease_token,
        lease_duration_seconds=120,
    )

    assert renewed is True

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.lease_expires_at is not None
    assert stored.lease_expires_at > original_expiry
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-a"
    assert stored.lease_token == lease_token
    assert stored.attempt_count == 1


async def test_renew_lease_with_wrong_token_fails(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(session_factory, "worker-a", batch_size=1)
    lease_token = claimed[0].lease_token

    assert lease_token is not None

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.lease_expires_at is not None

    original_expiry = stored.lease_expires_at

    renewed = await _renew_lease(
        session_factory,
        event.id,
        "worker-a",
        uuid.uuid4(),
        lease_duration_seconds=120,
    )

    assert renewed is False

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.lease_expires_at == original_expiry
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-a"
    assert stored.lease_token == lease_token


async def test_renew_lease_after_expiry_fails(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(session_factory, "worker-a", batch_size=1)
    lease_token = claimed[0].lease_token

    assert lease_token is not None

    await _expire_lease(session_factory, event.id)

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.lease_expires_at is not None

    expired_at = stored.lease_expires_at

    # An expired lease is not renewable: the worker must treat it as lost
    # ownership and let another worker reclaim the event.
    renewed = await _renew_lease(
        session_factory,
        event.id,
        "worker-a",
        lease_token,
        lease_duration_seconds=120,
    )

    assert renewed is False

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.lease_expires_at == expired_at
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-a"
    assert stored.lease_token == lease_token


async def test_renew_lease_for_another_workers_event_fails(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(session_factory, "worker-a", batch_size=1)
    lease_token = claimed[0].lease_token

    assert lease_token is not None

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.lease_expires_at is not None

    original_expiry = stored.lease_expires_at

    renewed = await _renew_lease(
        session_factory,
        event.id,
        "worker-b",
        lease_token,
        lease_duration_seconds=120,
    )

    assert renewed is False

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.lease_expires_at == original_expiry
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-a"
    assert stored.lease_token == lease_token


async def test_complete_refreshes_stale_identity_map_object(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(session_factory, "worker-a", batch_size=1)
    lease_token = claimed[0].lease_token

    assert lease_token is not None

    async with session_factory() as session:
        loaded = await session.get(OutboxEvent, event.id)

        assert loaded is not None
        assert loaded.status == OutboxEventStatus.PROCESSING

        processed = await OutboxRepository(session).complete(
            event.id,
            "worker-a",
            lease_token,
        )

        assert processed is not None
        assert processed is loaded
        assert processed.status == OutboxEventStatus.PROCESSED
        assert loaded.locked_by is None
        assert loaded.lease_token is None
        assert loaded.lease_expires_at is None


async def test_complete(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(session_factory, "worker-a", batch_size=1)
    lease_token = claimed[0].lease_token

    assert lease_token is not None

    processed = await _complete(
        session_factory,
        event.id,
        "worker-a",
        lease_token,
    )

    assert processed is not None
    assert processed.id == event.id
    assert processed.status == OutboxEventStatus.PROCESSED
    assert processed.processed_at is not None
    assert processed.locked_by is None
    assert processed.lease_token is None
    assert processed.lease_expires_at is None

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSED
    assert stored.processed_at is not None
    assert stored.lease_token is None
    assert await _claim(session_factory, "worker-b") == []


async def test_successful_processing_clears_last_error(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(session_factory, "worker-a", batch_size=1)

    assert claimed[0].lease_token is not None

    await _fail(
        session_factory,
        event.id,
        "worker-a",
        claimed[0].lease_token,
        "temporary failure",
    )
    await _make_available(session_factory, event.id)

    retried = await _claim(session_factory, "worker-a", batch_size=1)

    assert len(retried) == 1
    assert retried[0].last_error == "temporary failure"
    assert retried[0].lease_token is not None

    processed = await _complete(
        session_factory,
        event.id,
        "worker-a",
        retried[0].lease_token,
    )

    assert processed is not None
    assert processed.status == OutboxEventStatus.PROCESSED
    assert processed.last_error is None

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSED
    assert stored.last_error is None


async def test_fail_returns_event_to_pending(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(session_factory, "worker-a", batch_size=1)
    lease_token = claimed[0].lease_token

    assert claimed[0].attempt_count == 1
    assert lease_token is not None

    retried = await _fail(
        session_factory,
        event.id,
        "worker-a",
        lease_token,
        "temporary failure",
    )

    assert retried is not None
    assert retried.status == OutboxEventStatus.PENDING
    assert retried.attempt_count == 1
    assert retried.last_error == "temporary failure"
    assert retried.locked_by is None
    assert retried.lease_token is None
    assert retried.lease_expires_at is None
    assert retried.available_at > event.created_at

    delay_seconds = (retried.available_at - event.created_at).total_seconds()

    assert 1.5 <= delay_seconds <= 5.0

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PENDING
    assert stored.lease_token is None
    assert stored.available_at > event.created_at
    assert await _claim(session_factory, "worker-a") == []


async def test_failed_after_attempts_exhausted(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    for attempt in range(1, DEFAULT_MAX_ATTEMPTS + 1):
        claimed = await _claim(session_factory, "worker-a", batch_size=1)

        assert len(claimed) == 1
        assert claimed[0].attempt_count == attempt

        lease_token = claimed[0].lease_token

        assert lease_token is not None

        finished = await _handle_failure(
            session_factory,
            claimed[0],
            "worker-a",
            lease_token,
            f"failure {attempt}",
        )

        assert finished is not None

        if attempt < DEFAULT_MAX_ATTEMPTS:
            assert finished.status == OutboxEventStatus.PENDING
            await _make_available(session_factory, event.id)
        else:
            assert finished.status == OutboxEventStatus.FAILED

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.FAILED
    assert stored.attempt_count == DEFAULT_MAX_ATTEMPTS
    assert stored.last_error == f"failure {DEFAULT_MAX_ATTEMPTS}"
    assert stored.locked_by is None
    assert stored.lease_token is None
    assert stored.lease_expires_at is None


async def test_worker_cannot_finish_event_locked_by_another_worker(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    claimed = await _claim(session_factory, "worker-a", batch_size=1)
    lease_token = claimed[0].lease_token

    assert claimed[0].locked_by == "worker-a"
    assert lease_token is not None

    assert (
        await _complete(
            session_factory,
            event.id,
            "worker-b",
            lease_token,
        )
        is None
    )
    assert (
        await _fail(
            session_factory,
            event.id,
            "worker-b",
            lease_token,
            "hijack",
        )
        is None
    )
    assert (
        await _mark_failed(
            session_factory,
            event.id,
            "worker-b",
            lease_token,
            "hijack",
        )
        is None
    )

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-a"
    assert stored.lease_token == lease_token
    assert stored.attempt_count == 1
    assert stored.last_error is None


async def test_stale_lease_token_cannot_finish_reclaimed_event(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    first_claim = await _claim(session_factory, "worker-a", batch_size=1)
    stale_token = first_claim[0].lease_token

    assert stale_token is not None

    await _expire_lease(session_factory, event.id)

    second_claim = await _claim(session_factory, "worker-a", batch_size=1)

    assert len(second_claim) == 1

    fresh_token = second_claim[0].lease_token

    assert fresh_token is not None
    assert fresh_token != stale_token

    assert (
        await _complete(
            session_factory,
            event.id,
            "worker-a",
            stale_token,
        )
        is None
    )
    assert (
        await _fail(
            session_factory,
            event.id,
            "worker-a",
            stale_token,
            "stale failure",
        )
        is None
    )
    assert (
        await _mark_failed(
            session_factory,
            event.id,
            "worker-a",
            stale_token,
            "stale failure",
        )
        is None
    )

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-a"
    assert stored.lease_token == fresh_token
    assert stored.attempt_count == 2
    assert stored.last_error is None

    processed = await _complete(
        session_factory,
        event.id,
        "worker-a",
        fresh_token,
    )

    assert processed is not None
    assert processed.status == OutboxEventStatus.PROCESSED


async def test_old_worker_cannot_finish_after_reclaim_by_another_worker(
    session_factory: SessionFactory,
) -> None:
    event = await _create_event(session_factory)

    # 1. Worker A claims the event.
    claim_a = await _claim(session_factory, "worker-a", batch_size=1)
    token_a = claim_a[0].lease_token

    assert token_a is not None

    # 2. Worker A hangs; its lease expires.
    await _expire_lease(session_factory, event.id)

    # 3. Worker B reclaims the event with a fresh token.
    claim_b = await _claim(session_factory, "worker-b", batch_size=1)
    token_b = claim_b[0].lease_token

    assert token_b is not None
    assert token_b != token_a
    assert claim_b[0].locked_by == "worker-b"

    # 4. Worker A wakes up and tries to finish with its stale token.
    assert (
        await _complete(
            session_factory,
            event.id,
            "worker-a",
            token_a,
        )
        is None
    )
    assert (
        await _fail(
            session_factory,
            event.id,
            "worker-a",
            token_a,
            "stale failure",
        )
        is None
    )
    assert (
        await _mark_failed(
            session_factory,
            event.id,
            "worker-a",
            token_a,
            "stale failure",
        )
        is None
    )

    # Worker A's attempt changed nothing: Worker B still owns the event.
    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PROCESSING
    assert stored.locked_by == "worker-b"
    assert stored.lease_token == token_b
    assert stored.attempt_count == 2
    assert stored.last_error is None

    # Worker B finishes the job normally.
    processed = await _complete(
        session_factory,
        event.id,
        "worker-b",
        token_b,
    )

    assert processed is not None
    assert processed.status == OutboxEventStatus.PROCESSED
    assert processed.processed_at is not None
    assert processed.locked_by is None
    assert processed.lease_token is None
    assert processed.lease_expires_at is None


async def test_concurrent_workers_claim_different_events(
    engine: AsyncEngine,
) -> None:
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    event_ids: list[uuid.UUID] = []

    try:
        first = await _create_event(session_factory)
        event_ids.append(first.id)

        second = await _create_event(session_factory)
        event_ids.append(second.id)

        worker_a, worker_b = await asyncio.gather(
            _claim(session_factory, "worker-a", batch_size=1),
            _claim(session_factory, "worker-b", batch_size=1),
        )

        ids_a = {event.id for event in worker_a}
        ids_b = {event.id for event in worker_b}

        assert len(worker_a) == 1
        assert len(worker_b) == 1
        assert ids_a.isdisjoint(ids_b)
        assert ids_a | ids_b == set(event_ids)
    finally:
        if event_ids:
            async with session_factory() as session:
                await session.execute(delete(OutboxEvent).where(OutboxEvent.id.in_(event_ids)))
                await session.commit()


async def test_many_concurrent_workers_claim_each_event_once(
    engine: AsyncEngine,
) -> None:
    total_events = 12
    workers = [f"worker-{index}" for index in range(4)]
    session_factory = async_sessionmaker(
        bind=engine,
        class_=AsyncSession,
        expire_on_commit=False,
    )
    event_ids: list[uuid.UUID] = []

    try:
        for _ in range(total_events):
            event = await _create_event(session_factory)
            event_ids.append(event.id)

        batches = await asyncio.gather(
            *(
                _claim(
                    session_factory,
                    worker_id,
                    batch_size=3,
                    lease_duration_seconds=60,
                )
                for worker_id in workers
            )
        )

        claimed = [event for batch in batches for event in batch]
        claimed_ids = [event.id for event in claimed]

        assert len(claimed_ids) == total_events
        assert len(set(claimed_ids)) == total_events
        assert set(claimed_ids) == set(event_ids)

        assert len({event.lease_token for event in claimed}) == total_events

        for event in claimed:
            assert event.status == OutboxEventStatus.PROCESSING
            assert event.attempt_count == 1
            assert event.locked_by in workers
            assert event.lease_token is not None
            assert event.lease_expires_at is not None
            assert event.processed_at is None
    finally:
        if event_ids:
            async with session_factory() as session:
                await session.execute(delete(OutboxEvent).where(OutboxEvent.id.in_(event_ids)))
                await session.commit()


@pytest.mark.parametrize(
    "values",
    [
        {
            "status": OutboxEventStatus.PROCESSED,
            "processed_at": None,
            "locked_by": None,
            "lease_token": None,
            "lease_expires_at": None,
        },
        {
            "status": OutboxEventStatus.PROCESSING,
            "processed_at": None,
            "locked_by": None,
            "lease_token": func.gen_random_uuid(),
            "lease_expires_at": func.now(),
        },
        {
            "status": OutboxEventStatus.PROCESSING,
            "processed_at": None,
            "locked_by": "worker-a",
            "lease_token": func.gen_random_uuid(),
            "lease_expires_at": None,
        },
        {
            "status": OutboxEventStatus.PROCESSING,
            "processed_at": None,
            "locked_by": "worker-a",
            "lease_token": None,
            "lease_expires_at": func.now(),
        },
        {
            "status": OutboxEventStatus.PROCESSING,
            "processed_at": func.now(),
            "locked_by": "worker-a",
            "lease_token": func.gen_random_uuid(),
            "lease_expires_at": func.now(),
        },
        {
            "status": OutboxEventStatus.PENDING,
            "processed_at": None,
            "locked_by": "worker-a",
            "lease_token": func.gen_random_uuid(),
            "lease_expires_at": func.now(),
        },
        {
            "status": OutboxEventStatus.FAILED,
            "processed_at": None,
            "locked_by": "worker-a",
            "lease_token": func.gen_random_uuid(),
            "lease_expires_at": func.now(),
        },
        {
            "status": OutboxEventStatus.PROCESSED,
            "processed_at": func.now(),
            "locked_by": "worker-a",
            "lease_token": func.gen_random_uuid(),
            "lease_expires_at": func.now(),
        },
        {
            "status": OutboxEventStatus.PENDING,
            "processed_at": func.now(),
            "locked_by": None,
            "lease_token": None,
            "lease_expires_at": None,
        },
        {
            "status": OutboxEventStatus.PROCESSED,
            "processed_at": func.now(),
            "locked_by": None,
            "lease_token": func.gen_random_uuid(),
            "lease_expires_at": None,
        },
        {
            "status": OutboxEventStatus.PENDING,
            "processed_at": None,
            "locked_by": None,
            "lease_token": func.gen_random_uuid(),
            "lease_expires_at": None,
        },
    ],
)
async def test_inconsistent_status_is_rejected(
    session_factory: SessionFactory,
    values: dict[str, Any],
) -> None:
    event = await _create_event(session_factory)

    async with session_factory() as session:
        with pytest.raises(IntegrityError):
            await session.execute(
                update(OutboxEvent).where(OutboxEvent.id == event.id).values(**values)
            )
            await session.commit()

    stored = await _get_event(session_factory, event.id)

    assert stored is not None
    assert stored.status == OutboxEventStatus.PENDING
