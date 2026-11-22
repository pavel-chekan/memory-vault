import uuid
from typing import Any

from sqlalchemy import func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncSession
from sqlalchemy.ext.compiler import compiles
from sqlalchemy.sql.functions import FunctionElement

from memory_vault.models.outbox_event import OutboxEvent, OutboxEventStatus


class _MakeInterval(FunctionElement[Any]):
    """``make_interval(secs => ...)`` with an explicit named argument.

    SQLAlchemy's generic ``func`` does not accept keyword arguments, so the
    PostgreSQL named notation is rendered by a dedicated compiler hook.
    """

    name = "make_interval"
    inherit_cache = True


@compiles(_MakeInterval, "postgresql")
def _compile_make_interval(
    element: _MakeInterval,
    compiler: Any,
    **kw: Any,
) -> str:
    seconds = next(iter(element.clauses))

    return f"make_interval(secs => {compiler.process(seconds, **kw)})"


class OutboxRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: uuid.UUID,
        payload: dict[str, Any],
    ) -> OutboxEvent:
        event = OutboxEvent(
            event_type=event_type,
            aggregate_type=aggregate_type,
            aggregate_id=aggregate_id,
            payload=payload,
        )

        self.session.add(event)
        await self.session.flush()

        return event

    async def claim_batch(
        self,
        worker_id: str,
        batch_size: int,
        lease_duration_seconds: int,
    ) -> list[OutboxEvent]:
        """Atomically claim up to ``batch_size`` due events for ``worker_id``.

        Runs as a single statement: ``FOR UPDATE SKIP LOCKED`` lets concurrent
        workers skip rows already being claimed by another transaction, so no
        worker blocks on or steals another worker's batch. Every claim assigns
        a fresh ``lease_token``; a stale lease can be reclaimed once
        ``lease_expires_at`` has passed.
        """
        candidates = (
            select(OutboxEvent.id)
            .where(
                or_(
                    (OutboxEvent.status == OutboxEventStatus.PENDING)
                    & (OutboxEvent.available_at <= func.now()),
                    (OutboxEvent.status == OutboxEventStatus.PROCESSING)
                    & (OutboxEvent.lease_expires_at < func.now()),
                )
            )
            .order_by(OutboxEvent.created_at, OutboxEvent.id)
            .limit(batch_size)
            .with_for_update(skip_locked=True)
            .cte("candidates")
        )

        stmt = (
            update(OutboxEvent)
            .where(OutboxEvent.id == candidates.c.id)
            .values(
                status=OutboxEventStatus.PROCESSING,
                locked_by=worker_id,
                lease_token=func.gen_random_uuid(),
                lease_expires_at=func.now() + _MakeInterval(lease_duration_seconds),
                attempt_count=OutboxEvent.attempt_count + 1,
            )
            .returning(OutboxEvent)
            .execution_options(
                synchronize_session=False,
                populate_existing=True,
            )
        )

        result = await self.session.execute(stmt)
        events: list[OutboxEvent] = list(result.scalars().all())

        return events

    async def renew_lease(
        self,
        event_id: uuid.UUID,
        worker_id: str,
        lease_token: uuid.UUID,
        lease_duration_seconds: int,
    ) -> bool:
        stmt = (
            update(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.status == OutboxEventStatus.PROCESSING,
                OutboxEvent.locked_by == worker_id,
                OutboxEvent.lease_token == lease_token,
                OutboxEvent.lease_expires_at > func.now(),
            )
            .values(
                lease_expires_at=func.now() + _MakeInterval(lease_duration_seconds),
            )
            .returning(OutboxEvent.id)
            .execution_options(
                synchronize_session=False,
                populate_existing=True,
            )
        )
        result = await self.session.execute(stmt)
        return result.scalar_one_or_none() is not None

    async def complete(
        self,
        event_id: uuid.UUID,
        worker_id: str,
        lease_token: uuid.UUID,
    ) -> OutboxEvent | None:
        """Mark the event as PROCESSED if the caller still owns a valid lease.

        The ownership check is ``id + locked_by + lease_token + PROCESSING``
        plus ``lease_expires_at > now()``. A ``None`` result means the worker
        no longer owns the event (expired lease, reclaimed lease, or another
        worker), so a stale worker cannot overwrite another one.
        """
        return await self._finish(
            event_id,
            worker_id,
            lease_token,
            values={
                "status": OutboxEventStatus.PROCESSED,
                "processed_at": func.now(),
                "locked_by": None,
                "lease_token": None,
                "lease_expires_at": None,
                "last_error": None,
            },
        )

    async def fail(
        self,
        event_id: uuid.UUID,
        worker_id: str,
        lease_token: uuid.UUID,
        error: str,
    ) -> OutboxEvent | None:
        """Release the lease and return the event to PENDING for a later retry.

        ``available_at`` is pushed forward with exponential backoff based on
        the current ``attempt_count``, capped at one hour. Ownership is checked
        the same way as in ``complete``.
        """
        delay_seconds = func.least(
            func.power(2.0, OutboxEvent.attempt_count),
            3600.0,
        )

        return await self._finish(
            event_id,
            worker_id,
            lease_token,
            values={
                "status": OutboxEventStatus.PENDING,
                "available_at": func.now() + _MakeInterval(delay_seconds),
                "locked_by": None,
                "lease_token": None,
                "lease_expires_at": None,
                "last_error": error,
            },
        )

    async def mark_failed(
        self,
        event_id: uuid.UUID,
        worker_id: str,
        lease_token: uuid.UUID,
        error: str,
    ) -> OutboxEvent | None:
        return await self._finish(
            event_id,
            worker_id,
            lease_token,
            values={
                "status": OutboxEventStatus.FAILED,
                "locked_by": None,
                "lease_token": None,
                "lease_expires_at": None,
                "last_error": error,
            },
        )

    async def _finish(
        self,
        event_id: uuid.UUID,
        worker_id: str,
        lease_token: uuid.UUID,
        values: dict[str, Any],
    ) -> OutboxEvent | None:
        stmt = (
            update(OutboxEvent)
            .where(
                OutboxEvent.id == event_id,
                OutboxEvent.locked_by == worker_id,
                OutboxEvent.lease_token == lease_token,
                OutboxEvent.status == OutboxEventStatus.PROCESSING,
                OutboxEvent.lease_expires_at > func.now(),
            )
            .values(**values)
            .returning(OutboxEvent)
            .execution_options(
                synchronize_session=False,
                populate_existing=True,
            )
        )

        result = await self.session.execute(stmt)
        event: OutboxEvent | None = result.scalar_one_or_none()

        return event
