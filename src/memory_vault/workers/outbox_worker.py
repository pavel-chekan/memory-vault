import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable
from typing import cast

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from memory_vault.models.outbox_event import OutboxEvent
from memory_vault.services.outbox import DEFAULT_MAX_ATTEMPTS, OutboxService

logger = logging.getLogger(__name__)

OutboxEventHandler = Callable[[OutboxEvent], Awaitable[None]]


class LeaseLostError(Exception):
    """The worker no longer owns the event's lease."""

    def __init__(self, event_id: uuid.UUID) -> None:
        super().__init__(f"Lease for outbox event {event_id} is no longer held by this worker")
        self.event_id = event_id


class HeartbeatError(Exception):
    """The heartbeat could not renew the lease because renewal raised."""

    def __init__(self, event_id: uuid.UUID) -> None:
        super().__init__(f"Failed to renew the lease for outbox event {event_id}")
        self.event_id = event_id


class OutboxWorker:
    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        handlers: dict[str, OutboxEventHandler],
        *,
        worker_id: str | None = None,
        batch_size: int = 10,
        lease_duration_seconds: int = 60,
        heartbeat_interval_seconds: float | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        poll_interval: float = 1.0,
    ) -> None:
        heartbeat_interval = (
            heartbeat_interval_seconds
            if heartbeat_interval_seconds is not None
            else lease_duration_seconds / 3
        )

        if heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval_seconds must be greater than zero")

        if heartbeat_interval >= lease_duration_seconds:
            raise ValueError("heartbeat_interval_seconds must be less than lease_duration_seconds")

        self.session_factory = session_factory
        self.handlers = handlers
        self.worker_id = worker_id or f"outbox-worker-{uuid.uuid4()}"
        self.batch_size = batch_size
        self.lease_duration_seconds = lease_duration_seconds
        self.heartbeat_interval_seconds = heartbeat_interval
        self.max_attempts = max_attempts
        self.poll_interval = poll_interval
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        self._stop_event.set()

    async def run(self) -> None:
        while not self._stop_event.is_set():
            processed = await self.run_once()

            if processed == 0:
                try:
                    await asyncio.wait_for(
                        self._stop_event.wait(),
                        timeout=self.poll_interval,
                    )
                except TimeoutError:
                    pass

    async def run_once(self) -> int:
        async with self.session_factory() as session:
            events = await OutboxService(session).claim_batch(
                worker_id=self.worker_id,
                batch_size=self.batch_size,
                lease_duration_seconds=self.lease_duration_seconds,
            )

        for event in events:
            await self._process(event)

        return len(events)

    def _service(self, session: AsyncSession) -> OutboxService:
        return OutboxService(session, max_attempts=self.max_attempts)

    async def _process(self, event: OutboxEvent) -> None:
        lease_token = event.lease_token

        if lease_token is None:
            logger.error("Outbox event %s has no lease token", event.id)
            return

        handler = self.handlers.get(event.event_type)

        if handler is None:
            await self._fail(
                event,
                lease_token,
                f"No handler registered for event type {event.event_type!r}",
            )
            return

        # The handler may run for much longer than the lease (for example, an
        # LLM request). The heartbeat renews the lease in parallel and stops as
        # soon as the handler finishes. Both tasks live in one TaskGroup, so a
        # failure on either side cancels the other and is propagated here.
        handler_finished = asyncio.Event()

        async def run_handler() -> None:
            try:
                await handler(event)
            finally:
                handler_finished.set()

        lease_lost = False
        heartbeat_error: HeartbeatError | None = None
        handler_error: Exception | None = None

        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(self._heartbeat(event.id, lease_token, handler_finished))
                group.create_task(run_handler())
        except* LeaseLostError:
            lease_lost = True
        except* HeartbeatError as errors:
            heartbeat_error = cast(HeartbeatError, errors.exceptions[0])
        except* Exception as errors:
            handler_error = errors.exceptions[0]

        if lease_lost:
            # Another worker owns the event now. Do not complete or fail it:
            # any write would be rejected by the lease check, and the new owner
            # is responsible for the outcome.
            logger.warning(
                "Outbox event %s lost its lease; processing was cancelled",
                event.id,
            )
            return

        if heartbeat_error is not None:
            logger.error(
                "Outbox event %s heartbeat failed; processing was cancelled",
                event.id,
                exc_info=heartbeat_error.__cause__,
            )
            await self._fail(event, lease_token, str(heartbeat_error))
            return

        if handler_error is not None:
            logger.error(
                "Outbox event %s failed",
                event.id,
                exc_info=handler_error,
            )
            await self._fail(event, lease_token, str(handler_error))
            return

        await self._complete(event, lease_token)

    async def _heartbeat(
        self,
        event_id: uuid.UUID,
        lease_token: uuid.UUID,
        handler_finished: asyncio.Event,
    ) -> None:
        """Renew the lease every ``heartbeat_interval_seconds`` until the handler ends.

        ``handler_finished`` wakes the loop immediately, so the heartbeat stops
        without waiting for the next interval after processing completes.
        """
        while True:
            try:
                async with asyncio.timeout(self.heartbeat_interval_seconds):
                    await handler_finished.wait()
            except TimeoutError:
                await self._renew_lease(event_id, lease_token)
            else:
                return

    async def _renew_lease(
        self,
        event_id: uuid.UUID,
        lease_token: uuid.UUID,
    ) -> None:
        try:
            async with self.session_factory() as session:
                renewed = await self._service(session).renew_lease(
                    event_id,
                    self.worker_id,
                    lease_token,
                    self.lease_duration_seconds,
                )
        except Exception as exc:
            # The lease state is unknown (for example, the database is
            # unreachable), so stop processing instead of risking a duplicate.
            raise HeartbeatError(event_id) from exc

        if not renewed:
            raise LeaseLostError(event_id)

        logger.debug("Renewed lease for outbox event %s", event_id)

    async def _complete(
        self,
        event: OutboxEvent,
        lease_token: uuid.UUID,
    ) -> None:
        async with self.session_factory() as session:
            completed = await self._service(session).complete(
                event.id,
                self.worker_id,
                lease_token,
            )

        if completed is None:
            logger.warning(
                "Outbox event %s completed without a valid lease; another worker owns it now",
                event.id,
            )

    async def _fail(
        self,
        event: OutboxEvent,
        lease_token: uuid.UUID,
        error: str,
    ) -> None:
        async with self.session_factory() as session:
            failed = await self._service(session).handle_failure(
                event,
                worker_id=self.worker_id,
                lease_token=lease_token,
                error=error,
            )

        if failed is None:
            logger.warning(
                "Outbox event %s failed without a valid lease; another worker owns it now",
                event.id,
            )
