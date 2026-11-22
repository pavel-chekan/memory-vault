import asyncio
import logging
import uuid
from collections.abc import Awaitable, Callable

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from memory_vault.models.outbox_event import OutboxEvent
from memory_vault.services.outbox import DEFAULT_MAX_ATTEMPTS, OutboxService

logger = logging.getLogger(__name__)

# Handler contract:
#
# * idempotent — events are delivered at least once (a lease can expire and the
#   event be claimed again), so repeating a handler must be safe;
# * cancellation-cooperative — never suppress ``asyncio.CancelledError``; the
#   worker cancels the handler when the lease is lost, the heartbeat fails, or
#   the worker is stopped;
# * non-blocking — use async I/O, or move blocking/CPU-bound work to a thread.
#   A handler that blocks the event loop also blocks the heartbeat, so its lease
#   can expire while it is still running.
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


class HandlerTimeoutError(Exception):
    """The worker's own ``handler_timeout_seconds`` expired.

    Kept distinct from a ``TimeoutError`` raised by the handler itself (for
    example an HTTP client timeout), which is reported as a normal handler
    failure instead of being attributed to the worker's timeout.
    """

    def __init__(self, event_id: uuid.UUID, timeout: float) -> None:
        super().__init__(f"Handler for outbox event {event_id} timed out after {timeout} seconds")
        self.event_id = event_id
        self.timeout = timeout


def _error_text(exc: BaseException) -> str:
    message = str(exc)

    return message or type(exc).__name__


class OutboxWorker:
    """Claim and process outbox events with a lease heartbeat.

    Lifecycle invariants:

    * every claimed event has exactly one ``_process`` owner;
    * the handler and its heartbeat live in one ``TaskGroup`` per event, so
      neither outlives ``_process``;
    * an event is completed, failed, or (when the lease is lost) left alone;
    * ``stop()`` drains the current batch, while an external ``cancel()``
      cancels handlers and heartbeats and lets the leases expire.

    Cancellation is cooperative: ``TaskGroup`` cancels the handler when the
    lease is lost or the heartbeat fails, but a handler that suppresses
    ``CancelledError`` keeps the group (and therefore the batch) waiting. See
    ``OutboxEventHandler`` for the full handler contract.
    """

    def __init__(
        self,
        session_factory: async_sessionmaker[AsyncSession],
        handlers: dict[str, OutboxEventHandler],
        *,
        worker_id: str | None = None,
        batch_size: int = 10,
        lease_duration_seconds: int = 60,
        heartbeat_interval_seconds: float | None = None,
        handler_timeout_seconds: float | None = None,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
        poll_interval: float = 1.0,
    ) -> None:
        heartbeat_interval = (
            heartbeat_interval_seconds
            if heartbeat_interval_seconds is not None
            else lease_duration_seconds / 3
        )

        if batch_size < 1:
            raise ValueError("batch_size must be at least 1")

        if lease_duration_seconds <= 0:
            raise ValueError("lease_duration_seconds must be greater than zero")

        if heartbeat_interval <= 0:
            raise ValueError("heartbeat_interval_seconds must be greater than zero")

        if heartbeat_interval >= lease_duration_seconds:
            raise ValueError("heartbeat_interval_seconds must be less than lease_duration_seconds")

        if handler_timeout_seconds is not None and handler_timeout_seconds <= 0:
            raise ValueError("handler_timeout_seconds must be greater than zero")

        if poll_interval <= 0:
            raise ValueError("poll_interval must be greater than zero")

        self.session_factory = session_factory
        self.handlers = handlers
        self.worker_id = worker_id or f"outbox-worker-{uuid.uuid4()}"
        self.batch_size = batch_size
        self.lease_duration_seconds = lease_duration_seconds
        self.heartbeat_interval_seconds = heartbeat_interval
        self.handler_timeout_seconds = handler_timeout_seconds
        self.max_attempts = max_attempts
        self.poll_interval = poll_interval
        self._stop_event = asyncio.Event()

    def stop(self) -> None:
        """Request a graceful shutdown.

        The current batch finishes and ``run()`` returns. ``stop()`` also wakes
        ``run()`` immediately when it is waiting for the next poll.
        """
        self._stop_event.set()

    async def run(self) -> None:
        while not self._stop_event.is_set():
            processed = await self.run_once()

            if processed == 0:
                await self._wait_for_poll()

    async def run_once(self) -> int:
        events = await self._claim_batch()
        await self._process_batch(events)

        return len(events)

    async def _claim_batch(self) -> list[OutboxEvent]:
        # claim_batch() returns ORM instances after its transaction commits. The
        # worker reads their attributes after the session closes, so they must
        # not be expired on commit. Override the factory default here instead of
        # relying on callers to configure ``expire_on_commit=False``.
        async with self.session_factory(expire_on_commit=False) as session:
            return await OutboxService(session).claim_batch(
                worker_id=self.worker_id,
                batch_size=self.batch_size,
                lease_duration_seconds=self.lease_duration_seconds,
            )

    async def _wait_for_poll(self) -> None:
        try:
            await asyncio.wait_for(
                self._stop_event.wait(),
                timeout=self.poll_interval,
            )
        except TimeoutError:
            pass

    async def _process_batch(self, events: list[OutboxEvent]) -> None:
        """Process a claimed batch concurrently.

        ``batch_size`` therefore acts as the concurrency limit: every event's
        heartbeat starts at (roughly) the same time, so a long handler cannot
        let the leases of later events expire before they are even started.
        """
        if not events:
            return

        async with asyncio.TaskGroup() as group:
            for event in events:
                group.create_task(
                    self._process(event),
                    name=f"outbox-event-{event.id}",
                )

    def _service(self, session: AsyncSession) -> OutboxService:
        return OutboxService(session, max_attempts=self.max_attempts)

    async def _process(self, event: OutboxEvent) -> None:
        lease_token = event.lease_token

        if lease_token is None:
            logger.error("Outbox event %s has no lease token", event.id)
            return

        handler = self.handlers.get(event.event_type)

        if handler is None:
            await self._fail_safely(
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
        handler_completed = False

        async def run_handler() -> None:
            nonlocal handler_completed

            try:
                if self.handler_timeout_seconds is None:
                    await handler(event)
                else:
                    timeout = asyncio.timeout(self.handler_timeout_seconds)

                    try:
                        async with timeout:
                            await handler(event)
                    except TimeoutError as exc:
                        # Only the timeout owned by the worker is converted.
                        # A TimeoutError raised by the handler itself (e.g. an
                        # HTTP client timeout) must stay a normal handler
                        # failure, even when handler_timeout_seconds is set.
                        if not timeout.expired():
                            raise

                        raise HandlerTimeoutError(
                            event.id,
                            self.handler_timeout_seconds,
                        ) from exc

                handler_completed = True
            finally:
                handler_finished.set()

        lease_lost = False
        handler_timeout_error: BaseException | None = None
        heartbeat_error: BaseException | None = None
        handler_error: BaseException | None = None

        try:
            async with asyncio.TaskGroup() as group:
                group.create_task(
                    self._heartbeat(event.id, lease_token, handler_finished),
                    name=f"outbox-heartbeat-{event.id}",
                )
                group.create_task(
                    run_handler(),
                    name=f"outbox-handler-{event.id}",
                )
        except* LeaseLostError:
            lease_lost = True
        except* HeartbeatError as errors:
            heartbeat_error = errors.exceptions[0]
        except* HandlerTimeoutError as errors:
            handler_timeout_error = errors.exceptions[0]
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
            await self._fail_safely(event, lease_token, _error_text(heartbeat_error))
            return

        if handler_timeout_error is not None:
            logger.error(
                "Outbox event %s handler timed out after %s seconds",
                event.id,
                self.handler_timeout_seconds,
            )
            await self._fail_safely(
                event,
                lease_token,
                _error_text(handler_timeout_error),
            )
            return

        if handler_error is not None:
            logger.error(
                "Outbox event %s failed",
                event.id,
                exc_info=handler_error,
            )
            await self._fail_safely(event, lease_token, _error_text(handler_error))
            return

        if not handler_completed:
            # A TaskGroup silently ignores a child that ends cancelled when the
            # group itself is not cancelling. An external cancellation of the
            # worker never reaches this point (it propagates out of _process),
            # so the handler cancelled itself without finishing its work.
            logger.error(
                "Outbox event %s handler was cancelled before completing",
                event.id,
            )
            await self._fail_safely(
                event,
                lease_token,
                "Handler was cancelled before completing",
            )
            return

        await self._complete_safely(event, lease_token)

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
            # OutboxService owns the write transaction: its ``session.begin()``
            # commits on success and rolls back on error. Do not nest another
            # ``session.begin()`` here (SQLAlchemy rejects a second begin on the
            # same session).
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

    async def _complete_safely(
        self,
        event: OutboxEvent,
        lease_token: uuid.UUID,
    ) -> None:
        try:
            # OutboxService owns the write transaction (commit on success).
            async with self.session_factory() as session:
                completed = await self._service(session).complete(
                    event.id,
                    self.worker_id,
                    lease_token,
                )
        except Exception:
            logger.exception(
                "Failed to complete outbox event %s; "
                "its lease will expire and the event will be retried",
                event.id,
            )
            return

        if completed is None:
            logger.warning(
                "Outbox event %s was not completed because this worker "
                "no longer holds a valid lease",
                event.id,
            )

    async def _fail_safely(
        self,
        event: OutboxEvent,
        lease_token: uuid.UUID,
        error: str,
    ) -> None:
        try:
            # OutboxService owns the write transaction (commit on success).
            async with self.session_factory() as session:
                failed = await self._service(session).handle_failure(
                    event,
                    worker_id=self.worker_id,
                    lease_token=lease_token,
                    error=error,
                )
        except Exception:
            logger.exception(
                "Failed to record failure for outbox event %s; "
                "its lease will expire and the event will be retried",
                event.id,
            )
            return

        if failed is None:
            logger.warning(
                "Outbox event %s was not failed because this worker no longer holds a valid lease",
                event.id,
            )
