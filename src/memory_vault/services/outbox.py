import uuid
from typing import Any

from sqlalchemy.ext.asyncio import AsyncSession

from memory_vault.models.outbox_event import OutboxEvent
from memory_vault.repositories.outbox import OutboxRepository

DEFAULT_MAX_ATTEMPTS = 3


class OutboxService:
    def __init__(
        self,
        session: AsyncSession,
        max_attempts: int = DEFAULT_MAX_ATTEMPTS,
    ) -> None:
        self.session = session
        self.repository = OutboxRepository(session)
        self.max_attempts = max_attempts

    async def create_event(
        self,
        *,
        event_type: str,
        aggregate_type: str,
        aggregate_id: uuid.UUID,
        payload: dict[str, Any],
    ) -> OutboxEvent:
        async with self.session.begin():
            return await self.repository.create(
                event_type=event_type,
                aggregate_type=aggregate_type,
                aggregate_id=aggregate_id,
                payload=payload,
            )

    async def claim_batch(
        self,
        *,
        worker_id: str,
        batch_size: int = 10,
        lease_duration_seconds: int = 60,
    ) -> list[OutboxEvent]:
        async with self.session.begin():
            return await self.repository.claim_batch(
                worker_id=worker_id,
                batch_size=batch_size,
                lease_duration_seconds=lease_duration_seconds,
            )

    async def renew_lease(
        self,
        event_id: uuid.UUID,
        worker_id: str,
        lease_token: uuid.UUID,
        lease_duration_seconds: int,
    ) -> bool:
        async with self.session.begin():
            return await self.repository.renew_lease(
                event_id,
                worker_id,
                lease_token,
                lease_duration_seconds,
            )

    async def complete(
        self,
        event_id: uuid.UUID,
        worker_id: str,
        lease_token: uuid.UUID,
    ) -> OutboxEvent | None:
        async with self.session.begin():
            return await self.repository.complete(
                event_id,
                worker_id,
                lease_token,
            )

    async def fail(
        self,
        event_id: uuid.UUID,
        worker_id: str,
        lease_token: uuid.UUID,
        error: str,
    ) -> OutboxEvent | None:
        async with self.session.begin():
            return await self.repository.fail(
                event_id,
                worker_id,
                lease_token,
                error,
            )

    async def mark_failed(
        self,
        event_id: uuid.UUID,
        worker_id: str,
        lease_token: uuid.UUID,
        error: str,
    ) -> OutboxEvent | None:
        async with self.session.begin():
            return await self.repository.mark_failed(
                event_id,
                worker_id,
                lease_token,
                error,
            )

    async def handle_failure(
        self,
        event: OutboxEvent,
        *,
        worker_id: str,
        lease_token: uuid.UUID,
        error: str,
    ) -> OutboxEvent | None:
        if event.attempt_count >= self.max_attempts:
            return await self.mark_failed(
                event.id,
                worker_id,
                lease_token,
                error,
            )

        return await self.fail(
            event.id,
            worker_id,
            lease_token,
            error,
        )
