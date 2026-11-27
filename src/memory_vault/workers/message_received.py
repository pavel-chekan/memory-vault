import logging
import uuid

from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from memory_vault.models.outbox_event import OutboxEvent
from memory_vault.repositories.saved_messages import SavedMessageRepository
from memory_vault.workers.outbox_worker import OutboxEventHandler

logger = logging.getLogger(__name__)


def _saved_message_id(event: OutboxEvent) -> uuid.UUID:
    """Extract and validate the ``saved_message_id`` from the event payload.

    Malformed payloads raise ``ValueError`` so the worker records the event as
    failed instead of silently dropping it. The payload id must also match
    ``aggregate_id`` because the producer writes both from the same message.
    """
    payload = event.payload

    if not isinstance(payload, dict):
        raise ValueError(f"Outbox event {event.id} has a non-object payload")

    raw_message_id = payload.get("saved_message_id")

    if not isinstance(raw_message_id, str):
        raise ValueError(f"Outbox event {event.id} payload has no string 'saved_message_id'")

    try:
        message_id = uuid.UUID(raw_message_id)
    except ValueError as exc:
        raise ValueError(
            f"Outbox event {event.id} payload has an invalid 'saved_message_id': {raw_message_id!r}"
        ) from exc

    if message_id != event.aggregate_id:
        raise ValueError(
            f"Outbox event {event.id} 'saved_message_id' {message_id} "
            f"does not match aggregate_id {event.aggregate_id}"
        )

    return message_id


def create_message_received_handler(
    session_factory: async_sessionmaker[AsyncSession],
) -> OutboxEventHandler:
    """Build the ``message.received`` handler.

    The handler is read-only and idempotent: it loads the saved message and logs
    it, so repeating a delivery has no side effects. A missing message (for
    example deleted through ``ON DELETE CASCADE``) is logged and treated as
    processed, because retrying cannot bring the row back.
    """

    async def handle_message_received(event: OutboxEvent) -> None:
        message_id = _saved_message_id(event)

        async with session_factory() as session:
            message = await SavedMessageRepository(session).get_by_id(message_id)

            if message is None:
                logger.warning(
                    "Saved message %s for outbox event %s no longer exists",
                    message_id,
                    event.id,
                )
                return

            logger.info("Saved message %s received: text=%r", message.id, message.text)

    return handle_message_received
