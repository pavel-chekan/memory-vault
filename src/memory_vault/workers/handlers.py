from sqlalchemy.ext.asyncio import AsyncSession, async_sessionmaker

from memory_vault.services.saved_messages import MESSAGE_RECEIVED_EVENT_TYPE
from memory_vault.workers.message_received import create_message_received_handler
from memory_vault.workers.outbox_worker import OutboxEventHandler


def create_outbox_handlers(
    session_factory: async_sessionmaker[AsyncSession],
) -> dict[str, OutboxEventHandler]:
    """Return the ``event_type`` -> handler registry for the outbox worker.

    Every registered handler must follow the ``OutboxEventHandler`` contract:
    be idempotent, propagate ``asyncio.CancelledError``, and avoid blocking the
    event loop (a blocked loop also blocks the lease heartbeat).
    """
    return {
        MESSAGE_RECEIVED_EVENT_TYPE: create_message_received_handler(session_factory),
    }
