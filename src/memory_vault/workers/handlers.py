from memory_vault.workers.outbox_worker import OutboxEventHandler


def create_outbox_handlers() -> dict[str, OutboxEventHandler]:
    """Return the ``event_type`` -> handler registry for the outbox worker.

    Handlers are registered here as event types are introduced. The registry is
    empty until the first real handler exists, which is why the worker is
    disabled by default (``OUTBOX_WORKER_ENABLED=false``).

    Every registered handler must follow the ``OutboxEventHandler`` contract:
    be idempotent, propagate ``asyncio.CancelledError``, and avoid blocking the
    event loop (a blocked loop also blocks the lease heartbeat).
    """
    return {}
