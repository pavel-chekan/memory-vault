from sqlalchemy.ext.asyncio import AsyncSession

from memory_vault.models.saved_message import SavedMessage
from memory_vault.repositories.outbox import OutboxRepository
from memory_vault.repositories.saved_messages import SavedMessageRepository
from memory_vault.repositories.users import UserRepository
from memory_vault.schemas.messages import TelegramMessageCreate

MESSAGE_RECEIVED_EVENT_TYPE = "message.received"
SAVED_MESSAGE_AGGREGATE_TYPE = "saved_message"


class SavedMessageService:
    """Persist an incoming text message and its outbox event atomically.

    This service owns the unit of work: the sender upsert, the message insert
    and the outbox event are committed in one transaction. The message insert
    and the event therefore either both exist or neither does, and a repeated
    delivery of the same Telegram update cannot create a second row or event.

    Callers must pass a session that is not already inside an explicit
    transaction; the service begins and commits its own. Because the stored
    instance is returned after that commit, the caller's session factory must
    use ``expire_on_commit=False``, as the application factory does.
    """

    def __init__(self, session: AsyncSession) -> None:
        self.session = session
        self.messages = SavedMessageRepository(session)
        self.users = UserRepository(session)
        self.outbox = OutboxRepository(session)

    async def save_incoming_message(
        self,
        data: TelegramMessageCreate,
    ) -> SavedMessage | None:
        """Store one incoming text message and enqueue ``message.received``.

        Returns the stored message, or ``None`` when the update was already
        processed (no message and no event is created then).
        """
        async with self.session.begin():
            # The sender must exist before the insert because ``user_id`` is
            # NOT NULL, so a redelivered update still runs this upsert. A
            # SELECT-first existence check would reintroduce a TOCTOU race with
            # concurrent deliveries; the redundant upsert is the safer trade.
            user = await self.users.upsert(data.user)

            message = await self.messages.create_if_absent(
                user_id=user.id,
                telegram_chat_id=data.telegram_chat_id,
                telegram_message_id=data.telegram_message_id,
                telegram_update_id=data.telegram_update_id,
                text=data.text,
                message_date=data.message_date,
            )

            if message is None:
                return None

            await self.outbox.create(
                event_type=MESSAGE_RECEIVED_EVENT_TYPE,
                aggregate_type=SAVED_MESSAGE_AGGREGATE_TYPE,
                aggregate_id=message.id,
                payload={"saved_message_id": str(message.id)},
            )

            return message
