import uuid
from datetime import datetime

from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncSession

from memory_vault.models.saved_message import SavedMessage


class SavedMessageRepository:
    def __init__(self, session: AsyncSession) -> None:
        self.session = session

    async def create_if_absent(
        self,
        *,
        user_id: uuid.UUID,
        telegram_chat_id: int,
        telegram_message_id: int,
        telegram_update_id: int,
        text: str,
        message_date: datetime,
    ) -> SavedMessage | None:
        """Insert the message unless this Telegram update was already stored.

        The idempotency check is fused into the insert: ``ON CONFLICT`` on the
        ``telegram_update_id`` unique index makes check-and-insert atomic, so
        concurrent redeliveries of one update cannot both create a row. Returns
        ``None`` when the row already existed (the update is a duplicate).
        """
        stmt = (
            insert(SavedMessage)
            .values(
                user_id=user_id,
                telegram_chat_id=telegram_chat_id,
                telegram_message_id=telegram_message_id,
                telegram_update_id=telegram_update_id,
                text=text,
                message_date=message_date,
            )
            .on_conflict_do_nothing(
                index_elements=[SavedMessage.telegram_update_id],
            )
            .returning(SavedMessage)
        )

        result = await self.session.execute(stmt)

        return result.scalar_one_or_none()
