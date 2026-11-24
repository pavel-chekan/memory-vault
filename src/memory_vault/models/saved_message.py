import uuid
from datetime import datetime

from sqlalchemy import (
    BigInteger,
    DateTime,
    ForeignKey,
    Index,
    Text,
    UniqueConstraint,
    Uuid,
    func,
)
from sqlalchemy.orm import Mapped, mapped_column

from memory_vault.models.base import Base


class SavedMessage(Base):
    __tablename__ = "saved_messages"

    __table_args__ = (
        UniqueConstraint(
            "telegram_update_id",
            name="uq_saved_messages_telegram_update_id",
        ),
        UniqueConstraint(
            "telegram_chat_id",
            "telegram_message_id",
            name="uq_saved_messages_chat_message",
        ),
        Index(
            "ix_saved_messages_user_id_message_date",
            "user_id",
            "message_date",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )

    telegram_chat_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telegram_message_id: Mapped[int] = mapped_column(BigInteger, nullable=False)
    telegram_update_id: Mapped[int] = mapped_column(BigInteger, nullable=False)

    text: Mapped[str] = mapped_column(Text, nullable=False)

    message_date: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        nullable=False,
    )

    created_at: Mapped[datetime] = mapped_column(
        DateTime(timezone=True),
        server_default=func.now(),
        nullable=False,
    )
