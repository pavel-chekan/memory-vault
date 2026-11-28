import uuid
from enum import StrEnum

from sqlalchemy import (
    Enum,
    ForeignKey,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import Mapped, mapped_column
from sqlalchemy.types import Uuid

from memory_vault.models.base import Base, TimestampMixin


class KnowledgeSourceType(StrEnum):
    TELEGRAM_MESSAGE = "TELEGRAM_MESSAGE"


class KnowledgeItemStatus(StrEnum):
    PENDING = "PENDING"
    PROCESSING = "PROCESSING"
    PROCESSED = "PROCESSED"
    FAILED = "FAILED"


class KnowledgeItem(TimestampMixin, Base):
    __tablename__ = "knowledge_items"

    __table_args__ = (
        UniqueConstraint(
            "source_type",
            "source_id",
            name="uq_knowledge_items_source_type_source_id",
        ),
    )

    id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        primary_key=True,
        default=uuid.uuid4,
        nullable=False,
    )

    user_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("users.id", ondelete="CASCADE"),
        nullable=False,
    )

    source_type: Mapped[KnowledgeSourceType] = mapped_column(
        Enum(
            KnowledgeSourceType,
            name="knowledge_item_source_type",
            native_enum=False,
            create_constraint=True,
            length=32,
            values_callable=lambda enum: [member.value for member in enum],
        ),
        nullable=False,
        default=KnowledgeSourceType.TELEGRAM_MESSAGE,
        server_default=KnowledgeSourceType.TELEGRAM_MESSAGE.value,
    )

    source_id: Mapped[uuid.UUID] = mapped_column(
        Uuid(as_uuid=True),
        ForeignKey("saved_messages.id", ondelete="CASCADE"),
        nullable=False,
    )

    title: Mapped[str | None] = mapped_column(
        String(255),
        nullable=True,
    )

    content: Mapped[str] = mapped_column(
        Text,
        nullable=False,
    )

    # Caller contract: lowercase 64-character hex SHA-256 of `content`
    # (the schema does not enforce it; normalization is the caller's responsibility).
    content_hash: Mapped[str] = mapped_column(
        String(64),
        nullable=False,
    )

    status: Mapped[KnowledgeItemStatus] = mapped_column(
        Enum(
            KnowledgeItemStatus,
            name="knowledge_item_status",
            native_enum=False,
            create_constraint=True,
            length=16,
            values_callable=lambda enum: [member.value for member in enum],
        ),
        nullable=False,
        default=KnowledgeItemStatus.PENDING,
        server_default=KnowledgeItemStatus.PENDING.value,
    )
