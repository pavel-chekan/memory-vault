"""create saved_messages table

Revision ID: 00546e6740fa
Revises: b8f3c2a1d4e6
Create Date: 2026-11-24 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "00546e6740fa"
down_revision: str | Sequence[str] | None = "b8f3c2a1d4e6"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "saved_messages",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column("telegram_chat_id", sa.BigInteger(), nullable=False),
        sa.Column("telegram_message_id", sa.BigInteger(), nullable=False),
        sa.Column("telegram_update_id", sa.BigInteger(), nullable=False),
        sa.Column("text", sa.Text(), nullable=False),
        sa.Column("message_date", sa.DateTime(timezone=True), nullable=False),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_saved_messages_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_saved_messages")),
        sa.UniqueConstraint(
            "telegram_update_id",
            name=op.f("uq_saved_messages_telegram_update_id"),
        ),
        sa.UniqueConstraint(
            "telegram_chat_id",
            "telegram_message_id",
            name=op.f("uq_saved_messages_chat_message"),
        ),
    )
    op.create_index(
        "ix_saved_messages_user_id_message_date",
        "saved_messages",
        ["user_id", "message_date"],
        unique=False,
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_saved_messages_user_id_message_date",
        table_name="saved_messages",
    )
    op.drop_table("saved_messages")
