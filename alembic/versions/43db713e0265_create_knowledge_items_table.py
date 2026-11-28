"""create knowledge_items table

Revision ID: 43db713e0265
Revises: 00546e6740fa
Create Date: 2026-11-27 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

# revision identifiers, used by Alembic.
revision: str = "43db713e0265"
down_revision: str | Sequence[str] | None = "00546e6740fa"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "knowledge_items",
        sa.Column("id", sa.Uuid(), nullable=False),
        sa.Column("user_id", sa.Uuid(), nullable=False),
        sa.Column(
            "source_type",
            sa.String(length=32),
            server_default=sa.text("'TELEGRAM_MESSAGE'"),
            nullable=False,
        ),
        sa.Column("source_id", sa.Uuid(), nullable=False),
        sa.Column("title", sa.String(length=255), nullable=True),
        sa.Column("content", sa.Text(), nullable=False),
        sa.Column("content_hash", sa.String(length=64), nullable=False),
        sa.Column(
            "status",
            sa.String(length=16),
            server_default=sa.text("'PENDING'"),
            nullable=False,
        ),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column(
            "updated_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.ForeignKeyConstraint(
            ["user_id"],
            ["users.id"],
            name=op.f("fk_knowledge_items_user_id_users"),
            ondelete="CASCADE",
        ),
        sa.ForeignKeyConstraint(
            ["source_id"],
            ["saved_messages.id"],
            name=op.f("fk_knowledge_items_source_id_saved_messages"),
            ondelete="CASCADE",
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_knowledge_items")),
        sa.UniqueConstraint(
            "source_type",
            "source_id",
            name=op.f("uq_knowledge_items_source_type_source_id"),
        ),
        sa.CheckConstraint(
            "source_type IN ('TELEGRAM_MESSAGE')",
            name=op.f("ck_knowledge_items_knowledge_item_source_type"),
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'PROCESSED', 'FAILED')",
            name=op.f("ck_knowledge_items_knowledge_item_status"),
        ),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_table("knowledge_items")
