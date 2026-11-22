"""create outbox_events table

Revision ID: b8f3c2a1d4e6
Revises: a937129aa57e
Create Date: 2026-11-21 12:00:00.000000

"""

from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects import postgresql

# revision identifiers, used by Alembic.
revision: str = "b8f3c2a1d4e6"
down_revision: str | Sequence[str] | None = "a937129aa57e"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    """Upgrade schema."""
    op.create_table(
        "outbox_events",
        sa.Column(
            "id",
            sa.Uuid(),
            server_default=sa.text("gen_random_uuid()"),
            nullable=False,
        ),
        sa.Column("event_type", sa.String(length=128), nullable=False),
        sa.Column("aggregate_type", sa.String(length=64), nullable=False),
        sa.Column("aggregate_id", sa.Uuid(), nullable=False),
        sa.Column(
            "payload",
            postgresql.JSONB(astext_type=sa.Text()),
            nullable=False,
        ),
        sa.Column(
            "status",
            sa.String(length=16),
            server_default=sa.text("'PENDING'"),
            nullable=False,
        ),
        sa.Column(
            "attempt_count",
            sa.Integer(),
            server_default=sa.text("0"),
            nullable=False,
        ),
        sa.Column(
            "available_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.Column("locked_by", sa.String(length=128), nullable=True),
        sa.Column("lease_token", sa.Uuid(), nullable=True),
        sa.Column(
            "lease_expires_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column(
            "processed_at",
            sa.DateTime(timezone=True),
            nullable=True,
        ),
        sa.Column("last_error", sa.Text(), nullable=True),
        sa.Column(
            "created_at",
            sa.DateTime(timezone=True),
            server_default=sa.text("now()"),
            nullable=False,
        ),
        sa.CheckConstraint(
            "attempt_count >= 0",
            name=op.f("ck_outbox_events_attempt_count_non_negative"),
        ),
        sa.CheckConstraint(
            """
            (
                status = 'PROCESSING'
                AND locked_by IS NOT NULL
                AND lease_token IS NOT NULL
                AND lease_expires_at IS NOT NULL
                AND processed_at IS NULL
            )
            OR
            (
                status = 'PROCESSED'
                AND locked_by IS NULL
                AND lease_token IS NULL
                AND lease_expires_at IS NULL
                AND processed_at IS NOT NULL
            )
            OR
            (
                status IN ('PENDING', 'FAILED')
                AND locked_by IS NULL
                AND lease_token IS NULL
                AND lease_expires_at IS NULL
                AND processed_at IS NULL
            )
            """,
            name=op.f("ck_outbox_events_state_consistency"),
        ),
        sa.CheckConstraint(
            "status IN ('PENDING', 'PROCESSING', 'PROCESSED', 'FAILED')",
            name=op.f("ck_outbox_events_outbox_event_status"),
        ),
        sa.PrimaryKeyConstraint("id", name=op.f("pk_outbox_events")),
    )
    op.create_index(
        "ix_outbox_events_pending",
        "outbox_events",
        ["available_at", "created_at"],
        unique=False,
        postgresql_where=sa.text("status = 'PENDING'"),
    )
    op.create_index(
        "ix_outbox_events_processing",
        "outbox_events",
        ["lease_expires_at"],
        unique=False,
        postgresql_where=sa.text("status = 'PROCESSING'"),
    )


def downgrade() -> None:
    """Downgrade schema."""
    op.drop_index(
        "ix_outbox_events_processing",
        table_name="outbox_events",
    )
    op.drop_index(
        "ix_outbox_events_pending",
        table_name="outbox_events",
    )
    op.drop_table("outbox_events")
