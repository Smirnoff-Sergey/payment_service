"""idempotency keys with TTL, index for outbox cleanup

Revision ID: 0002
Revises: 0001
Create Date: 2026-11-09
"""
from collections.abc import Sequence

import sqlalchemy as sa
from alembic import op

revision: str = "0002"
down_revision: str | None = "0001"
branch_labels: str | Sequence[str] | None = None
depends_on: str | Sequence[str] | None = None


def upgrade() -> None:
    op.create_table(
        "idempotency_keys",
        sa.Column("key", sa.String(255), primary_key=True),
        sa.Column("request_hash", sa.String(64), nullable=False),
        sa.Column(
            "payment_id",
            sa.Uuid(),
            sa.ForeignKey("payments.id", ondelete="CASCADE", deferrable=True, initially="DEFERRED"),
            nullable=False,
        ),
        sa.Column("created_at", sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        sa.Column("expires_at", sa.DateTime(timezone=True), nullable=False),
    )
    op.create_index("ix_idempotency_keys_expires_at", "idempotency_keys", ["expires_at"])
    # Existing keys keep the default 24h TTL counted from the payment creation.
    op.execute(
        """
        INSERT INTO idempotency_keys (key, request_hash, payment_id, created_at, expires_at)
        SELECT idempotency_key, request_hash, id, created_at, created_at + interval '24 hours'
        FROM payments
        """
    )
    op.drop_constraint("uq_payments_idempotency_key", "payments", type_="unique")
    op.drop_column("payments", "request_hash")

    op.create_index(
        "ix_outbox_published",
        "outbox",
        ["published_at"],
        postgresql_where=sa.text("published_at IS NOT NULL"),
    )


def downgrade() -> None:
    # Fails on the unique constraint if a key has been reused after it expired.
    op.drop_index("ix_outbox_published", table_name="outbox")
    op.add_column("payments", sa.Column("request_hash", sa.String(64), nullable=False, server_default=""))
    op.execute(
        """
        UPDATE payments p SET request_hash = k.request_hash
        FROM idempotency_keys k WHERE k.payment_id = p.id
        """
    )
    op.alter_column("payments", "request_hash", server_default=None)
    op.create_unique_constraint("uq_payments_idempotency_key", "payments", ["idempotency_key"])
    op.drop_index("ix_idempotency_keys_expires_at", table_name="idempotency_keys")
    op.drop_table("idempotency_keys")
