"""Persist inactive split access delivery attempts.

Revision ID: 0145
Revises: 0144
"""

import sqlalchemy as sa
from alembic import op

revision = '0145'
down_revision = '0144'
branch_labels = None
depends_on = None


def upgrade():
    if sa.inspect(op.get_bind()).has_table('bs_traffic_deliveries'):
        return
    op.create_table(
        'bs_traffic_deliveries',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'subscription_id',
            sa.Integer(),
            sa.ForeignKey('bs_traffic_ledgers.subscription_id', ondelete='RESTRICT'),
            nullable=False,
        ),
        sa.Column('operation_key', sa.String(160), nullable=False),
        sa.Column('revision', sa.BigInteger(), nullable=False),
        sa.Column('guard_hash', sa.String(64), nullable=False),
        sa.Column('local_guard_hash', sa.String(64), nullable=False),
        sa.Column('squads', sa.JSON(), nullable=False),
        sa.Column('state', sa.String(16), nullable=False),
        sa.Column('token', sa.String(36)),
        sa.Column('not_sent_reason', sa.String(32)),
        sa.Column('valid_until', sa.DateTime(timezone=True), nullable=False),
        sa.Column('attempted_at', sa.DateTime(timezone=True)),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('subscription_id', 'operation_key', name='uq_bs_delivery_operation'),
        sa.CheckConstraint(
            "state IN ('pending', 'attempted', 'verified', 'attention', 'cancelled', 'not_sent')",
            name='ck_bs_delivery_state',
        ),
        sa.CheckConstraint(
            "(state IN ('pending', 'cancelled') AND token IS NULL AND attempted_at IS NULL) OR "
            "(state IN ('attempted', 'verified', 'attention', 'not_sent') AND token IS NOT NULL AND attempted_at IS NOT NULL)",
            name='ck_bs_delivery_attempt',
        ),
        sa.CheckConstraint(
            "(state = 'not_sent' AND not_sent_reason IS NOT NULL AND not_sent_reason IN "
            "('stale_evidence', 'account_changed', 'policy_changed', 'selection_changed', 'provider_backoff')) OR "
            "(state <> 'not_sent' AND not_sent_reason IS NULL)",
            name='ck_bs_delivery_not_sent',
        ),
    )
    op.create_index('ix_bs_delivery_scan', 'bs_traffic_deliveries', ['state', 'id'])
    op.create_index(
        'ix_bs_delivery_recovery_scan',
        'bs_traffic_deliveries',
        ['id'],
        postgresql_where=sa.text("state IN ('attempted', 'attention')"),
    )
    op.create_index(
        'ix_bs_delivery_pending_expiry',
        'bs_traffic_deliveries',
        ['valid_until', 'id'],
        postgresql_where=sa.text("state = 'pending'"),
    )
    op.create_index(
        'uq_bs_delivery_unresolved',
        'bs_traffic_deliveries',
        ['subscription_id'],
        unique=True,
        postgresql_where=sa.text("state IN ('pending', 'attempted', 'attention')"),
    )


def downgrade():
    # Preserve evidence of potentially applied writes across application rollback.
    pass
