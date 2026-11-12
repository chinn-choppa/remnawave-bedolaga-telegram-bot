"""Persist immutable full split grace restoration targets.

Revision ID: 0147
Revises: 0146
"""

import sqlalchemy as sa
from alembic import op

revision = '0147'
down_revision = '0146'
branch_labels = None
depends_on = None


def upgrade():
    if sa.inspect(op.get_bind()).has_table('bs_traffic_grace_intents'):
        return
    op.create_table(
        'bs_traffic_grace_intents',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'subscription_id', sa.Integer(), sa.ForeignKey('subscriptions.id', ondelete='RESTRICT'), nullable=False
        ),
        sa.Column(
            'grace_id', sa.String(36), sa.ForeignKey('grace_access_sessions.id', ondelete='RESTRICT'), nullable=False
        ),
        sa.Column('grace_version', sa.Integer(), nullable=False),
        sa.Column('ledger_revision', sa.BigInteger(), nullable=False),
        sa.Column('panel_scope', sa.String(36), nullable=False),
        sa.Column('account_generation', sa.String(64), nullable=False),
        sa.Column('target', sa.JSON(), nullable=False),
        sa.Column('expected_source', sa.JSON(), nullable=False),
        sa.Column('source_hash', sa.String(64), nullable=False),
        sa.Column('target_hash', sa.String(64), nullable=False),
        sa.Column('guard_hash', sa.String(64), nullable=False),
        sa.Column('state', sa.String(16), nullable=False),
        sa.Column('attempt_token', sa.String(36)),
        sa.Column('verified_at', sa.DateTime(timezone=True)),
        sa.Column('attempted_at', sa.DateTime(timezone=True)),
        sa.Column('send_deadline', sa.DateTime(timezone=True)),
        sa.Column('valid_until', sa.DateTime(timezone=True), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint(
            "state IN ('pending', 'attempted', 'verified', 'attention', 'cancelled')", name='ck_bs_grace_intent_state'
        ),
        sa.CheckConstraint(
            "(state IN ('pending', 'cancelled') AND attempt_token IS NULL AND attempted_at IS NULL AND send_deadline IS NULL) OR "
            "(state <> 'pending' AND attempt_token IS NOT NULL AND attempted_at IS NOT NULL AND send_deadline IS NOT NULL)",
            name='ck_bs_grace_intent_attempt',
        ),
        sa.CheckConstraint(
            "(state = 'verified' AND verified_at IS NOT NULL) OR (state <> 'verified' AND verified_at IS NULL)",
            name='ck_bs_grace_intent_verified',
        ),
        sa.CheckConstraint('grace_version > 0 AND ledger_revision >= 0', name='ck_bs_grace_intent_versions'),
    )

    op.create_index(
        'uq_bs_grace_intent_session',
        'bs_traffic_grace_intents',
        ['grace_id'],
        unique=True,
        postgresql_where=sa.text("state <> 'cancelled'"),
    )

    op.create_index('ix_bs_grace_intent_subscription', 'bs_traffic_grace_intents', ['subscription_id'])


def downgrade():
    # Preserve evidence of possible external attempts rather than enabling replay.
    pass
