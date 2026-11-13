"""Durable paid split renewal receipts and single-attempt delivery."""

import sqlalchemy as sa
from alembic import op

revision = '0155'
down_revision = '0154'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'bs_traffic_renewal_intents',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'subscription_id', sa.Integer(), sa.ForeignKey('subscriptions.id', ondelete='RESTRICT'), nullable=False
        ),
        sa.Column('operation_key', sa.String(160), nullable=False),
        sa.Column('transaction_id', sa.Integer(), sa.ForeignKey('transactions.id', ondelete='RESTRICT'), nullable=True),
        sa.Column('charged_kopeks', sa.Integer(), nullable=False),
        sa.Column('parameters', sa.JSON(), nullable=False),
        sa.Column('attempt_history', sa.JSON(), nullable=False, server_default=sa.text("'[]'")),
        sa.Column('panel_scope', sa.String(36), nullable=False),
        sa.Column('account_generation', sa.String(64), nullable=False),
        sa.Column('intended_expires_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('state', sa.String(16), nullable=False),
        sa.Column('target', sa.JSON()),
        sa.Column('expected_source', sa.JSON()),
        sa.Column('target_hash', sa.String(64)),
        sa.Column('source_hash', sa.String(64)),
        sa.Column('ledger_revision', sa.BigInteger()),
        sa.Column('guard_hash', sa.String(64)),
        sa.Column('valid_until', sa.DateTime(timezone=True)),
        sa.Column('attempt_token', sa.String(36)),
        sa.Column('attempted_at', sa.DateTime(timezone=True)),
        sa.Column('send_deadline', sa.DateTime(timezone=True)),
        sa.Column('verified_at', sa.DateTime(timezone=True)),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('subscription_id', 'operation_key', name='uq_bs_renewal_operation'),
        sa.CheckConstraint("state IN ('pending', 'attempted', 'verified')", name='ck_bs_renewal_state'),
        sa.CheckConstraint('charged_kopeks >= 0', name='ck_bs_renewal_charge'),
        sa.CheckConstraint(
            "(state = 'pending' AND attempt_token IS NULL AND attempted_at IS NULL AND send_deadline IS NULL) OR "
            "(state <> 'pending' AND attempt_token IS NOT NULL AND attempted_at IS NOT NULL AND send_deadline IS NOT NULL "
            'AND target IS NOT NULL AND target_hash IS NOT NULL AND expected_source IS NOT NULL '
            'AND source_hash IS NOT NULL AND ledger_revision IS NOT NULL AND guard_hash IS NOT NULL '
            'AND valid_until IS NOT NULL)',
            name='ck_bs_renewal_attempt',
        ),
        sa.CheckConstraint(
            "(state = 'verified' AND verified_at IS NOT NULL) OR (state <> 'verified' AND verified_at IS NULL)",
            name='ck_bs_renewal_verified',
        ),
    )
    op.create_index('ix_bs_traffic_renewal_intents_subscription_id', 'bs_traffic_renewal_intents', ['subscription_id'])


def downgrade():
    if op.get_bind().scalar(sa.text('SELECT EXISTS (SELECT 1 FROM bs_traffic_renewal_intents)')):
        raise RuntimeError('Paid renewal receipts must be retained; downgrade refused')
    op.drop_table('bs_traffic_renewal_intents')
