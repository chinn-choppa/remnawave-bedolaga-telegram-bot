"""Retain removed-node provenance and bounded unresolved polling tasks."""

import sqlalchemy as sa
from alembic import op

revision = '0167'
down_revision = '0166'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'bs_traffic_node_memberships',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'subscription_id',
            sa.Integer(),
            sa.ForeignKey('bs_traffic_ledgers.subscription_id', ondelete='RESTRICT'),
            nullable=False,
        ),
        sa.Column('panel_scope', sa.String(64), nullable=False),
        sa.Column('panel_user_id', sa.BigInteger(), nullable=False),
        sa.Column('account_generation', sa.String(64), nullable=False),
        sa.Column('ownership_cycle', sa.Integer(), nullable=False),
        sa.Column('node_uuid', sa.String(36), nullable=False),
        sa.Column('traffic_group', sa.String(16), nullable=False),
        sa.Column(
            'transition_receipt_id', sa.Integer(), sa.ForeignKey('bs_traffic_renewal_intents.id', ondelete='RESTRICT')
        ),
        sa.Column('source_fingerprint', sa.String(64), nullable=False),
        sa.Column('effective_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('retired_at', sa.DateTime(timezone=True)),
        sa.Column('source_paid_end_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('state', sa.String(16), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('subscription_id', 'node_uuid', name='uq_bs_node_membership'),
        sa.CheckConstraint("traffic_group IN ('ordinary', 'bs')", name='ck_bs_node_group'),
        sa.CheckConstraint("state IN ('active', 'tail', 'attention')", name='ck_bs_node_state'),
    )
    op.create_index('ix_bs_node_scope_state', 'bs_traffic_node_memberships', ['panel_scope', 'state', 'id'])
    op.create_index('ix_bs_node_subscription_state', 'bs_traffic_node_memberships', ['subscription_id', 'state'])
    op.create_table(
        'bs_traffic_node_tails',
        sa.Column(
            'membership_id',
            sa.Integer(),
            sa.ForeignKey('bs_traffic_node_memberships.id', ondelete='RESTRICT'),
            primary_key=True,
        ),
        sa.Column('day', sa.Date(), primary_key=True),
        sa.Column('state', sa.String(16), nullable=False),
        sa.Column('next_poll_at', sa.DateTime(timezone=True), nullable=False),
        sa.Column('attempts', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('last_error', sa.String(64)),
        sa.Column('job_id', sa.Integer(), sa.ForeignKey('bs_traffic_collection_jobs.id', ondelete='RESTRICT')),
        sa.CheckConstraint("state IN ('pending', 'attention')", name='ck_bs_tail_state'),
    )
    op.create_index('ix_bs_tail_due', 'bs_traffic_node_tails', ['state', 'next_poll_at', 'membership_id'])


def downgrade():
    raise RuntimeError('Historical raw ownership cannot be dropped safely')
