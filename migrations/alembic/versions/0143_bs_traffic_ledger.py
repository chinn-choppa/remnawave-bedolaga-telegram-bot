"""Add inactive split grants and pending settlement records.

Revision ID: 0143
Revises: 0142
"""

import sqlalchemy as sa
from alembic import op


revision = '0143'
down_revision = '0142'
branch_labels = None
depends_on = None


def upgrade():
    if not sa.inspect(op.get_bind()).has_table('bs_traffic_ledgers'):
        op.create_table(
            'bs_traffic_ledgers',
            sa.Column(
                'subscription_id',
                sa.Integer(),
                sa.ForeignKey('subscriptions.id', ondelete='RESTRICT'),
                primary_key=True,
            ),
            sa.Column('panel_scope', sa.String(36), nullable=False),
            sa.Column('panel_user_id', sa.BigInteger(), nullable=False),
            sa.Column('account_generation', sa.String(64), nullable=False),
            sa.Column('revision', sa.BigInteger(), nullable=False),
            sa.Column('access_policy', sa.JSON()),
        )
    if not sa.inspect(op.get_bind()).has_table('bs_traffic_allowances'):
        op.create_table(
            'bs_traffic_allowances',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column(
                'subscription_id',
                sa.Integer(),
                sa.ForeignKey('bs_traffic_ledgers.subscription_id', ondelete='RESTRICT'),
                nullable=False,
            ),
            sa.Column('traffic_group', sa.String(16), nullable=False),
            sa.Column('operation_key', sa.String(160), nullable=False),
            sa.Column('granted_bytes', sa.BigInteger(), nullable=False),
            sa.Column('remaining_bytes', sa.BigInteger(), nullable=False),
            sa.Column('starts_at', sa.DateTime(timezone=True), nullable=False),
            sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint('subscription_id', 'traffic_group', 'operation_key', name='uq_bs_allowance_operation'),
            sa.CheckConstraint("traffic_group IN ('ordinary', 'bs')", name='ck_bs_allowance_group'),
            sa.CheckConstraint(
                'granted_bytes > 0 AND remaining_bytes >= 0 AND remaining_bytes <= granted_bytes',
                name='ck_bs_allowance_balance',
            ),
            sa.CheckConstraint('expires_at > starts_at', name='ck_bs_allowance_interval'),
        )
        op.create_index(
            'ix_bs_allowance_balance', 'bs_traffic_allowances', ['subscription_id', 'traffic_group', 'expires_at']
        )
    if not sa.inspect(op.get_bind()).has_table('bs_traffic_usage'):
        op.create_table(
            'bs_traffic_usage',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column(
                'subscription_id',
                sa.Integer(),
                sa.ForeignKey('bs_traffic_ledgers.subscription_id', ondelete='RESTRICT'),
                nullable=False,
            ),
            sa.Column('traffic_group', sa.String(16), nullable=False),
            sa.Column('source_key', sa.String(160), nullable=False),
            sa.Column('request_hash', sa.String(64), nullable=False),
            sa.Column('account_generation', sa.String(64), nullable=False),
            sa.Column('interval_start', sa.DateTime(timezone=True), nullable=False),
            sa.Column('evidence_kind', sa.String(16), nullable=False),
            sa.Column('interval_end', sa.DateTime(timezone=True), nullable=False),
            sa.Column('amount_bytes', sa.BigInteger(), nullable=False),
            sa.Column('eligible_grants', sa.JSON(), nullable=False),
            sa.Column('allocations', sa.JSON(), nullable=False),
            sa.Column('state', sa.String(16), nullable=False),
            sa.Column('charged_bytes', sa.BigInteger(), nullable=False),
            sa.Column('unallocated_bytes', sa.BigInteger(), nullable=False),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.Column('resolved_at', sa.DateTime(timezone=True)),
            sa.UniqueConstraint('subscription_id', 'source_key', name='uq_bs_usage_source'),
            sa.CheckConstraint("traffic_group IN ('ordinary', 'bs')", name='ck_bs_usage_group'),
            sa.CheckConstraint("state IN ('pending', 'charged', 'written_off')", name='ck_bs_usage_state'),
            sa.CheckConstraint(
                "(state = 'pending' AND resolved_at IS NULL) OR (state <> 'pending' AND resolved_at IS NOT NULL)",
                name='ck_bs_usage_resolution_time',
            ),
            sa.CheckConstraint("state <> 'written_off' OR charged_bytes = 0", name='ck_bs_usage_write_off'),
            sa.CheckConstraint(
                "evidence_kind IN ('classified', 'raw_pending') AND "
                "(evidence_kind <> 'raw_pending' OR state <> 'charged')",
                name='ck_bs_usage_evidence',
            ),
            sa.CheckConstraint('interval_end > interval_start', name='ck_bs_usage_interval'),
            sa.CheckConstraint(
                'amount_bytes >= 0 AND charged_bytes >= 0 AND unallocated_bytes >= 0 AND '
                "((state = 'pending' AND charged_bytes = 0 AND unallocated_bytes = 0) OR "
                "(state <> 'pending' AND charged_bytes + unallocated_bytes = amount_bytes))",
                name='ck_bs_usage_conservation',
            ),
        )
        op.create_index('ix_bs_usage_pending', 'bs_traffic_usage', ['subscription_id', 'state'])
    if not sa.inspect(op.get_bind()).has_table('bs_traffic_usage_cursors'):
        op.create_table(
            'bs_traffic_usage_cursors',
            sa.Column(
                'subscription_id',
                sa.Integer(),
                sa.ForeignKey('bs_traffic_ledgers.subscription_id', ondelete='RESTRICT'),
                primary_key=True,
            ),
            sa.Column('node_uuid', sa.String(36), primary_key=True),
            sa.Column('day', sa.Date(), primary_key=True),
            sa.Column('account_generation', sa.String(64), nullable=False),
            sa.Column('traffic_group', sa.String(16), nullable=False),
            sa.Column('baseline_bytes', sa.BigInteger(), nullable=False),
            sa.Column('processed_bytes', sa.BigInteger(), nullable=False),
            sa.Column('needs_review', sa.Boolean(), nullable=False),
            sa.CheckConstraint("traffic_group IN ('ordinary', 'bs')", name='ck_bs_cursor_group'),
            sa.CheckConstraint('processed_bytes >= baseline_bytes AND baseline_bytes >= 0', name='ck_bs_cursor_bytes'),
        )


def downgrade():
    # Retain grant provenance and pending usage; never erase financial evidence.
    pass
