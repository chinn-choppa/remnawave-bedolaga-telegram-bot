"""Durable bounded node-shared fleet rounds and local work scheduling."""

import sqlalchemy as sa
from alembic import op

revision = '0157'
down_revision = '0156'
branch_labels = None
depends_on = None


def upgrade():
    existing = set(sa.inspect(op.get_bind()).get_table_names())
    if 'bs_traffic_fleet_rounds' not in existing:
        op.create_table(
            'bs_traffic_fleet_rounds',
            sa.Column('panel_scope', sa.String(36), primary_key=True),
            sa.Column('round_id', sa.String(36), nullable=False, unique=True),
            sa.Column('day', sa.Date(), nullable=False),
            sa.Column('upper_id', sa.Integer(), nullable=False),
            sa.Column('cursor', sa.Integer(), nullable=False),
            sa.Column('phase', sa.String(16), nullable=False),
            sa.Column('started_at', sa.DateTime(timezone=True), nullable=False),
            sa.Column('completed_at', sa.DateTime(timezone=True)),
            sa.Column('last_success_at', sa.DateTime(timezone=True)),
            sa.Column('error_count', sa.Integer(), nullable=False),
            sa.CheckConstraint(
                "phase IN ('building', 'collecting', 'settling', 'complete', 'attention')", name='ck_bs_fleet_phase'
            ),
            sa.CheckConstraint('cursor >= 0 AND upper_id >= cursor AND error_count >= 0', name='ck_bs_fleet_bounds'),
        )
    if 'bs_traffic_fleet_members' not in existing:
        op.create_table(
            'bs_traffic_fleet_members',
            sa.Column(
                'round_id',
                sa.String(36),
                sa.ForeignKey('bs_traffic_fleet_rounds.round_id', ondelete='CASCADE'),
                primary_key=True,
            ),
            sa.Column('subscription_id', sa.Integer(), primary_key=True),
            sa.Column('user_id', sa.Integer(), nullable=False),
            sa.Column('panel_user_id', sa.BigInteger(), nullable=False),
            sa.Column('account_generation', sa.String(64), nullable=False),
            sa.Column('nodes', sa.JSON(), nullable=False),
        )
    if 'bs_traffic_fleet_nodes' not in existing:
        op.create_table(
            'bs_traffic_fleet_nodes',
            sa.Column(
                'round_id',
                sa.String(36),
                sa.ForeignKey('bs_traffic_fleet_rounds.round_id', ondelete='CASCADE'),
                primary_key=True,
            ),
            sa.Column('node_uuid', sa.String(36), primary_key=True),
            sa.Column('job_id', sa.Integer(), sa.ForeignKey('bs_traffic_collection_jobs.id', ondelete='RESTRICT')),
        )
    if 'bs_traffic_fleet_work' not in existing:
        op.create_table(
            'bs_traffic_fleet_work',
            sa.Column(
                'subscription_id', sa.Integer(), sa.ForeignKey('subscriptions.id', ondelete='CASCADE'), primary_key=True
            ),
            sa.Column('panel_scope', sa.String(36), nullable=False),
            sa.Column('account_generation', sa.String(64), nullable=False),
            sa.Column('target_round', sa.String(36), nullable=False),
            sa.Column('completed_round', sa.String(36)),
            sa.Column('due_at', sa.DateTime(timezone=True), nullable=False),
            sa.Column('settled_at', sa.DateTime(timezone=True)),
            sa.Column('settled_revision', sa.BigInteger()),
            sa.Column('error', sa.String(32)),
            sa.Column('applied_squads', sa.JSON()),
            sa.Column('access_dirty', sa.Boolean(), nullable=False),
            sa.Column('calendar_due_at', sa.DateTime(timezone=True)),
            sa.Column('calendar_receipt_id', sa.Integer()),
            sa.Column('claim_token', sa.String(36)),
            sa.Column('lease_until', sa.DateTime(timezone=True)),
        )
        op.create_index('ix_bs_fleet_due', 'bs_traffic_fleet_work', ['panel_scope', 'due_at', 'subscription_id'])
    if 'bs_traffic_fleet_lanes' not in existing:
        op.create_table(
            'bs_traffic_fleet_lanes',
            sa.Column('panel_scope', sa.String(36), primary_key=True),
            sa.Column('kind', sa.String(16), primary_key=True),
            sa.Column('cursor', sa.Integer(), nullable=False),
            sa.Column('upper_id', sa.Integer(), nullable=False),
            sa.Column('due_at', sa.DateTime(timezone=True), nullable=False),
            sa.Column('completed_at', sa.DateTime(timezone=True)),
            sa.Column('lease_until', sa.DateTime(timezone=True)),
            sa.Column('lease_token', sa.String(36)),
        )


def downgrade():
    for name in (
        'bs_traffic_fleet_work',
        'bs_traffic_fleet_members',
        'bs_traffic_fleet_nodes',
        'bs_traffic_fleet_rounds',
    ):
        if op.get_bind().scalar(sa.text(f'SELECT EXISTS (SELECT 1 FROM {name})')):
            raise RuntimeError('Drain persisted fleet work before removing scheduling state')
    for name in (
        'bs_traffic_fleet_lanes',
        'bs_traffic_fleet_work',
        'bs_traffic_fleet_nodes',
        'bs_traffic_fleet_members',
        'bs_traffic_fleet_rounds',
    ):
        op.drop_table(name)
