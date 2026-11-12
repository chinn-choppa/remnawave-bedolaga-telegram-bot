"""Add inactive PostgreSQL raw-usage collection queue.

Revision ID: 0142
Revises: 0141
"""

import sqlalchemy as sa
from alembic import op


revision = '0142'
down_revision = '0141'
branch_labels = None
depends_on = None


def upgrade():
    if not sa.inspect(op.get_bind()).has_table('bs_traffic_collection_budgets'):
        op.create_table(
            'bs_traffic_collection_budgets',
            sa.Column('panel_scope', sa.String(36), primary_key=True),
            sa.Column('cooldown_until', sa.DateTime(timezone=True)),
            sa.Column('slot_token', sa.String(36)),
            sa.Column('slot_until', sa.DateTime(timezone=True)),
            sa.CheckConstraint(
                '(slot_token IS NULL AND slot_until IS NULL) OR (slot_token IS NOT NULL AND slot_until IS NOT NULL)',
                name='ck_bs_collection_slot',
            ),
        )
    if sa.inspect(op.get_bind()).has_table('bs_traffic_collection_jobs'):
        return
    op.create_table(
        'bs_traffic_collection_jobs',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column('panel_scope', sa.String(36), nullable=False),
        sa.Column('operation_key', sa.String(160), nullable=False),
        sa.Column('day', sa.Date(), nullable=False),
        sa.Column('node_uuids', sa.JSON(), nullable=False),
        sa.Column('identities', sa.JSON(), nullable=False),
        sa.Column('selected_accounts', sa.Integer(), nullable=False),
        sa.Column('request_hash', sa.String(64), nullable=False),
        sa.Column('state', sa.String(16), nullable=False),
        sa.Column('due_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('attempts', sa.Integer(), nullable=False),
        sa.Column('lease_token', sa.String(36)),
        sa.Column('lease_until', sa.DateTime(timezone=True)),
        sa.Column('poll_id', sa.Integer(), sa.ForeignKey('bs_traffic_polls.id', ondelete='RESTRICT')),
        sa.Column('last_error', sa.String(64)),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('panel_scope', 'operation_key', name='uq_bs_collection_operation'),
        sa.CheckConstraint('attempts >= 0', name='ck_bs_collection_attempts'),
        sa.CheckConstraint(
            "state IN ('pending', 'leased', 'complete', 'attention', 'cancelled')", name='ck_bs_collection_state'
        ),
        sa.CheckConstraint(
            "(state = 'leased' AND lease_token IS NOT NULL AND lease_until IS NOT NULL) OR "
            "(state <> 'leased' AND lease_token IS NULL AND lease_until IS NULL)",
            name='ck_bs_collection_lease',
        ),
    )
    op.create_index('ix_bs_collection_due', 'bs_traffic_collection_jobs', ['panel_scope', 'state', 'due_at', 'id'])


def downgrade():
    # Keep diagnostic/unfinished work; an older binary must not silently erase it.
    pass
