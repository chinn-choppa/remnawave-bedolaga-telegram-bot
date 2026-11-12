"""Add inactive split-traffic raw evidence storage, without paid balances.

Revision ID: 0141
Revises: 0140
"""

import sqlalchemy as sa
from alembic import op


revision = '0141'
down_revision = '0140'
branch_labels = None
depends_on = None


def upgrade():
    inspector = sa.inspect(op.get_bind())
    if not inspector.has_table('bs_traffic_polls'):
        op.create_table(
            'bs_traffic_polls',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column('panel_scope', sa.String(36), nullable=False),
            sa.Column('day', sa.Date(), nullable=False),
            sa.Column('node_uuids', sa.JSON(), nullable=False),
            sa.Column('identities', sa.JSON(), nullable=False),
            sa.Column('selected_accounts', sa.Integer(), nullable=False),
            sa.Column('observed_pairs', sa.Integer()),
            sa.Column('response_hash', sa.String(64)),
            sa.Column('created_at', sa.DateTime(timezone=True), server_default=sa.func.now(), nullable=False),
        )
    if not inspector.has_table('bs_traffic_observations'):
        op.create_table(
            'bs_traffic_observations',
            sa.Column('panel_scope', sa.String(36), primary_key=True),
            sa.Column('panel_user_id', sa.BigInteger(), primary_key=True),
            sa.Column('account_generation', sa.String(128), primary_key=True),
            sa.Column('node_uuid', sa.String(36), primary_key=True),
            sa.Column('day', sa.Date(), primary_key=True),
            sa.Column('high_water_bytes', sa.BigInteger(), nullable=False),
            sa.Column('latest_bytes', sa.BigInteger(), nullable=False),
            sa.Column('latest_poll_id', sa.Integer(), nullable=False),
            sa.Column('needs_review', sa.Boolean(), nullable=False),
            sa.CheckConstraint(
                'high_water_bytes >= 0 AND latest_bytes >= 0 AND latest_bytes <= high_water_bytes',
                name='ck_bs_observation_bytes',
            ),
            sa.CheckConstraint('latest_poll_id > 0 AND panel_user_id > 0', name='ck_bs_observation_identity'),
        )
        op.create_index('ix_bs_observation_day', 'bs_traffic_observations', ['day'])


def downgrade():
    # Preserve evidence: older application versions simply ignore these tables.
    # A destructive downgrade could erase data needed to reconcile paid usage.
    pass
