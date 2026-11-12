"""Retain consumed poll boundaries without inventing history for old cursors."""

import sqlalchemy as sa
from alembic import op


revision = '0152'
down_revision = '0151'
branch_labels = None
depends_on = None


def upgrade():
    columns = {c['name'] for c in sa.inspect(op.get_bind()).get_columns('bs_traffic_usage_cursors')}
    if 'last_poll_started_at' not in columns:
        op.add_column(
            'bs_traffic_usage_cursors', sa.Column('last_poll_started_at', sa.DateTime(timezone=True), nullable=True)
        )


def downgrade():
    if op.get_bind().scalar(
        sa.text('SELECT EXISTS (SELECT 1 FROM bs_traffic_usage_cursors WHERE last_poll_started_at IS NOT NULL)')
    ):
        raise RuntimeError('Consumed poll boundary retention required; downgrade refused')
    op.drop_column('bs_traffic_usage_cursors', 'last_poll_started_at')
