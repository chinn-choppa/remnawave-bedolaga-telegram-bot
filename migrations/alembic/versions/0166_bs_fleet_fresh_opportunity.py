"""Rotate finite fresh collection opportunities without extending sample validity."""

import sqlalchemy as sa
from alembic import op

revision = '0166'
down_revision = '0165'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column(
        'bs_traffic_fleet_work', sa.Column('last_fresh_attempt_at', sa.DateTime(timezone=True), nullable=True)
    )
    op.create_index(
        'ix_bs_fleet_fair',
        'bs_traffic_fleet_work',
        ['panel_scope', sa.text('last_fresh_attempt_at NULLS FIRST'), 'due_at', 'subscription_id'],
    )


def downgrade():
    op.drop_index('ix_bs_fleet_fair', table_name='bs_traffic_fleet_work')
    op.drop_column('bs_traffic_fleet_work', 'last_fresh_attempt_at')
