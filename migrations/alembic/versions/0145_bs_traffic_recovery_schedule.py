"""Persist opt-in read-only recovery admission and scan cursor.

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
    if sa.inspect(op.get_bind()).has_table('bs_traffic_recovery_schedules'):
        return
    op.create_table(
        'bs_traffic_recovery_schedules',
        sa.Column('panel_scope', sa.String(36), primary_key=True),
        sa.Column('cursor', sa.Integer(), nullable=False, server_default='0'),
        sa.Column('upper_id', sa.Integer()),
        sa.Column('eligible_before', sa.DateTime(timezone=True)),
        sa.Column('due_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('lease_token', sa.String(36)),
        sa.Column('lease_until', sa.DateTime(timezone=True)),
        sa.Column('operation_id', sa.Integer()),
        sa.Column('last_result', sa.String(32)),
        sa.CheckConstraint('cursor >= 0', name='ck_bs_recovery_cursor'),
        sa.CheckConstraint(
            '(upper_id IS NULL AND eligible_before IS NULL AND cursor = 0) OR '
            '(upper_id IS NOT NULL AND eligible_before IS NOT NULL AND upper_id >= cursor)',
            name='ck_bs_recovery_window',
        ),
        sa.CheckConstraint(
            '(lease_token IS NULL AND lease_until IS NULL AND operation_id IS NULL) OR '
            '(lease_token IS NOT NULL AND lease_until IS NOT NULL AND operation_id IS NOT NULL AND operation_id > 0)',
            name='ck_bs_recovery_lease',
        ),
    )


def downgrade():
    # Preserve active lease/cursor rather than silently duplicating recovery work.
    pass
