"""Persist explicit split panel epochs without storing credentials or URLs.

Revision ID: 0146
Revises: 0145
"""

import sqlalchemy as sa
from alembic import op

revision = '0146'
down_revision = '0145'
branch_labels = None
depends_on = None


def upgrade():
    if sa.inspect(op.get_bind()).has_table('bs_traffic_panel_bindings'):
        return
    op.create_table(
        'bs_traffic_panel_bindings',
        sa.Column('panel_scope', sa.String(36), primary_key=True),
        sa.Column('endpoint_hash', sa.String(64), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.Column('retired_at', sa.DateTime(timezone=True)),
        sa.CheckConstraint('length(endpoint_hash) = 64', name='ck_bs_panel_endpoint_hash'),
    )
    op.create_index(
        'uq_bs_panel_active_endpoint',
        'bs_traffic_panel_bindings',
        ['endpoint_hash'],
        unique=True,
        postgresql_where=sa.text('retired_at IS NULL'),
    )


def downgrade():
    # Removing a retired epoch would allow old requests to be enrolled again.
    pass
