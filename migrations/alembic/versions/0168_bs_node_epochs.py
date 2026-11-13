"""Preserve repeated node membership epochs and entry counter provenance."""

import sqlalchemy as sa
from alembic import op

revision = '0168'
down_revision = '0167'
branch_labels = None
depends_on = None


def upgrade():
    op.drop_constraint('ck_bs_node_state', 'bs_traffic_node_memberships', type_='check')
    op.create_check_constraint(
        'ck_bs_node_state', 'bs_traffic_node_memberships', "state IN ('active', 'tail', 'attention', 'archived')"
    )
    op.drop_constraint('uq_bs_node_membership', 'bs_traffic_node_memberships', type_='unique')
    op.add_column('bs_traffic_node_memberships', sa.Column('entry_evidence', sa.JSON(), nullable=True))
    op.create_index(
        'uq_bs_node_active_membership',
        'bs_traffic_node_memberships',
        ['subscription_id', 'node_uuid'],
        unique=True,
        postgresql_where=sa.text("state = 'active'"),
    )


def downgrade():
    raise RuntimeError('Historical node epochs cannot be collapsed safely')
