"""Persist opt-in connection drop attempts separately from access writes."""

import sqlalchemy as sa
from alembic import op

revision = '0164'
down_revision = '0163'
branch_labels = None
depends_on = None


def upgrade():
    columns = {c['name'] for c in sa.inspect(op.get_bind()).get_columns('bs_traffic_deliveries')}
    if 'connection_drop' not in columns:
        op.add_column('bs_traffic_deliveries', sa.Column('connection_drop', sa.JSON(), nullable=True))
    indexes = {i['name'] for i in sa.inspect(op.get_bind()).get_indexes('bs_traffic_deliveries')}
    if 'ix_bs_delivery_drop_pending' not in indexes:
        op.create_index(
            'ix_bs_delivery_drop_pending',
            'bs_traffic_deliveries',
            ['id'],
            postgresql_where=sa.text("state = 'verified' AND connection_drop->>'state' IN ('pending', 'attempted')"),
        )


def downgrade():
    bind = op.get_bind()
    columns = {c['name'] for c in sa.inspect(bind).get_columns('bs_traffic_deliveries')}
    if 'connection_drop' not in columns:
        return
    if bind.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM bs_traffic_deliveries WHERE connection_drop IS NOT NULL AND connection_drop::text != 'null')"
        )
    ).scalar():
        raise RuntimeError('Connection drop history must be retained')
    op.drop_column('bs_traffic_deliveries', 'connection_drop')
