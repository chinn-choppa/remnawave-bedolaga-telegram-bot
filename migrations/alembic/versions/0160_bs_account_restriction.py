"""Retain ambiguous whole-account revocations before issuing one panel write."""

import sqlalchemy as sa
from alembic import op


revision = '0160'
down_revision = '0159'
branch_labels = None
depends_on = None


def upgrade():
    columns = {c['name'] for c in sa.inspect(op.get_bind()).get_columns('bs_traffic_ledgers')}
    if 'restriction_attempt' not in columns:
        op.add_column('bs_traffic_ledgers', sa.Column('restriction_attempt', sa.JSON(), nullable=True))


def downgrade():
    bind = op.get_bind()
    columns = {c['name'] for c in sa.inspect(bind).get_columns('bs_traffic_ledgers')}
    if 'restriction_attempt' not in columns:
        return
    if bind.execute(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM bs_traffic_ledgers WHERE restriction_attempt IS NOT NULL AND restriction_attempt::text != 'null')"
        )
    ).scalar():
        raise RuntimeError('Account restriction history must be retained')
    op.drop_column('bs_traffic_ledgers', 'restriction_attempt')
