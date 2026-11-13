"""Preserve old split base balances for delayed pre-renewal usage."""

import sqlalchemy as sa
from alembic import op

revision = '0154'
down_revision = '0153'
branch_labels = None
depends_on = None


def upgrade():
    columns = {c['name'] for c in sa.inspect(op.get_bind()).get_columns('bs_traffic_allowances')}
    if 'retired_at' not in columns:
        op.add_column('bs_traffic_allowances', sa.Column('retired_at', sa.DateTime(timezone=True), nullable=True))


def downgrade():
    if op.get_bind().scalar(
        sa.text('SELECT EXISTS (SELECT 1 FROM bs_traffic_allowances WHERE retired_at IS NOT NULL)')
    ):
        raise RuntimeError('Retired split bases retain historical usage rights; downgrade refused')
    op.drop_column('bs_traffic_allowances', 'retired_at')
