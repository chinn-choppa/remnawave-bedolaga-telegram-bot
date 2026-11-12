"""Retain verified managed-transfer calendar terms without inventing payments."""

import sqlalchemy as sa
from alembic import op

revision = '0156'
down_revision = '0155'
branch_labels = None
depends_on = None


def upgrade():
    columns = {column['name'] for column in sa.inspect(op.get_bind()).get_columns('bs_traffic_managed_transfers')}
    if 'calendar_terms' not in columns:
        op.add_column('bs_traffic_managed_transfers', sa.Column('calendar_terms', sa.JSON(), nullable=True))


def downgrade():
    if op.get_bind().scalar(
        sa.text(
            'SELECT EXISTS (SELECT 1 FROM bs_traffic_managed_transfers WHERE calendar_terms IS NOT NULL '
            "AND calendar_terms::text <> 'null')"
        )
    ):
        raise RuntimeError('Transferred paid calendar history must be retained')
    op.drop_column('bs_traffic_managed_transfers', 'calendar_terms')
