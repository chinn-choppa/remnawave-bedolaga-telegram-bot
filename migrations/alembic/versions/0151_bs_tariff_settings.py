"""Store separate BS tariff terms without changing existing tariffs."""

import sqlalchemy as sa
from alembic import op


revision = '0151'
down_revision = '0150'
branch_labels = None
depends_on = None


def upgrade():
    if 'bs_traffic_settings' not in {c['name'] for c in sa.inspect(op.get_bind()).get_columns('tariffs')}:
        op.add_column('tariffs', sa.Column('bs_traffic_settings', sa.JSON(none_as_null=True), nullable=True))


def downgrade():
    if op.get_bind().scalar(sa.text('SELECT EXISTS (SELECT 1 FROM tariffs WHERE bs_traffic_settings IS NOT NULL)')):
        raise RuntimeError('BS tariff settings must be preserved; downgrade refused')
    op.drop_column('tariffs', 'bs_traffic_settings')
