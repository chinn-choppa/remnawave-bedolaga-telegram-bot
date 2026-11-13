"""Retain typed absent-account coverage without inventing raw counters."""

import sqlalchemy as sa
from alembic import op

revision = '0165'
down_revision = '0164'
branch_labels = None
depends_on = None


def upgrade():
    op.add_column('bs_traffic_polls', sa.Column('response_nodes', sa.JSON(none_as_null=True), nullable=True))
    op.add_column(
        'bs_traffic_usage_cursors', sa.Column('bootstrap_evidence', sa.JSON(none_as_null=True), nullable=True)
    )


def downgrade():
    if (
        op.get_bind()
        .execute(sa.text('SELECT EXISTS (SELECT 1 FROM bs_traffic_usage_cursors WHERE bootstrap_evidence IS NOT NULL)'))
        .scalar()
    ):
        raise RuntimeError('Explicit omitted baseline provenance must be retained')
    op.drop_column('bs_traffic_usage_cursors', 'bootstrap_evidence')

    op.drop_column('bs_traffic_polls', 'response_nodes')
