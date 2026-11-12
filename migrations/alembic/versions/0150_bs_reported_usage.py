"""Distinguish approximate reported usage from classified consumption."""

import sqlalchemy as sa
from alembic import op


revision = '0150'
down_revision = '0149'
branch_labels = None
depends_on = None


def _replace(kinds):
    op.drop_constraint('ck_bs_usage_evidence', 'bs_traffic_usage', type_='check')
    op.create_check_constraint(
        'ck_bs_usage_evidence',
        'bs_traffic_usage',
        f"evidence_kind IN ({kinds}) AND (evidence_kind <> 'raw_pending' OR state <> 'charged')",
    )


def upgrade():
    _replace("'classified', 'reported', 'raw_pending'")


def downgrade():
    if op.get_bind().scalar(sa.text("SELECT EXISTS (SELECT 1 FROM bs_traffic_usage WHERE evidence_kind='reported')")):
        raise RuntimeError('Reported accounting records require retention; downgrade refused')
    _replace("'classified', 'raw_pending'")
