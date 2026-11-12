"""Retain every ownership cycle and fence late usage from newer balances."""

import sqlalchemy as sa
from alembic import op

revision = '0162'
down_revision = '0161'
branch_labels = None
depends_on = None

_TABLES = (
    'bs_traffic_ledgers',
    'bs_traffic_allowances',
    'bs_traffic_usage',
    'bs_traffic_usage_cursors',
    'bs_traffic_managed_transfers',
    'bs_traffic_cutover_intents',
    'bs_traffic_enrollments',
)


def upgrade():
    bind = op.get_bind()
    for table in _TABLES:
        columns = {c['name'] for c in sa.inspect(bind).get_columns(table)}
        if 'ownership_cycle' not in columns:
            op.add_column(table, sa.Column('ownership_cycle', sa.Integer(), nullable=False, server_default='0'))
    for table, name in [('bs_traffic_ledgers', 'ownership_started_at'), ('traffic_allowances', 'retired_at')]:
        if name not in {c['name'] for c in sa.inspect(bind).get_columns(table)}:
            op.add_column(table, sa.Column(name, sa.DateTime(timezone=True), nullable=True))
    if 'bs_traffic_ownership_cycles' not in sa.inspect(bind).get_table_names():
        op.create_table(
            'bs_traffic_ownership_cycles',
            sa.Column(
                'subscription_id',
                sa.Integer(),
                sa.ForeignKey('bs_traffic_ledgers.subscription_id', ondelete='RESTRICT'),
                primary_key=True,
            ),
            sa.Column('ownership_cycle', sa.Integer(), primary_key=True),
            sa.Column('state', sa.String(24), nullable=False),
            sa.Column('forward_receipt', sa.JSON(), nullable=False),
            sa.Column('reverse_manifest', sa.JSON(), nullable=True),
            sa.Column('reverse_attempt', sa.JSON(), nullable=True),
            sa.Column('cursor_archive', sa.JSON(), nullable=True),
            sa.Column('managed_at', sa.DateTime(timezone=True), nullable=True),
            sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
            sa.CheckConstraint('ownership_cycle >= 0', name='ck_bs_ownership_cycle_number'),
            sa.CheckConstraint("state IN ('split', 'reverse_pending', 'managed')", name='ck_bs_ownership_cycle_state'),
        )
    enrollment_unique = {c['name'] for c in sa.inspect(bind).get_unique_constraints('bs_traffic_enrollments')}
    if 'uq_bs_enrollment_subscription' in enrollment_unique:
        op.drop_constraint('uq_bs_enrollment_subscription', 'bs_traffic_enrollments', type_='unique')
    if 'uq_bs_enrollment_subscription_cycle' not in enrollment_unique:
        op.create_unique_constraint(
            'uq_bs_enrollment_subscription_cycle', 'bs_traffic_enrollments', ['subscription_id', 'ownership_cycle']
        )
    indexes = {i['name']: i for i in sa.inspect(bind).get_indexes('bs_traffic_cutover_intents')}
    if indexes.get('uq_bs_cutover_live', {}).get('column_names') != ['subscription_id', 'ownership_cycle']:
        if 'uq_bs_cutover_live' in indexes:
            op.drop_index('uq_bs_cutover_live', table_name='bs_traffic_cutover_intents')
        op.create_index(
            'uq_bs_cutover_live',
            'bs_traffic_cutover_intents',
            ['subscription_id', 'ownership_cycle'],
            unique=True,
            postgresql_where=sa.text("state NOT IN ('cancelled', 'not_sent')"),
        )


def downgrade():
    raise RuntimeError('Ownership cycle history cannot be discarded by downgrade')
