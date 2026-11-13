"""Keep no-reset base renewals on a shared historical balance."""

import sqlalchemy as sa
from alembic import op

revision = '0156'
down_revision = '0155'
branch_labels = None
depends_on = None


def upgrade():
    if not sa.inspect(op.get_bind()).has_table('bs_traffic_base_extensions'):
        op.create_table(
            'bs_traffic_base_extensions',
            sa.Column('id', sa.Integer(), primary_key=True),
            sa.Column(
                'subscription_id', sa.Integer(), sa.ForeignKey('subscriptions.id', ondelete='RESTRICT'), nullable=False
            ),
            sa.Column(
                'allowance_id',
                sa.Integer(),
                sa.ForeignKey('bs_traffic_allowances.id', ondelete='RESTRICT'),
                nullable=False,
            ),
            sa.Column('traffic_group', sa.String(16), nullable=False),
            sa.Column('operation_key', sa.String(160), nullable=False),
            sa.Column('request_hash', sa.String(64), nullable=False),
            sa.Column('requested_at', sa.DateTime(timezone=True), nullable=False),
            sa.Column('starts_at', sa.DateTime(timezone=True), nullable=False),
            sa.Column('expires_at', sa.DateTime(timezone=True), nullable=False),
            sa.UniqueConstraint(
                'subscription_id', 'traffic_group', 'operation_key', name='uq_bs_base_extension_operation'
            ),
            sa.CheckConstraint("traffic_group IN ('ordinary', 'bs')", name='ck_bs_base_extension_group'),
            sa.CheckConstraint('starts_at < expires_at', name='ck_bs_base_extension_interval'),
        )
        op.create_index('ix_bs_base_extension_allowance', 'bs_traffic_base_extensions', ['allowance_id', 'expires_at'])


def downgrade():
    if op.get_bind().scalar(sa.text('SELECT EXISTS (SELECT 1 FROM bs_traffic_base_extensions)')):
        raise RuntimeError('Paid base extensions retain usage rights; downgrade refused')
    op.drop_table('bs_traffic_base_extensions')
