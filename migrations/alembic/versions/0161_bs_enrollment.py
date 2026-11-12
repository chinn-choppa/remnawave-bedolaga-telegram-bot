"""Freeze funded initial BS terms before asynchronous account enrollment."""

import sqlalchemy as sa
from alembic import op


revision = '0161'
down_revision = '0160'
branch_labels = None
depends_on = None


def upgrade():
    op.create_table(
        'bs_traffic_enrollments',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'subscription_id', sa.Integer(), sa.ForeignKey('subscriptions.id', ondelete='RESTRICT'), nullable=False
        ),
        sa.Column('user_id', sa.Integer(), sa.ForeignKey('users.id', ondelete='RESTRICT'), nullable=False),
        sa.Column('tariff_id', sa.Integer(), sa.ForeignKey('tariffs.id', ondelete='RESTRICT'), nullable=False),
        sa.Column('source_kind', sa.String(16), nullable=False),
        sa.Column('transaction_id', sa.Integer(), sa.ForeignKey('transactions.id', ondelete='RESTRICT')),
        sa.Column('actor_id', sa.Integer(), sa.ForeignKey('users.id', ondelete='RESTRICT')),
        sa.Column('guest_purchase_id', sa.Integer(), sa.ForeignKey('guest_purchases.id', ondelete='RESTRICT')),
        sa.Column('terms', sa.JSON(), nullable=False),
        sa.Column('terms_hash', sa.String(64), nullable=False),
        sa.Column('state', sa.String(16), nullable=False),
        sa.Column('panel_scope', sa.String(36)),
        sa.Column('account_generation', sa.String(64)),
        sa.Column('cutover_id', sa.Integer(), sa.ForeignKey('bs_traffic_cutover_intents.id', ondelete='RESTRICT')),
        sa.Column('delivery_id', sa.Integer(), sa.ForeignKey('bs_traffic_deliveries.id', ondelete='RESTRICT')),
        sa.Column('calendar_terms', sa.JSON()),
        sa.Column('verified_at', sa.DateTime(timezone=True)),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('subscription_id', name='uq_bs_enrollment_subscription'),
        sa.CheckConstraint("source_kind IN ('payment', 'admin', 'guest')", name='ck_bs_enrollment_source'),
        sa.CheckConstraint(
            "state IN ('pending', 'provisioned', 'transferred', 'activated', 'seeded', 'verified')",
            name='ck_bs_enrollment_state',
        ),
        sa.CheckConstraint(
            "(source_kind = 'payment' AND transaction_id IS NOT NULL) OR (source_kind = 'admin' AND actor_id IS NOT NULL) OR (source_kind = 'guest' AND guest_purchase_id IS NOT NULL)",
            name='ck_bs_enrollment_provenance',
        ),
    )

    op.create_index(
        'ix_bs_enrollment_pending_scope_age',
        'bs_traffic_enrollments',
        ['panel_scope', 'created_at'],
        postgresql_where=sa.text("state != 'verified'"),
    )


def downgrade():
    if op.get_bind().scalar(sa.text('SELECT EXISTS (SELECT 1 FROM bs_traffic_enrollments)')):
        raise RuntimeError('Cannot remove funded enrollment receipts')
    op.drop_table('bs_traffic_enrollments')
