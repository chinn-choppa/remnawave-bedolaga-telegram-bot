"""Persist split transfer receipts while keeping the original accounting history."""

import sqlalchemy as sa
from alembic import op

revision = '0149'
down_revision = '0148'
branch_labels = None
depends_on = None


def upgrade():
    if sa.inspect(op.get_bind()).has_table('bs_traffic_managed_transfers'):
        return
    op.create_table(
        'bs_traffic_managed_transfers',
        sa.Column(
            'subscription_id',
            sa.Integer(),
            sa.ForeignKey('bs_traffic_ledgers.subscription_id', ondelete='RESTRICT'),
            primary_key=True,
        ),
        sa.Column('operation_key', sa.String(160), nullable=False),
        sa.Column('account_generation', sa.String(64), nullable=False),
        sa.Column('panel_scope', sa.String(36), nullable=False),
        sa.Column('manifest_hash', sa.String(64), nullable=False),
        sa.Column('destination_group', sa.String(16), nullable=False),
        sa.Column('source_identity', sa.String(128), nullable=False),
        sa.Column('source_phase', sa.String(32), nullable=False),
        sa.Column('source_revision', sa.BigInteger(), nullable=False),
        sa.Column('source_lifetime_bytes', sa.BigInteger(), nullable=False),
        sa.Column('source_used_bytes', sa.BigInteger(), nullable=False),
        sa.Column('imported_bytes', sa.BigInteger(), nullable=False),
        sa.Column('grants', sa.JSON(), nullable=False),
        sa.Column('uncertain_policy', sa.String(32), nullable=False),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.CheckConstraint("destination_group IN ('ordinary', 'bs')", name='ck_bs_transfer_group'),
        sa.CheckConstraint('imported_bytes >= 0 AND source_revision >= 0', name='ck_bs_transfer_amount'),
        sa.CheckConstraint("uncertain_policy = 'operator_write_off'", name='ck_bs_transfer_uncertain'),
    )


def downgrade():
    # Dropping receipts would permit double imports after a rollback.
    pass
