"""Persist one-shot managed cutover attempts without enabling a sender."""

import sqlalchemy as sa
from alembic import op

revision = '0150'
down_revision = '0149'
branch_labels = None
depends_on = None


def upgrade():
    if sa.inspect(op.get_bind()).has_table('bs_traffic_cutover_intents'):
        _ensure_scan_index()
        return
    op.create_table(
        'bs_traffic_cutover_intents',
        sa.Column('id', sa.Integer(), primary_key=True),
        sa.Column(
            'subscription_id',
            sa.Integer(),
            sa.ForeignKey('bs_traffic_managed_transfers.subscription_id', ondelete='RESTRICT'),
            nullable=False,
        ),
        sa.Column('operation_key', sa.String(160), nullable=False),
        sa.Column('ledger_revision', sa.BigInteger(), nullable=False),
        sa.Column('panel_scope', sa.String(36), nullable=False),
        sa.Column('account_generation', sa.String(64), nullable=False),
        sa.Column('target', sa.JSON(), nullable=False),
        sa.Column('expected_source', sa.JSON(), nullable=False),
        sa.Column('target_hash', sa.String(64), nullable=False),
        sa.Column('source_hash', sa.String(64), nullable=False),
        sa.Column('guard_hash', sa.String(64), nullable=False),
        sa.Column('valid_until', sa.DateTime(timezone=True), nullable=False),
        sa.Column('state', sa.String(16), nullable=False),
        sa.Column('activated_at', sa.DateTime(timezone=True)),
        sa.Column('not_sent_reason', sa.String(32)),
        sa.Column('attempt_token', sa.String(36)),
        sa.Column('attempted_at', sa.DateTime(timezone=True)),
        sa.Column('send_deadline', sa.DateTime(timezone=True)),
        sa.Column('created_at', sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()),
        sa.UniqueConstraint('subscription_id', 'operation_key', name='uq_bs_cutover_operation'),
        sa.CheckConstraint(
            "state IN ('pending', 'attempted', 'verified', 'cancelled', 'not_sent', 'activated')",
            name='ck_bs_cutover_state',
        ),
        sa.CheckConstraint(
            "(state IN ('pending', 'cancelled') AND attempt_token IS NULL AND attempted_at IS NULL AND send_deadline IS NULL) OR "
            "(state IN ('attempted', 'verified', 'not_sent', 'activated') AND attempt_token IS NOT NULL AND attempted_at IS NOT NULL AND send_deadline IS NOT NULL)",
            name='ck_bs_cutover_attempt',
        ),
        sa.CheckConstraint(
            "(state = 'not_sent' AND not_sent_reason IS NOT NULL) OR (state <> 'not_sent' AND not_sent_reason IS NULL)",
            name='ck_bs_cutover_not_sent',
        ),
        sa.CheckConstraint(
            "(state = 'activated' AND activated_at IS NOT NULL) OR (state <> 'activated' AND activated_at IS NULL)",
            name='ck_bs_cutover_activated',
        ),
        sa.CheckConstraint('ledger_revision >= 0', name='ck_bs_cutover_revision'),
    )
    op.create_index(
        'uq_bs_cutover_live',
        'bs_traffic_cutover_intents',
        ['subscription_id'],
        unique=True,
        postgresql_where=sa.text("state NOT IN ('cancelled', 'not_sent')"),
    )
    _ensure_scan_index()


def _ensure_scan_index():
    if 'ix_bs_cutover_scan' not in {
        index['name'] for index in sa.inspect(op.get_bind()).get_indexes('bs_traffic_cutover_intents')
    }:
        op.create_index(
            'ix_bs_cutover_scan',
            'bs_traffic_cutover_intents',
            ['panel_scope', 'id'],
            postgresql_where=sa.text("state IN ('pending', 'attempted', 'verified')"),
        )


def downgrade():
    # Preserve attempts: dropping these records can cause a duplicate PATCH.
    pass
