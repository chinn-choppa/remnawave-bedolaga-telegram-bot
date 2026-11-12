"""Distinguish known-unsent renewals covered by a newer verified receipt."""

from alembic import op


revision = '0159'
down_revision = '0158'
branch_labels = None
depends_on = None


def _constraints(*, superseded):
    states = "'pending', 'attempted', 'verified', 'superseded'" if superseded else "'pending', 'attempted', 'verified'"
    unsent = "state IN ('pending', 'superseded')" if superseded else "state = 'pending'"
    op.create_check_constraint('ck_bs_renewal_state', 'bs_traffic_renewal_intents', f'state IN ({states})')
    op.create_check_constraint(
        'ck_bs_renewal_attempt',
        'bs_traffic_renewal_intents',
        f'({unsent} AND attempt_token IS NULL AND attempted_at IS NULL AND send_deadline IS NULL) OR '
        "(state IN ('attempted', 'verified') AND attempt_token IS NOT NULL AND attempted_at IS NOT NULL "
        'AND send_deadline IS NOT NULL AND target IS NOT NULL AND target_hash IS NOT NULL '
        'AND expected_source IS NOT NULL AND source_hash IS NOT NULL AND ledger_revision IS NOT NULL '
        'AND guard_hash IS NOT NULL AND valid_until IS NOT NULL)',
    )


def upgrade():
    op.drop_constraint('ck_bs_renewal_state', 'bs_traffic_renewal_intents', type_='check')
    op.drop_constraint('ck_bs_renewal_attempt', 'bs_traffic_renewal_intents', type_='check')
    _constraints(superseded=True)


def downgrade():
    op.drop_constraint('ck_bs_renewal_state', 'bs_traffic_renewal_intents', type_='check')
    op.drop_constraint('ck_bs_renewal_attempt', 'bs_traffic_renewal_intents', type_='check')
    # Old versions may scan these rows again, but cannot send their stale expiry.
    op.execute("UPDATE bs_traffic_renewal_intents SET state = 'pending' WHERE state = 'superseded'")
    _constraints(superseded=False)
