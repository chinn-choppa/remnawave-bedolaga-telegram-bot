"""Persist split overlay attempts and explicit restoration purposes."""

import sqlalchemy as sa
from alembic import op

revision = '0159'
down_revision = '0158'
branch_labels = None
depends_on = None


def upgrade():
    connection = op.get_bind()
    if 'split_overlay_attempt' not in {c['name'] for c in sa.inspect(connection).get_columns('grace_access_sessions')}:
        op.add_column('grace_access_sessions', sa.Column('split_overlay_attempt', sa.JSON(), nullable=True))
    if 'completion_reason' not in {c['name'] for c in sa.inspect(connection).get_columns('bs_traffic_grace_intents')}:
        op.add_column(
            'bs_traffic_grace_intents',
            sa.Column('completion_reason', sa.String(16), nullable=False, server_default='paid'),
        )

    if 'ck_bs_grace_intent_completion_reason' not in {
        c['name'] for c in sa.inspect(connection).get_check_constraints('bs_traffic_grace_intents')
    }:
        op.create_check_constraint(
            'ck_bs_grace_intent_completion_reason',
            'bs_traffic_grace_intents',
            "completion_reason IN ('paid', 'timeout', 'revoked', 'drained', 'conflict')",
        )


def downgrade():
    connection = op.get_bind()
    if connection.scalar(
        sa.text(
            "SELECT EXISTS (SELECT 1 FROM grace_access_sessions WHERE split_overlay_attempt IS NOT NULL AND split_overlay_attempt::text <> 'null')"
        )
    ) or connection.scalar(
        sa.text("SELECT EXISTS (SELECT 1 FROM bs_traffic_grace_intents WHERE completion_reason <> 'paid')")
    ):
        raise RuntimeError('Split grace attempt and completion history must be retained')
    op.drop_constraint('ck_bs_grace_intent_completion_reason', 'bs_traffic_grace_intents', type_='check')
    op.drop_column('grace_access_sessions', 'split_overlay_attempt')
    op.drop_column('bs_traffic_grace_intents', 'completion_reason')
