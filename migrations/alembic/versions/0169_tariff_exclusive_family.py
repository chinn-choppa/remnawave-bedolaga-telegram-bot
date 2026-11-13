"""Взаимное исключение живых тарифных подписок одного семейства.

Revision ID: 0169
Revises: 0168
"""

import sqlalchemy as sa
from alembic import op

from app.database.tariff_family_schema import DROP_TRIGGER_SQL, TRIGGER_SQL


revision = '0169'
down_revision = '0168'
branch_labels = None
depends_on = None


def upgrade():
    if op.get_bind().dialect.name != 'postgresql':
        raise RuntimeError('Tariff-family unique enforcement requires PostgreSQL')
    op.add_column('tariffs', sa.Column('exclusive_family', sa.String(length=64), nullable=True))
    op.add_column('subscriptions', sa.Column('exclusive_family', sa.String(length=64), nullable=True))
    op.create_check_constraint(
        'ck_tariffs_exclusive_family_slug',
        'tariffs',
        "exclusive_family IS NULL OR exclusive_family ~ '^[a-z][a-z0-9_-]{0,63}$'",
    )
    op.create_index(
        'uq_subscriptions_user_exclusive_family_live',
        'subscriptions',
        ['user_id', 'exclusive_family'],
        unique=True,
        postgresql_where=sa.text("exclusive_family IS NOT NULL AND status IN ('active', 'trial', 'limited')"),
    )
    for ddl in TRIGGER_SQL:
        op.execute(sa.text(ddl))


def downgrade():
    for ddl in DROP_TRIGGER_SQL:
        op.execute(sa.text(ddl))
    op.drop_index('uq_subscriptions_user_exclusive_family_live', table_name='subscriptions')
    op.drop_constraint('ck_tariffs_exclusive_family_slug', 'tariffs', type_='check')
    op.drop_column('subscriptions', 'exclusive_family')
    op.drop_column('tariffs', 'exclusive_family')
