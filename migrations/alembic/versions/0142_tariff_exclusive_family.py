"""Взаимное исключение живых подписок одной тарифной семьи.

Revision ID: 0142
Revises: 0141
"""

import sqlalchemy as sa
from alembic import op


revision = '0142'
down_revision = '0141'
branch_labels = None
depends_on = None

# Индекс обеспечивает межтранзакционную уникальность даже при параллельных
# покупках. Триггеры не позволяют entrypoint обойти индекс, не заполнив ключ.
from app.database.tariff_family_schema import (
    DROP_SQLITE_TRIGGER_SQL,
    DROP_TRIGGER_SQL,
    SQLITE_TRIGGER_SQL,
    TRIGGER_SQL,
)


def upgrade():
    op.add_column('tariffs', sa.Column('exclusive_family', sa.String(length=64), nullable=True))
    op.add_column('subscriptions', sa.Column('exclusive_family', sa.String(length=64), nullable=True))
    if op.get_bind().dialect.name == 'postgresql':
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
        sqlite_where=sa.text("exclusive_family IS NOT NULL AND status IN ('active', 'trial', 'limited')"),
    )
    trigger_sql = TRIGGER_SQL if op.get_bind().dialect.name == 'postgresql' else SQLITE_TRIGGER_SQL
    for statement in trigger_sql:
        op.execute(sa.text(statement))


def downgrade():
    is_postgres = op.get_bind().dialect.name == 'postgresql'
    for statement in DROP_TRIGGER_SQL if is_postgres else DROP_SQLITE_TRIGGER_SQL:
        op.execute(sa.text(statement))
    op.drop_index('uq_subscriptions_user_exclusive_family_live', table_name='subscriptions')
    if is_postgres:
        op.drop_constraint('ck_tariffs_exclusive_family_slug', 'tariffs', type_='check')
    op.drop_column('subscriptions', 'exclusive_family')
    op.drop_column('tariffs', 'exclusive_family')
