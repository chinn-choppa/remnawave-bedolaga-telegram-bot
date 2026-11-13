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
TRIGGER_SQL = (
    """
    CREATE OR REPLACE FUNCTION sync_subscription_exclusive_family() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        IF NEW.tariff_id IS NULL THEN
            NEW.exclusive_family := NULL;
        ELSE
            SELECT exclusive_family INTO NEW.exclusive_family
              FROM tariffs WHERE id = NEW.tariff_id FOR SHARE;
        END IF;
        RETURN NEW;
    END
    $$
    """,
    """
    CREATE TRIGGER tg_subscriptions_exclusive_family
    BEFORE INSERT OR UPDATE OF tariff_id, user_id, status, exclusive_family
    ON subscriptions FOR EACH ROW
    EXECUTE FUNCTION sync_subscription_exclusive_family()
    """,
    """
    CREATE OR REPLACE FUNCTION propagate_tariff_exclusive_family() RETURNS trigger
    LANGUAGE plpgsql AS $$
    BEGIN
        UPDATE subscriptions
           SET exclusive_family = NEW.exclusive_family
         WHERE tariff_id = NEW.id
           AND exclusive_family IS DISTINCT FROM NEW.exclusive_family;
        RETURN NULL;
    END
    $$
    """,
    """
    CREATE TRIGGER tg_tariffs_exclusive_family
    AFTER UPDATE OF exclusive_family ON tariffs
    FOR EACH ROW
    WHEN (OLD.exclusive_family IS DISTINCT FROM NEW.exclusive_family)
    EXECUTE FUNCTION propagate_tariff_exclusive_family()
    """,
)

DROP_TRIGGER_SQL = (
    'DROP TRIGGER IF EXISTS tg_tariffs_exclusive_family ON tariffs',
    'DROP FUNCTION IF EXISTS propagate_tariff_exclusive_family()',
    'DROP TRIGGER IF EXISTS tg_subscriptions_exclusive_family ON subscriptions',
    'DROP FUNCTION IF EXISTS sync_subscription_exclusive_family()',
)


def upgrade():
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
    for statement in TRIGGER_SQL:
        op.execute(sa.text(statement))


def downgrade():
    for statement in DROP_TRIGGER_SQL:
        op.execute(sa.text(statement))
    op.drop_index('uq_subscriptions_user_exclusive_family_live', table_name='subscriptions')
    op.drop_constraint('ck_tariffs_exclusive_family_slug', 'tariffs', type_='check')
    op.drop_column('subscriptions', 'exclusive_family')
    op.drop_column('tariffs', 'exclusive_family')
