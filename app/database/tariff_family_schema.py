"""Триггеры PostgreSQL для mutually exclusive tariff families."""

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



# SQLite не позволяет присваивать NEW.field в BEFORE trigger, поэтому
# AFTER trigger обновляет текущую строку; unique partial index отвергает
# конфликт в той же исходной транзакции.
SQLITE_TRIGGER_SQL = (
    """
    CREATE TRIGGER IF NOT EXISTS tg_subscriptions_family_insert_sqlite
    AFTER INSERT ON subscriptions FOR EACH ROW
    WHEN NEW.exclusive_family IS NOT
         (SELECT exclusive_family FROM tariffs WHERE id = NEW.tariff_id)
    BEGIN
        UPDATE subscriptions
        SET exclusive_family = (SELECT exclusive_family FROM tariffs WHERE id = NEW.tariff_id)
        WHERE id = NEW.id;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS tg_subscriptions_family_update_sqlite
    AFTER UPDATE OF user_id, tariff_id, status, exclusive_family
    ON subscriptions FOR EACH ROW
    WHEN NEW.exclusive_family IS NOT
         (SELECT exclusive_family FROM tariffs WHERE id = NEW.tariff_id)
    BEGIN
        UPDATE subscriptions
        SET exclusive_family = (SELECT exclusive_family FROM tariffs WHERE id = NEW.tariff_id)
        WHERE id = NEW.id;
    END
    """,
    """
    CREATE TRIGGER IF NOT EXISTS tg_tariffs_family_update_sqlite
    AFTER UPDATE OF exclusive_family ON tariffs FOR EACH ROW
    WHEN OLD.exclusive_family IS NOT NEW.exclusive_family
    BEGIN
        UPDATE subscriptions
        SET exclusive_family = NEW.exclusive_family
        WHERE tariff_id = NEW.id
          AND exclusive_family IS NOT NEW.exclusive_family;
    END
    """,
)

DROP_SQLITE_TRIGGER_SQL = (
    'DROP TRIGGER IF EXISTS tg_tariffs_family_update_sqlite',
    'DROP TRIGGER IF EXISTS tg_subscriptions_family_update_sqlite',
    'DROP TRIGGER IF EXISTS tg_subscriptions_family_insert_sqlite',
)
