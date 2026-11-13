"""SQLite fallback: тот же Main XOR через триггеры без PostgreSQL."""

from datetime import UTC, datetime, timedelta

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from app.database.models import Base, Subscription, Tariff, User
from app.database.tariff_family_schema import SQLITE_TRIGGER_SQL
from tests.fixtures.sqlite_memory import memory_session


async def seed(db):
    for ddl in SQLITE_TRIGGER_SQL:
        await db.execute(text(ddl))
    db.add(User(id=1, telegram_id=70001, language='ru'))
    db.add_all(
        [
            Tariff(id=10, name='Basic', exclusive_family='main'),
            Tariff(id=11, name='Premium', exclusive_family='main'),
            Tariff(id=20, name='CDN', exclusive_family='cdn'),
        ]
    )
    await db.commit()


def subscription(tariff_id, panel_id, *, status='active'):
    return Subscription(
        user_id=1,
        tariff_id=tariff_id,
        remnawave_id=panel_id,
        status=status,
        is_trial=False,
        end_date=datetime.now(UTC) + timedelta(days=30),
    )


async def test_sqlite_main_xor_and_addon_allowed(monkeypatch):
    async with memory_session(monkeypatch, list(Base.metadata.sorted_tables)) as db:
        await seed(db)
        db.add_all([subscription(10, 101), subscription(20, 202)])
        await db.commit()
        items = (await db.scalars(select(Subscription).order_by(Subscription.tariff_id))).all()
        assert [(x.exclusive_family, x.remnawave_id) for x in items] == [('main', 101), ('cdn', 202)]
        db.add(subscription(11, 303))
        with pytest.raises(IntegrityError):
            await db.flush()
        await db.rollback()
        assert len((await db.scalars(select(Subscription))).all()) == 2


async def test_sqlite_main_switch_and_manual_null_spoof(monkeypatch):
    async with memory_session(monkeypatch, list(Base.metadata.sorted_tables)) as db:
        await seed(db)
        main = subscription(10, 101)
        db.add(main)
        await db.commit()
        main.tariff_id = 11
        await db.commit()
        await db.refresh(main)
        assert main.exclusive_family == 'main' and main.remnawave_id == 101
        await db.execute(text('UPDATE subscriptions SET exclusive_family = NULL WHERE id = :id'), {'id': main.id})
        await db.refresh(main)
        assert main.exclusive_family == 'main'


async def test_sqlite_admin_family_reassignment_cannot_duplicate(monkeypatch):
    async with memory_session(monkeypatch, list(Base.metadata.sorted_tables)) as db:
        await seed(db)
        db.add_all([subscription(10, 101), subscription(20, 202)])
        await db.commit()
        addon_tariff = await db.get(Tariff, 20)
        addon_tariff.exclusive_family = 'main'
        with pytest.raises(IntegrityError):
            await db.flush()
        await db.rollback()
        rows = (await db.scalars(select(Subscription).order_by(Subscription.tariff_id))).all()
        assert [x.exclusive_family for x in rows] == ['main', 'cdn']
