"""Проверка Main XOR на реальном PostgreSQL, включая гонку двух transactions."""

import asyncio
from datetime import UTC, datetime, timedelta
from importlib import import_module

import pytest
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from app.database.models import Base, Subscription, Tariff, User
from tests.fixtures.postgres_db import postgres_sessions, wait_for_lock_waiter


pytestmark = pytest.mark.postgres
TABLES = list(Base.metadata.sorted_tables)
DDL = import_module('migrations.alembic.versions.0142_tariff_exclusive_family').TRIGGER_SQL


async def prepare(db):
    # create_all в тестовой БД создаёт индексы, но не триггеры миграции.
    await db.execute(text('DROP TRIGGER IF EXISTS tg_tariffs_exclusive_family ON tariffs'))
    await db.execute(text('DROP TRIGGER IF EXISTS tg_subscriptions_exclusive_family ON subscriptions'))
    for statement in DDL:
        await db.execute(text(statement))
    db.add(User(id=1, telegram_id=70001, language='ru'))
    db.add_all(
        [
            Tariff(id=10, name='Basic', exclusive_family='main', period_prices={'30': 1000}),
            Tariff(id=11, name='Standard', exclusive_family='main', period_prices={'30': 2000}),
            Tariff(id=12, name='Premium', exclusive_family='main', period_prices={'30': 3000}),
            Tariff(id=20, name='CDN', exclusive_family='cdn', period_prices={'30': 500}),
            Tariff(id=30, name='Independent', exclusive_family=None, period_prices={'30': 500}),
        ]
    )
    await db.commit()


def subscription(tariff_id, *, status='active', panel_id=None, traffic=20):
    return Subscription(
        user_id=1,
        tariff_id=tariff_id,
        status=status,
        is_trial=status == 'trial',
        end_date=datetime.now(UTC) + timedelta(days=30),
        remnawave_id=panel_id,
        traffic_limit_gb=traffic,
    )


@pytest.mark.parametrize('status', ['active', 'trial', 'limited'])
async def test_main_xor_blocks_other_live_main(postgres_database, status):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        db.add(subscription(10, status=status, panel_id=100))
        await db.commit()
        db.add(subscription(11, panel_id=200))
        with pytest.raises(IntegrityError):
            await db.flush()
        await db.rollback()
        rows = (await db.scalars(select(Subscription))).all()
        assert len(rows) == 1
        assert rows[0].remnawave_id == 100


async def test_switch_preserves_identity_and_addon_quota(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        main = subscription(10, panel_id=101, traffic=200)
        addon = subscription(20, panel_id=202, traffic=10)
        db.add_all([main, addon])
        await db.commit()
        main_id = main.id
        main_url = main.subscription_url
        main.tariff_id = 11
        addon.status = 'limited'
        addon.traffic_used_gb = 10
        await db.commit()
        await db.refresh(main)
        await db.refresh(addon)
        assert main.id == main_id
        assert main.tariff_id == 11
        assert main.exclusive_family == 'main'
        assert main.remnawave_id == 101
        assert main.subscription_url == main_url
        assert main.status == 'active' and main.traffic_limit_gb == 200
        assert addon.exclusive_family == 'cdn'
        assert addon.remnawave_id == 202 and addon.traffic_limit_gb == 10
        assert addon.status == 'limited' and addon.traffic_used_gb == 10


async def test_expired_main_allows_new_main_and_repeat_same_tariff_is_unique(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        old = subscription(10, status='expired', panel_id=100)
        db.add(old)
        await db.commit()
        active = subscription(11, panel_id=200)
        db.add(active)
        await db.commit()
        db.add(subscription(11, panel_id=201))
        with pytest.raises(IntegrityError):
            await db.flush()
        await db.rollback()
        assert len((await db.scalars(select(Subscription))).all()) == 2


async def test_admin_reassignment_cannot_create_existing_family_conflict(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        independent = subscription(30, panel_id=300)
        basic = subscription(10, panel_id=100)
        db.add_all([independent, basic])
        await db.commit()
        tariff = await db.get(Tariff, 30)
        tariff.exclusive_family = 'main'
        with pytest.raises(IntegrityError):
            await db.flush()
        await db.rollback()
        await db.refresh(independent)
        assert independent.exclusive_family is None


async def test_manual_family_null_does_not_bypass_trigger(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        db.add(subscription(10, panel_id=100))
        await db.commit()
        actual = await db.scalar(
            text("UPDATE subscriptions SET exclusive_family = NULL WHERE tariff_id = 10 RETURNING exclusive_family")
        )
        assert actual == 'main'
        await db.rollback()


async def test_concurrent_main_purchases_cannot_both_commit(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=3) as (first, second, observer):
        await prepare(first)
        first.add(subscription(10, panel_id=100))
        await first.flush()
        second.add(subscription(11, panel_id=200))
        competing = asyncio.create_task(second.flush())
        try:
            await wait_for_lock_waiter(observer)
            assert not competing.done()
            await first.commit()
            with pytest.raises(IntegrityError):
                await competing
        finally:
            if not competing.done():
                competing.cancel()
                await asyncio.gather(competing, return_exceptions=True)
            await second.rollback()
        rows = (await first.scalars(select(Subscription))).all()
        assert len(rows) == 1
        assert rows[0].tariff_id == 10
