"""Проверка Main XOR на реальном PostgreSQL, включая гонку двух transactions."""

import asyncio
from datetime import UTC, datetime, timedelta
from importlib import import_module
from types import SimpleNamespace

import pytest
from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from app.cabinet.routes.subscription_modules.purchase import _resolve_tariff_purchase_target
from app.config import settings
from app.database.crud.subscription import get_live_tariff_family_conflict
from app.database.models import Base, Subscription, Tariff, Transaction, User
from app.services import subscription_auto_purchase_service as auto
from tests.fixtures.postgres_db import postgres_sessions, wait_for_lock_waiter


pytestmark = pytest.mark.postgres
TABLES = list(Base.metadata.sorted_tables)
DDL = import_module('migrations.alembic.versions.0169_tariff_exclusive_family').TRIGGER_SQL


async def prepare(db):
    # Проверяем те же триггеры, которые устанавливает Alembic 0142 и bootstrap.
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
        main.subscription_url = 'https://example.invalid/main'
        addon.subscription_url = 'https://example.invalid/cdn'
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
        assert main_url != addon.subscription_url
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


async def test_precharge_guard_allows_cdn_but_rejects_second_main(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        main = subscription(10, panel_id=101)
        db.add(main)
        await db.commit()
        assert await get_live_tariff_family_conflict(db, 1, 11) == main.id
        assert await get_live_tariff_family_conflict(db, 1, 11, exclude_subscription_id=main.id) is None
        assert await get_live_tariff_family_conflict(db, 1, 20) is None
        assert await get_live_tariff_family_conflict(db, 1, 30) is None


async def test_cabinet_fails_before_charging_for_another_main(postgres_database, monkeypatch):
    monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        db.add(subscription(10, panel_id=101))
        await db.commit()
        user = await db.get(User, 1)
        premium = await db.get(Tariff, 12)
        request = SimpleNamespace(subscription_id=None, selected_device_limit=None)
        with pytest.raises(HTTPException) as error:
            await _resolve_tariff_purchase_target(db, user, premium, request)
        assert error.value.status_code == 409
        assert error.value.detail['code'] == 'tariff_family_conflict'
        assert list(await db.scalars(select(Transaction))) == []


async def test_telegram_saved_cart_fails_before_debit(postgres_database, monkeypatch):
    monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        user = await db.get(User, 1)
        user.balance_kopeks = 10000
        db.add(subscription(10, panel_id=101))
        await db.commit()
        assert await auto._auto_purchase_tariff(db, user, {'tariff_id': 11, 'period_days': 30}) is False
        await db.refresh(user)
        assert user.balance_kopeks == 10000
        assert list(await db.scalars(select(Transaction))) == []


async def test_addon_lifecycle_does_not_change_main(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        main, cdn = subscription(10, panel_id=101, traffic=200), subscription(20, panel_id=202, traffic=5)
        db.add_all([main, cdn])
        await db.commit()
        cdn.status = 'disabled'
        cdn.traffic_limit_gb = 15
        cdn.end_date += timedelta(days=10)
        await db.commit()
        await db.refresh(main)
        assert main.status == 'active'
        assert main.traffic_limit_gb == 200
        assert main.remnawave_id == 101
        await db.refresh(cdn)
        assert cdn.remnawave_id == 202 and cdn.traffic_limit_gb == 15


async def test_independent_main_and_cdn_can_be_inserted_concurrently(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (main_db, cdn_db):
        await prepare(main_db)
        main_db.add(subscription(10, panel_id=101))
        cdn_db.add(subscription(20, panel_id=202))
        await asyncio.gather(main_db.commit(), cdn_db.commit())
        rows = list(await main_db.scalars(select(Subscription).order_by(Subscription.tariff_id)))
        assert [(r.tariff_id, r.exclusive_family, r.remnawave_id) for r in rows] == [
            (10, 'main', 101),
            (20, 'cdn', 202),
        ]


async def test_reactivate_expired_main_conflicts_with_new_main(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        expired = subscription(10, status='expired', panel_id=100)
        active = subscription(11, panel_id=200)
        db.add_all([expired, active])
        await db.commit()
        expired.status = 'active'
        with pytest.raises(IntegrityError):
            await db.flush()
        await db.rollback()
        rows = list(await db.scalars(select(Subscription).order_by(Subscription.tariff_id)))
        assert [r.status for r in rows] == ['expired', 'active']


async def test_main_upgrade_and_downgrade_reuses_single_row(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        main, cdn = subscription(10, panel_id=101), subscription(20, panel_id=202, traffic=7)
        db.add_all([main, cdn])
        await db.commit()
        main_id = main.id
        for tariff_id in (11, 12, 10):
            main.tariff_id = tariff_id
            await db.commit()
            await db.refresh(main)
            assert main.id == main_id
            assert main.remnawave_id == 101
            assert main.exclusive_family == 'main'
        await db.refresh(cdn)
        assert cdn.exclusive_family == 'cdn'
        assert cdn.remnawave_id == 202 and cdn.traffic_limit_gb == 7
        assert len((await db.scalars(select(Subscription))).all()) == 2
