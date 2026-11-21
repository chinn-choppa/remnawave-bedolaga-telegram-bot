"""Проверка Main XOR на реальном PostgreSQL, включая гонку двух transactions."""

import asyncio
from datetime import UTC, datetime, timedelta
from importlib import import_module
from inspect import unwrap
from types import SimpleNamespace
from unittest.mock import AsyncMock
from uuid import uuid4

import pytest
import sqlalchemy as sa
from alembic.migration import MigrationContext
from alembic.operations import Operations
from fastapi import HTTPException
from sqlalchemy import select, text
from sqlalchemy.exc import IntegrityError

from app.cabinet.routes.subscription_modules.purchase import _resolve_tariff_purchase_target
from app.config import settings
from app.database.crud.subscription import (
    _trial_conversion_is_ready,
    create_paid_subscription,
    get_live_tariff_family_conflict,
    resolve_trial_conversion_candidate,
)
from app.database.models import Base, GuestPurchase, Subscription, Tariff, TariffPurchaseIntent, Transaction, User
from app.handlers.subscription import tariff_purchase as telegram
from app.services import subscription_auto_purchase_service as auto
from app.services.guest_purchase_service import GuestPurchaseError, create_purchase
from app.services.recurrent_purchase_selection import _resolve_new_device_target, prepare_recurrent_device_purchase
from tests.fixtures.postgres_db import postgres_engine, postgres_sessions, wait_for_lock_waiter


pytestmark = pytest.mark.postgres
TABLES = list(Base.metadata.sorted_tables)
DDL = import_module('migrations.alembic.versions.0144_tariff_exclusive_family').TRIGGER_SQL


async def prepare(db):
    # Проверяем те же триггеры, которые устанавливает Alembic 0144 и bootstrap.
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


def subscription(tariff_id, *, panel_id: int, status='active', traffic=20):
    return Subscription(
        user_id=1,
        tariff_id=tariff_id,
        status=status,
        is_trial=status == 'trial',
        end_date=datetime.now(UTC) + timedelta(days=30),
        remnawave_id=panel_id,
        remnawave_short_id=f'{panel_id:06x}',
        traffic_limit_gb=traffic,
    )


async def test_alembic_0144_upgrade_downgrade_and_family_backfill(postgres_database):
    migration = import_module('migrations.alembic.versions.0144_tariff_exclusive_family')
    async with postgres_engine(postgres_database, TABLES) as engine:
        async with engine.connect() as connection:
            outer = await connection.begin()
            try:

                def exercise(sync):
                    with Operations.context(MigrationContext.configure(sync)):
                        migration.downgrade()
                        assert 'exclusive_family' not in {
                            column['name'] for column in sa.inspect(sync).get_columns('tariffs')
                        }
                        sync.execute(User.__table__.insert().values(id=1, telegram_id=70001, language='ru'))
                        sync.execute(Tariff.__table__.insert().values(id=10, name='Basic', period_prices={'30': 1000}))
                        sync.execute(
                            Subscription.__table__.insert().values(
                                user_id=1,
                                tariff_id=10,
                                status='active',
                                is_trial=False,
                                remnawave_short_id='000064',
                                end_date=datetime.now(UTC) + timedelta(days=30),
                            )
                        )
                        migration.upgrade()
                        indexes = sa.inspect(sync).get_indexes('subscriptions')
                        assert any(
                            index['name'] == 'uq_subscriptions_user_exclusive_family_live' and index['unique']
                            for index in indexes
                        )
                        constraints = sa.inspect(sync).get_check_constraints('tariffs')
                        assert any(check['name'] == 'ck_tariffs_exclusive_family_slug' for check in constraints)
                        assert (
                            sync.scalar(
                                sa.text(
                                    'SELECT COUNT(*) FROM pg_trigger WHERE tgname IN '
                                    "('tg_subscriptions_exclusive_family', 'tg_tariffs_exclusive_family')"
                                )
                            )
                            == 2
                        )
                        sync.execute(sa.text("UPDATE tariffs SET exclusive_family = 'main' WHERE id = 10"))
                        assert (
                            sync.scalar(sa.text('SELECT exclusive_family FROM subscriptions WHERE user_id = 1'))
                            == 'main'
                        )
                        migration.downgrade()
                        assert 'exclusive_family' not in {
                            column['name'] for column in sa.inspect(sync).get_columns('subscriptions')
                        }
                        assert sync.scalar(sa.text('SELECT COUNT(*) FROM subscriptions')) == 1

                await connection.run_sync(exercise)
            finally:
                # PostgreSQL DDL is transactional: restore the fresh bootstrap
                # schema for other test modules sharing the session fixture.
                await outer.rollback()


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
            text('UPDATE subscriptions SET exclusive_family = NULL WHERE tariff_id = 10 RETURNING exclusive_family')
        )
        assert actual == 'main'
        await db.rollback()


async def test_cdn_family_xor_keeps_main_and_unclassified_tariff(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        db.add(Tariff(id=21, name='CDN Backup', exclusive_family='cdn', period_prices={'30': 500}))
        await db.commit()
        db.add_all(
            [
                subscription(10, panel_id=101),
                subscription(20, status='limited', panel_id=202),
                subscription(30, panel_id=303),
            ]
        )
        await db.commit()
        assert await get_live_tariff_family_conflict(db, user_id=1, tariff_id=21) is not None
        db.add(subscription(21, panel_id=404))
        with pytest.raises(IntegrityError):
            await db.flush()
        await db.rollback()
        rows = (await db.scalars(select(Subscription))).all()
        assert {row.remnawave_id for row in rows} == {101, 202, 303}


async def test_main_family_is_scoped_to_user_not_global(postgres_database):
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        db.add(User(id=2, telegram_id=70002, language='ru'))
        await db.commit()
        other_main = subscription(11, panel_id=202)
        other_main.user_id = 2
        db.add_all([subscription(10, panel_id=101), other_main])
        await db.commit()
        rows = (await db.scalars(select(Subscription))).all()
        assert {(row.user_id, row.exclusive_family) for row in rows} == {(1, 'main'), (2, 'main')}


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


async def test_payhot_quote_rejects_second_main_before_provider(postgres_database, monkeypatch):
    from app.services import payhot_recurrent_checkout as payhot

    monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    monkeypatch.setattr(
        payhot,
        'get_payhot_recurrent_policy',
        AsyncMock(return_value={'enabled': True, 'methods': ('card',)}),
    )
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        db.add(subscription(10, panel_id=101))
        premium = await db.get(Tariff, 12)
        premium.is_active = True
        await db.commit()

        user = await db.get(User, 1)
        with pytest.raises(ValueError, match='tariff_family_conflict'):
            await payhot._terms(
                db,
                user=user,
                tariff_id=12,
                period_days=30,
                device_limit=None,
                subscription_id=None,
                payment_method='card',
                channel='cabinet',
            )
        # A CDN checkout explicitly targeting a trial Main is forbidden before
        # any provider interaction or account mutation.
        main = await db.scalar(select(Subscription).where(Subscription.tariff_id == 10))
        main.is_trial = True
        main.status = 'trial'
        main.autopay_enabled = False
        cdn = await db.get(Tariff, 20)
        cdn.is_active = True
        await db.commit()
        with pytest.raises(ValueError, match='tariff_family_conflict'):
            await payhot._terms(
                db,
                user=user,
                tariff_id=20,
                period_days=30,
                device_limit=None,
                subscription_id=main.id,
                payment_method='card',
                channel='cabinet',
            )
        await db.refresh(main)
        assert main.tariff_id == 10 and main.is_trial and main.remnawave_id == 101
        assert list(await db.scalars(select(Transaction))) == []


async def test_payhot_paid_callback_does_not_activate_second_main(postgres_database, monkeypatch):
    from app.services import payhot_recurrent_service as payhot

    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    monkeypatch.setattr(payhot, 'terms_match', AsyncMock(return_value=True))
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        existing = subscription(10, panel_id=101, status='expired')
        pending = subscription(12, panel_id=202, status='pending')
        db.add_all([existing, pending])
        await db.commit()

        user = await db.get(User, 1)
        agreement = SimpleNamespace(owner_id=1, reason=None, consent_snapshot={'tariff_id': 12})
        assert await payhot._target_matches(db, user, pending, agreement, None, first=True)
        existing.status = 'active'
        await db.commit()
        assert not await payhot._target_matches(db, user, pending, agreement, None, first=True)
        assert pending.status == 'pending'
        assert existing.status == 'active'

        # Stale callback must not repurpose a trial Main into CDN B.
        existing.status = 'trial'
        existing.is_trial = True
        await db.commit()
        cdn_agreement = SimpleNamespace(owner_id=1, reason=None, consent_snapshot={'tariff_id': 20})
        assert not await payhot._target_matches(db, user, existing, cdn_agreement, None, first=True)
        await db.refresh(existing)
        assert existing.tariff_id == 10 and existing.remnawave_id == 101 and existing.is_trial


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


async def test_cabinet_trial_race_refunds_after_wallet_debit(postgres_database, monkeypatch):
    """A stale trial selected before charging must not lose money or Main identity."""
    from app.cabinet.routes.subscription_modules import purchase as cabinet
    from app.database.crud import subscription as crud

    monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    # Model a competing checkout converting the trial after its selection.
    # The real SELECT FOR UPDATE / status refresh is tested separately.
    rejected_conversion = AsyncMock(return_value=False)
    monkeypatch.setattr(crud, '_trial_conversion_is_ready', rejected_conversion)
    monkeypatch.setattr(
        'app.services.checkout_squad_validation.validate_checkout_squads',
        AsyncMock(return_value=None),
    )

    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        user = await db.get(User, 1)
        user.balance_kopeks = 10000
        tariff = await db.get(Tariff, 10)
        tariff.is_active = True
        tariff.traffic_limit_gb = 20
        tariff.device_limit = 1
        tariff.allowed_squads = ['test-squad']
        trial = subscription(10, panel_id=101, status='trial')
        db.add(trial)
        await db.commit()
        initial_end = trial.end_date

        with pytest.raises(HTTPException) as error:
            await cabinet.purchase_tariff(
                cabinet.TariffPurchaseRequest(tariff_id=10, period_days=30),
                user,
                db,
            )
        assert error.value.status_code == 500
        rejected_conversion.assert_awaited_once()
        await db.refresh(user)
        await db.refresh(trial)
        assert user.balance_kopeks == 10000
        assert (trial.status, trial.is_trial, trial.tariff_id, trial.remnawave_id) == ('trial', True, 10, 101)
        assert trial.end_date == initial_end
        assert len((await db.scalars(select(Subscription))).all()) == 1
        transactions = list(await db.scalars(select(Transaction)))
        assert len(transactions) == 2
        amounts = sorted(tx.amount_kopeks for tx in transactions)
        assert amounts[0] < 0 and amounts[1] == -amounts[0]


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


async def test_addon_lifecycle_does_not_change_main(postgres_database, monkeypatch):
    monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, competitor):
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

        # Cross-family purchases must leave an unrelated trial identity intact.
        # Legacy unclassified -> unclassified trial conversion is tested by
        # test_expired_trial_conversion_postgres (pre-existing behaviour).
        for user_id, trial_tariff, paid_tariff in (
            (2, 10, 20),
            (3, 20, 10),
            (4, 10, 30),
            (5, 30, 20),
        ):
            db.add(User(id=user_id, telegram_id=70000 + user_id, language='ru'))
            trial = subscription(trial_tariff, panel_id=300 + user_id, status='trial')
            trial.user_id = user_id
            db.add(trial)
            await db.commit()

            assert await resolve_trial_conversion_candidate(db, user_id, paid_tariff) is None
            purchased = await create_paid_subscription(
                db,
                user_id,
                duration_days=30,
                tariff_id=paid_tariff,
                traffic_limit_gb=5,
                connected_squads=['test-squad'],
                commit=False,
            )
            await db.commit()
            await db.refresh(trial)
            await db.refresh(purchased)
            assert purchased.id != trial.id
            assert purchased.tariff_id == paid_tariff and purchased.status == 'active'
            assert purchased.remnawave_id != trial.remnawave_id
            assert trial.tariff_id == trial_tariff
            assert trial.status == 'trial' and trial.is_trial
            assert trial.remnawave_id == 300 + user_id

        # Cabinet must not overwrite an already paid conversion selected
        # earlier in another session. The guard rereads under SELECT FOR UPDATE.
        db.add(User(id=6, telegram_id=70006, language='ru'))
        old_trial = subscription(10, status='trial', panel_id=306)
        old_trial.user_id = 6
        db.add(old_trial)
        await db.commit()
        stale = await competitor.get(Subscription, old_trial.id)
        old_trial.is_trial = False
        old_trial.status = 'active'
        old_trial.tariff_id = 11
        await db.commit()
        assert not await _trial_conversion_is_ready(competitor, stale, 12)
        await competitor.rollback()
        await db.refresh(old_trial)
        assert old_trial.tariff_id == 11 and old_trial.remnawave_id == 306


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


async def test_guest_purchase_rejects_second_main_before_payment_record(postgres_database, monkeypatch):
    monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        user = await db.get(User, 1)
        user.email = 'customer@example.invalid'
        db.add(subscription(10, panel_id=101))
        await db.commit()
        premium = await db.get(Tariff, 12)
        with pytest.raises(GuestPurchaseError) as error:
            await create_purchase(
                db=db,
                landing=None,
                tariff=premium,
                period_days=30,
                amount_kopeks=3000,
                contact_type='email',
                contact_value='customer@example.invalid',
                payment_method='yookassa',
            )
        assert error.value.status_code == 409
        assert not list(await db.scalars(select(GuestPurchase)))


async def test_guest_purchase_accepts_main_trial_conversion_without_creating_payment(postgres_database, monkeypatch):
    monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        user = await db.get(User, 1)
        user.email = 'trial@example.invalid'
        trial = subscription(10, status='trial', panel_id=101)
        db.add(trial)
        await db.commit()

        premium = await db.get(Tariff, 12)
        guest = await create_purchase(
            db=db,
            landing=None,
            tariff=premium,
            period_days=30,
            amount_kopeks=3000,
            contact_type='email',
            contact_value='trial@example.invalid',
            payment_method='yookassa',
        )
        assert guest.id is not None
        assert len(list(await db.scalars(select(Subscription)))) == 1
        assert not list(await db.scalars(select(Transaction)))


async def test_guest_paid_conversion_with_disabled_same_tariff_reuses_trial(postgres_database, monkeypatch):
    # Payment is real to the application, but the provider and panel boundaries
    # are synthetic. A disabled target row must not steal the trial conversion.
    from app.database.models import EtoplatezhiPayment
    from app.services import guest_purchase_service as guest
    from app.services.payment.etoplatezhi import EtoplatezhiPaymentMixin
    from app.utils.cache import cache

    monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    monkeypatch.setattr(settings, 'RESET_DEVICES_ON_RENEWAL', False)
    monkeypatch.setattr(cache, 'get', AsyncMock(return_value=None))
    for method in ('send_guest_notification', '_send_admin_notification', '_create_nalogo_receipt_for_purchase'):
        monkeypatch.setattr(guest, method, AsyncMock())

    async def provision(_self, db, subscription, **kwargs):
        subscription.subscription_url = 'https://example.invalid/sub/canary'
        await db.commit()
        return SimpleNamespace(id=subscription.remnawave_id)

    monkeypatch.setattr(guest.SubscriptionService, 'create_remnawave_user', provision)
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        user = await db.get(User, 1)
        user.email = 'paid-trial@example.invalid'
        user.auth_type = 'email'
        user.password_hash = 'test-only-hash'
        user.email_verified = True
        main = await db.get(Tariff, 10)
        premium = await db.get(Tariff, 11)
        for tariff in (main, premium):
            tariff.is_active = True
            tariff.allowed_squads = ['test-squad']
        trial = subscription(10, panel_id=101, status='trial')
        disabled = subscription(11, panel_id=202, status='disabled')
        db.add_all([trial, disabled])
        purchase = GuestPurchase(
            token='p' * 64,
            tariff_id=11,
            period_days=30,
            amount_kopeks=2000,
            contact_type='email',
            contact_value=user.email,
            is_gift=False,
            payment_method='etoplatezhi',
            status='pending',
        )
        payment = EtoplatezhiPayment(
            order_id='test-guest-xor',
            amount_kopeks=2000,
            status='success',
            is_paid=True,
            metadata_json={'purpose': 'guest_purchase', 'purchase_token': purchase.token},
        )
        db.add_all([purchase, payment])
        await db.commit()
        handler = EtoplatezhiPaymentMixin()
        for _ in range(2):
            locked = await db.scalar(
                select(EtoplatezhiPayment).where(EtoplatezhiPayment.id == payment.id).with_for_update()
            )
            assert await handler._finalize_etoplatezhi_payment(db, locked, trigger='test')
        await db.refresh(purchase)
        await db.refresh(trial)
        await db.refresh(disabled)
        assert purchase.status == 'delivered'
        assert (trial.tariff_id, trial.status, trial.is_trial, trial.remnawave_id) == (11, 'active', False, 101)
        assert (disabled.tariff_id, disabled.status, disabled.remnawave_id) == (11, 'disabled', 202)
        assert len(list(await db.scalars(select(Subscription)))) == 2
        assert len(list(await db.scalars(select(Transaction)))) == 1


async def test_recurrent_checkout_rejects_second_main_before_provider(postgres_database, monkeypatch):
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        db.add(subscription(10, panel_id=101))
        await db.commit()
        user = await db.get(User, 1)
        premium = await db.get(Tariff, 12)
        with pytest.raises(ValueError, match='tariff_family_conflict'):
            await _resolve_new_device_target(db, user, premium, subscription_id=None)


async def test_recurrent_checkout_replay_rejects_new_main_conflict(postgres_database, monkeypatch):
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        earlier = subscription(10, status='expired', panel_id=101)
        current = subscription(11, panel_id=202)
        db.add_all([earlier, current])
        await db.flush()
        checkout_id = str(uuid4())
        db.add(
            TariffPurchaseIntent(
                user_id=1,
                checkout_id=checkout_id,
                fingerprint='stale-replay',
                parameters={'request': {'tariff_id': 10}, 'terms': {}},
                subscription_id=earlier.id,
                charged_kopeks=0,
            )
        )
        await db.commit()

        user = await db.get(User, 1)
        basic = await db.get(Tariff, 10)
        with pytest.raises(ValueError, match='tariff_family_conflict'):
            await prepare_recurrent_device_purchase(
                db,
                user=user,
                tariff=basic,
                provider='platega',
                selected_device_limit=1,
                quote_id=str(uuid4()),
                checkout_id=checkout_id,
            )
        assert len(list(await db.scalars(select(TariffPurchaseIntent)))) == 1
        assert not list(await db.scalars(select(Transaction)))


async def test_telegram_direct_daily_rejects_second_main_before_debit(postgres_database, monkeypatch):
    monkeypatch.setattr(settings, 'SALES_MODE', 'tariffs')
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    monkeypatch.setattr(telegram, '_reject_unavailable_tariff', AsyncMock(return_value=False))
    monkeypatch.setattr(telegram, '_resolve_purchase_subscription', AsyncMock(return_value=None))
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        user = await db.get(User, 1)
        user.balance_kopeks = 10000
        premium = await db.get(Tariff, 12)
        premium.is_daily = True
        premium.daily_price_kopeks = 100
        premium.is_active = True
        db.add(subscription(10, panel_id=101))
        await db.commit()
        callback = SimpleNamespace(
            data='daily_confirm:12', answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock())
        )
        state = SimpleNamespace(get_state=AsyncMock(return_value=None), get_data=AsyncMock(return_value={}))
        await unwrap(telegram.confirm_daily_tariff_purchase)(callback, user, db, state)
        assert callback.answer.await_args.kwargs.get('show_alert') is True
        assert 'смену тарифа' in callback.answer.await_args.args[0]
        await db.refresh(user)
        assert user.balance_kopeks == 10000
        assert not list(await db.scalars(select(Transaction)))


@pytest.mark.parametrize(
    'provider,handler',
    [
        ('lava', 'purchase_tariff_with_lava'),
        ('cashera', 'purchase_tariff_with_cashera'),
        ('platega', 'purchase_tariff_with_sbp'),
    ],
)
async def test_telegram_recurring_rejects_second_main_before_provider(
    postgres_database, monkeypatch, provider, handler
):
    monkeypatch.setattr(settings, 'MULTI_TARIFF_ENABLED', True)
    monkeypatch.setattr(telegram, '_prepare_telegram_recurrent_devices', AsyncMock(return_value={}))
    async with postgres_sessions(postgres_database, TABLES, count=2) as (db, _):
        await prepare(db)
        db.add(subscription(10, panel_id=101))
        await db.commit()
        user = await db.get(User, 1)
        premium = await db.get(Tariff, 12)
        premium.is_active = True
        await db.commit()
        callback = SimpleNamespace(
            data=f'{provider}:12', answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock())
        )
        await unwrap(getattr(telegram, handler))(callback, user, db, None)
        assert callback.answer.await_args.kwargs.get('show_alert') is True
        assert 'смену тарифа' in callback.answer.await_args.args[0]
