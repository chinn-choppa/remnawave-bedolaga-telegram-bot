"""Daily split payment owns one atomic balance/date/entitlement receipt."""

from datetime import UTC, datetime, timedelta
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import func, select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.config import settings
from app.database.models import BsTrafficAllowance, BsTrafficRenewalIntent, Subscription, Tariff, Transaction, User
from tests.database.test_bs_paid_renewal_postgres import prepared as checkout_setup
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def prepared(db, monkeypatch, *, reset=True, strategy='NO_RESET'):
    reader, owner, sub = await checkout_setup(db, monkeypatch)
    tariff = await db.get(Tariff, sub.tariff_id)
    tariff.is_daily = True
    tariff.daily_price_kopeks = 100
    tariff.traffic_reset_mode = strategy
    sub.is_trial = False
    sub.end_date = datetime.now(UTC) - timedelta(seconds=1)
    sub.last_daily_charge_at = datetime.now(UTC) - timedelta(days=2)
    from app.services.bs_traffic.base_renewal import reset_renewal_bases

    now = datetime.now(UTC)
    await reset_renewal_bases(
        db,
        **owner,
        operation_key='synthetic-prior-paid-base',
        amounts={'ordinary': 10 * 1024**3, 'bs': 5 * 1024**3},
        starts_at=now - timedelta(seconds=1),
        expires_at=now + timedelta(days=30),
    )
    monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_PAYMENT', reset)
    await db.commit()
    return reader, owner, sub, tariff


async def test_daily_charge_and_restart_replay_are_one_period(postgres_database, monkeypatch):
    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        due = sub.end_date
        before = datetime.now(UTC)
        result = await renew_daily_from_balance(db, **owner, expected_due_at=due)
        await db.commit()
        after = datetime.now(UTC)
        assert result.charged_kopeks == 100
        assert before + timedelta(days=1) <= result.new_end_date <= after + timedelta(days=1)
        assert (await db.get(User, 1)).balance_kopeks == 9900
        assert sub.last_daily_charge_at == result.new_end_date - timedelta(days=1)
        assert set(result.grants) == {'ordinary', 'bs'}
        await db.rollback()
        async with async_sessionmaker(db.bind, expire_on_commit=False)() as restarted, restarted.begin():
            replay = await renew_daily_from_balance(restarted, **owner, expected_due_at=due)
            assert replay.replayed and replay.intent_id == result.intent_id
            assert await restarted.scalar(select(func.count()).select_from(Transaction)) == 1
            assert (await restarted.get(User, 1)).balance_kopeks == 9900


@pytest.mark.parametrize('fault', ['future', 'paused', 'trial', 'recent_charge'])
async def test_not_due_daily_never_debits(postgres_database, monkeypatch, fault):
    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        if fault == 'future':
            sub.end_date = datetime.now(UTC) + timedelta(days=3)
        elif fault == 'paused':
            sub.is_daily_paused = True
        elif fault == 'trial':
            sub.is_trial = True
        else:
            sub.last_daily_charge_at = datetime.now(UTC)
        await db.commit()
        assert await renew_daily_from_balance(db, **owner, expected_due_at=sub.end_date) is None
        assert (await db.get(User, 1)).balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_daily_policy_day_never_refreshes_balance_twice(postgres_database, monkeypatch):
    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch, reset=True, strategy='DAY')
        old = (await db.scalars(select(BsTrafficAllowance))).all()
        balances = {row.id: row.remaining_bytes for row in old}
        result = await renew_daily_from_balance(db, **owner, expected_due_at=sub.end_date)
        await db.commit()
        row = await db.get(BsTrafficRenewalIntent, result.intent_id)
        assert row.parameters['reset_base'] is False
        assert {
            item.id: item.remaining_bytes
            for item in (await db.scalars(select(BsTrafficAllowance))).all()
            if item.id in balances
        } == balances


async def test_daily_service_uses_durable_delivery_and_skips_legacy_reset(postgres_database, monkeypatch):
    from app.services.daily_subscription_service import DailySubscriptionService

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub, _ = await prepared(db, monkeypatch)
        delivery = AsyncMock(return_value=False)
        reset = AsyncMock(side_effect=AssertionError('legacy reset called'))
        monkeypatch.setattr('app.services.bs_traffic.renewal.deliver_renewal', delivery)
        monkeypatch.setattr('app.services.paid_traffic_reset_service.stage_daily_reset', reset)
        monkeypatch.setattr('app.services.subscription_renewal_service.with_admin_notification_service', AsyncMock())
        await db.refresh(sub, ['user', 'tariff'])
        service = DailySubscriptionService()
        assert await service._process_single_charge(db, sub) == 'charged'
        assert await service._process_single_charge(db, sub) == 'skipped'
        delivery.assert_awaited_once()
        reset.assert_not_awaited()
        assert (await db.get(User, 1)).balance_kopeks == 9900
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_daily_price_includes_devices_group_discount_and_keeps_offer(postgres_database, monkeypatch):
    from app.database.models import PromoGroup
    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, tariff = await prepared(db, monkeypatch)
        tariff.device_limit, tariff.device_price_kopeks = 1, 300
        sub.device_limit, sub.daily_devices_billing_version = 3, 1
        group = PromoGroup(name='synthetic daily discount', period_discounts={'1': 25}, device_discount_percent=50)
        db.add(group)
        await db.flush()
        user = await db.get(User, 1)
        user.promo_group_id = group.id
        user.promo_offer_discount_percent = 90
        user.promo_offer_discount_expires_at = datetime.now(UTC) + timedelta(days=1)
        await db.commit()
        result = await renew_daily_from_balance(db, **owner, expected_due_at=sub.end_date)
        await db.commit()
        assert result.charged_kopeks == 85  # 100 * 75% + (2 * 300 / 30) * 50%.
        assert user.balance_kopeks == 9915
        assert user.promo_offer_discount_percent == 90


async def test_concurrent_daily_workers_charge_once(postgres_database, monkeypatch):
    import asyncio

    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        due = sub.end_date
        factory = async_sessionmaker(db.bind, expire_on_commit=False)

        async def charge():
            async with factory() as worker, worker.begin():
                return await renew_daily_from_balance(worker, **owner, expected_due_at=due)

        first, second = await asyncio.gather(charge(), charge())
        assert first.intent_id == second.intent_id
        assert {first.replayed, second.replayed} == {False, True}
        assert (await db.get(User, 1, populate_existing=True)).balance_kopeks == 9900
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_daily_failure_rolls_back_charge_and_all_entitlements(postgres_database, monkeypatch):
    from app.services.bs_traffic import renewal
    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        end, charged_at = sub.end_date, sub.last_daily_charge_at
        grants = await db.scalar(select(func.count()).select_from(BsTrafficAllowance))
        original = renewal.reset_renewal_bases

        async def fail(*args, **kwargs):
            await original(*args, **kwargs)
            raise RuntimeError('synthetic failure after grants')

        monkeypatch.setattr(renewal, 'reset_renewal_bases', fail)
        with pytest.raises(RuntimeError, match='after grants'):
            await renew_daily_from_balance(db, **owner, expected_due_at=end)
        await db.commit()
        await db.refresh(sub)
        assert sub.end_date == end and sub.last_daily_charge_at == charged_at
        assert (await db.get(User, 1)).balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == grants


async def test_daily_insufficient_balance_uses_existing_suspension_without_debit(postgres_database, monkeypatch):
    from app.services.daily_subscription_service import DailySubscriptionService

    async with postgres_session(postgres_database, TABLES) as db:
        _, _, sub, _ = await prepared(db, monkeypatch)
        user = await db.get(User, 1)
        user.balance_kopeks = 50
        await db.commit()
        assert await DailySubscriptionService()._process_single_charge(db, sub) == 'suspended'
        assert (await db.get(User, 1)).balance_kopeks == 50
        assert (await db.get(Subscription, 1)).status == 'disabled'
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_stopped_bs_daily_charge_retains_original_drain_horizon(postgres_database, monkeypatch):
    from app.database.crud.tariff import update_tariff
    from app.database.models import BsTrafficLedger
    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, tariff = await prepared(db, monkeypatch)
        due = sub.end_date
        bs_base = await db.scalar(
            select(BsTrafficAllowance).where(
                BsTrafficAllowance.traffic_group == 'bs', BsTrafficAllowance.retired_at.is_(None)
            )
        )
        bs_base.expires_at = datetime.now(UTC) + timedelta(minutes=10)
        before = bs_base.expires_at.isoformat()
        await db.commit()
        await update_tariff(db, tariff, bs_traffic_settings=None)
        result = await renew_daily_from_balance(db, **owner, expected_due_at=due)
        await db.commit()
        assert set(result.grants) == {'ordinary'}
        assert result.new_end_date > datetime.fromisoformat(before)
        assert (await db.get(BsTrafficLedger, 1)).access_policy['bs_drain_until'] == before


async def test_daily_no_reset_keeps_ordinary_access_when_only_bs_is_empty(postgres_database, monkeypatch):
    from dataclasses import replace

    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance
    from app.services.bs_traffic.ledger import open_usage, resolve_usage, summary
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal
    from tests.database.test_bs_traffic_policy_postgres import ORDINARY

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _ = await prepared(db, monkeypatch, reset=False)
        now = datetime.now(UTC)
        usage = await open_usage(
            db,
            **owner,
            group='bs',
            source_key='synthetic-bs-exhaustion',
            amount_bytes=5 * 1024**3,
            interval_start=now - timedelta(microseconds=1),
            interval_end=now,
        )
        await resolve_usage(db, **owner, usage_id=usage, decision='charge')
        remote = {'state': reader._api.get_split_grace_snapshot.return_value}
        used = remote['state'].used_traffic_bytes
        reader._api.get_split_grace_snapshot = AsyncMock(side_effect=lambda _: remote['state'])

        async def write(target):
            remote['state'] = replace(target, used_traffic_bytes=used + 1)

        reader._api.restore_split_user_once = AsyncMock(side_effect=write)
        ordinary_before = (await summary(db, **owner, now=datetime.now(UTC)))['ordinary']['remaining_bytes']
        result = await renew_daily_from_balance(db, **owner, expected_due_at=sub.end_date)
        await db.commit()
        balances = await summary(db, **owner, now=datetime.now(UTC))
        assert balances['bs']['remaining_bytes'] == 0
        assert balances['ordinary']['remaining_bytes'] == ordinary_before > 0
        await db.rollback()
        assert await advance_paid_renewal(reader, **owner, operation_id=result.intent_id) == 'verified'
        assert remote['state'].status == 'ACTIVE'
        assert remote['state'].expire_at == result.new_end_date
        assert remote['state'].traffic_limit_bytes == 0 and remote['state'].traffic_limit_strategy == 'NO_RESET'
        assert remote['state'].active_internal_squads == [{'uuid': ORDINARY}]
        assert remote['state'].used_traffic_bytes == used + 1
        reader._api.restore_split_user_once.assert_awaited_once()


async def test_daily_fully_discounted_cycle_still_has_one_durable_receipt(postgres_database, monkeypatch):
    from app.database.models import PromoGroup
    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        group = PromoGroup(name='synthetic free daily group', period_discounts={'1': 100})
        db.add(group)
        await db.flush()
        (await db.get(User, 1)).promo_group_id = group.id
        await db.commit()
        result = await renew_daily_from_balance(db, **owner, expected_due_at=sub.end_date)
        await db.commit()
        assert result.charged_kopeks == 0
        assert (await db.get(User, 1)).balance_kopeks == 10000
        assert (await db.get(Transaction, result.transaction_id)).amount_kopeks == 0
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 1
