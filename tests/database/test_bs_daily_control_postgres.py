"""Explicit daily controls separate free flags from immutable paid consent."""

from datetime import UTC, datetime, timedelta
from importlib import import_module, util
from uuid import uuid4

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, BsTrafficRenewalIntent, Transaction, User
from tests.database.test_bs_daily_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def core():
    assert util.find_spec('app.services.bs_traffic.daily_control') is not None, 'shared daily controls missing'
    return import_module('app.services.bs_traffic.daily_control')


async def snapshot(db, sub):
    return (
        sub.end_date,
        sub.last_daily_charge_at,
        sub.status,
        list(
            await db.execute(
                select(
                    BsTrafficAllowance.id,
                    BsTrafficAllowance.remaining_bytes,
                    BsTrafficAllowance.expires_at,
                    BsTrafficAllowance.retired_at,
                ).order_by(BsTrafficAllowance.id)
            )
        ),
    )


async def test_pause_and_paid_active_unpause_never_require_balance_or_change_entitlements(
    postgres_database, monkeypatch
):
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        sub.end_date = datetime.now(UTC) + timedelta(days=3)
        user = await db.get(User, sub.user_id)
        user.balance_kopeks = 0
        await db.commit()
        before = await snapshot(db, sub)
        first = await core().set_daily_pause(db, **owner, paused=True)
        assert first.is_paused and first.changed
        assert not (await core().set_daily_pause(db, **owner, paused=True)).changed
        resumed = await core().set_daily_pause(db, **owner, paused=False)
        assert not resumed.is_paused and resumed.changed
        assert await snapshot(db, sub) == before
        assert user.balance_kopeks == 0
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 0


async def test_expired_free_unpause_requires_separate_paid_consent(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        sub.is_daily_paused = True
        await db.commit()
        with pytest.raises(core().DailyResumeConsentRequired):
            await core().set_daily_pause(db, **owner, paused=False)
        await db.refresh(sub)
        assert sub.is_daily_paused
        assert (await db.get(User, sub.user_id)).balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_paid_resume_quote_confirm_and_offline_replay_are_one_day(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        sub.is_daily_paused = True
        await db.commit()
        quote = await core().quote_daily_resume(db, **owner)
        args = dict(
            **owner,
            operation_key=str(uuid4()),
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
        )
        result = await core().resume_daily_from_balance(db, **args)
        await db.commit()
        assert result.charged_kopeks == 100 and result.new_end_date >= quote.intended_end_date
        assert not sub.is_daily_paused and sub.last_daily_charge_at is not None
        sub.traffic_accounting_mode = 'managed'
        await db.commit()
        repeated = await core().resume_daily_from_balance(db, **args)
        assert repeated.replayed and repeated.intent_id == result.intent_id
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_daily_resume_checkout_timeout_keeps_owned_offline_receipt(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _ = await prepared(db, monkeypatch)
        identifier = sub.id
        assert util.find_spec('app.services.bs_traffic.daily_control_checkout') is not None
        checkout = import_module('app.services.bs_traffic.daily_control_checkout')
        quote = await checkout.prepare_daily_resume(reader, user_id=owner['user_id'], subscription_id=identifier)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(side_effect=TimeoutError))
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        request = dict(
            user_id=owner['user_id'],
            subscription_id=identifier,
            operation_key=str(uuid4()),
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
        )
        paid = await checkout.checkout_daily_resume(reader, **request)
        assert paid.delivery_pending
        reader._api.get_split_access_snapshot = AsyncMock(side_effect=AssertionError('offline replay'))
        replay = await checkout.checkout_daily_resume(reader, **request)
        assert replay.replayed and replay.transaction_id == paid.transaction_id
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def paid_args(db, owner):
    quote = await core().quote_daily_resume(db, **owner)
    return dict(
        **owner,
        operation_key=str(uuid4()),
        expected_price_kopeks=quote.price_kopeks,
        expected_terms_hash=quote.terms_hash,
    )


@pytest.mark.parametrize('fault', ['price', 'end', 'last_charge', 'paused', 'status'])
async def test_changed_paid_source_never_debits_stale_quote(postgres_database, monkeypatch, fault):
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, tariff = await prepared(db, monkeypatch)
        args = await paid_args(db, owner)
        if fault == 'price':
            tariff.daily_price_kopeks += 1
        elif fault == 'end':
            sub.end_date -= timedelta(seconds=1)
        elif fault == 'last_charge':
            sub.last_daily_charge_at -= timedelta(seconds=1)
        elif fault == 'paused':
            sub.is_daily_paused = not sub.is_daily_paused
        else:
            sub.status = 'disabled'
        await db.commit()
        with pytest.raises(core().DailyResumeQuoteChanged):
            await core().resume_daily_from_balance(db, **args)
        assert (await db.get(User, sub.user_id)).balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('reset', [False, True])
async def test_disabled_long_future_window_cannot_pay_for_no_extension(postgres_database, monkeypatch, reset):
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch, reset=reset)
        sub.status = 'disabled'
        sub.end_date = datetime.now(UTC) + timedelta(days=4)
        await db.commit()
        with pytest.raises(core().LedgerConflict, match='already paid window'):
            await core().quote_daily_resume(db, **owner)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_bs_only_exhaustion_free_unpause_changes_no_paid_rights(postgres_database, monkeypatch):
    from sqlalchemy import update

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        sub.status, sub.is_daily_paused = 'limited', True
        sub.end_date = datetime.now(UTC) + timedelta(days=3)
        await db.execute(
            update(BsTrafficAllowance).where(BsTrafficAllowance.traffic_group == 'bs').values(remaining_bytes=0)
        )
        await db.commit()
        before = await snapshot(db, sub)
        assert not (await core().set_daily_pause(db, **owner, paused=False)).is_paused
        assert await snapshot(db, sub) == before
        with pytest.raises(core().LedgerConflict, match='already paid'):
            await core().quote_daily_resume(db, **owner)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('fault', ['foreign', 'generation', 'restricted', 'non_daily', 'implicit', 'attempted'])
async def test_daily_flags_keep_authorization_and_delivery_guards(postgres_database, monkeypatch, fault):
    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, tariff = await prepared(db, monkeypatch)
        args = dict(owner, paused=True)
        if fault == 'foreign':
            args['user_id'] = 2
        elif fault == 'generation':
            args['account_generation'] = 'f' * 64
        elif fault == 'restricted':
            (await db.get(User, sub.user_id)).restriction_subscription = True
        elif fault == 'non_daily':
            tariff.is_daily = False
        elif fault == 'implicit':
            args['paused'] = None
        else:
            paid = await core().resume_daily_from_balance(db, **await paid_args(db, owner))
            from unittest.mock import AsyncMock

            from app.services.bs_traffic.renewal_delivery import advance_renewal_delivery

            await db.commit()
            reader._api.restore_split_user_once = AsyncMock(side_effect=TimeoutError)
            await advance_renewal_delivery(reader, **owner, operation_id=paid.intent_id)
            receipt = await db.get(BsTrafficRenewalIntent, paid.intent_id, populate_existing=True)
            assert receipt.state == 'attempted'
        await db.commit()
        with pytest.raises(core().LedgerConflict):
            await core().set_daily_pause(db, **args)
        await db.refresh(sub)
        assert not sub.is_daily_paused


async def test_daily_resume_races_cron_without_second_day(postgres_database, monkeypatch):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        args = await paid_args(db, owner)
        due = sub.end_date
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)

        async def manual():
            async with factory() as session, session.begin():
                try:
                    return await core().resume_daily_from_balance(session, **args)
                except core().LedgerConflict:
                    return None

        async def cron():
            async with factory() as session, session.begin():
                return await renew_daily_from_balance(session, **owner, expected_due_at=due)

        await asyncio.gather(manual(), cron())
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        await db.refresh(sub)
        assert sub.end_date > datetime.now(UTC)


async def test_pause_races_cron_without_refund_or_future_charge(postgres_database, monkeypatch):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.services.bs_traffic.daily_renewal import renew_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        due = sub.end_date
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)

        async def pause():
            async with factory() as session, session.begin():
                return await core().set_daily_pause(session, **owner, paused=True)

        async def cron():
            async with factory() as session, session.begin():
                return await renew_daily_from_balance(session, **owner, expected_due_at=due)

        _, paid = await asyncio.gather(pause(), cron())
        await db.refresh(sub)
        assert sub.is_daily_paused
        count = await db.scalar(select(func.count()).select_from(Transaction))
        assert count == (1 if paid is not None else 0)
        assert await renew_daily_from_balance(db, **owner, expected_due_at=sub.end_date) is None


async def test_same_uuid_concurrent_manual_resumes_charge_once(postgres_database, monkeypatch):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, _, _ = await prepared(db, monkeypatch)
        args = await paid_args(db, owner)
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)

        async def pay():
            async with factory() as session, session.begin():
                return await core().resume_daily_from_balance(session, **args)

        results = await asyncio.gather(pay(), pay())
        assert sorted(row.replayed for row in results) == [False, True]
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_resume_uses_existing_device_group_price_without_offer(postgres_database, monkeypatch):
    from app.database.models import PromoGroup

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, tariff = await prepared(db, monkeypatch)
        tariff.device_limit, tariff.device_price_kopeks = 1, 300
        sub.device_limit, sub.daily_devices_billing_version = 3, 1
        group = PromoGroup(name='manual daily price', period_discounts={'1': 25}, device_discount_percent=50)
        db.add(group)
        await db.flush()
        user = await db.get(User, sub.user_id)
        user.promo_group_id = group.id
        user.promo_offer_discount_percent = 90
        user.promo_offer_discount_expires_at = datetime.now(UTC) + timedelta(days=1)
        await db.commit()
        args = await paid_args(db, owner)
        assert args['expected_price_kopeks'] == 85
        result = await core().resume_daily_from_balance(db, **args)
        assert result.charged_kopeks == 85
        assert sub.device_limit == 3 and sub.daily_devices_billing_version == 1
        assert user.promo_offer_discount_percent == 90


async def test_paid_resume_keeps_topup_terms_and_old_pending_eligibility(postgres_database, monkeypatch):
    from app.services.bs_traffic.ledger import grant, open_usage, resolve_usage

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        now = datetime.now(UTC)
        old = await db.scalar(select(BsTrafficAllowance).where(BsTrafficAllowance.traffic_group == 'bs'))
        pending = await open_usage(
            db,
            **owner,
            group='bs',
            source_key='pending-before-manual-resume',
            amount_bytes=100,
            interval_start=old.starts_at,
            interval_end=old.starts_at + timedelta(microseconds=1),
        )
        topup = await grant(
            db,
            **owner,
            group='bs',
            operation_key='topup:manual-daily',
            amount_bytes=777,
            starts_at=now,
            expires_at=now + timedelta(days=7),
        )
        prior = await db.get(BsTrafficAllowance, topup)
        frozen = (prior.remaining_bytes, prior.expires_at, prior.retired_at)
        result = await core().resume_daily_from_balance(db, **await paid_args(db, owner))
        new = await db.get(BsTrafficAllowance, result.grants['bs'])
        await resolve_usage(db, **owner, usage_id=pending, decision='charge')
        assert new.remaining_bytes == 5 * 1024**3
        assert (prior.remaining_bytes, prior.expires_at, prior.retired_at) == frozen


async def test_daily_payment_failure_rolls_back_flags_time_and_grants(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    from app.services.bs_traffic import renewal

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        sub.is_daily_paused = True
        await db.commit()
        before = await snapshot(db, sub)
        args = await paid_args(db, owner)
        monkeypatch.setattr(renewal, 'create_transaction', AsyncMock(side_effect=RuntimeError('receipt failure')))
        with pytest.raises(RuntimeError, match='receipt failure'):
            await core().resume_daily_from_balance(db, **args)
        await db.refresh(sub)
        assert sub.is_daily_paused
        assert await snapshot(db, sub) == before
        assert (await db.get(User, sub.user_id)).balance_kopeks == 10000
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('attempted', [False, True])
async def test_manual_daily_resume_uses_existing_grace_restore_or_blocks_attempt(
    postgres_database, monkeypatch, attempted
):
    from app.database.models import GraceAccessSessionModel, Subscription, Tariff
    from app.services.bs_traffic.renewal_grace import advance_paid_renewal
    from tests.database import test_bs_paid_renewal_grace_postgres as grace_fixture

    async def manual_payment(db, **args):
        sub = await db.get(Subscription, args['subscription_id'])
        tariff = await db.get(Tariff, sub.tariff_id)
        tariff.is_daily, tariff.daily_price_kopeks = True, 100
        if attempted:
            grace = await db.scalar(select(GraceAccessSessionModel))
            grace.split_overlay_attempt = {'state': 'attempted'}
        await db.flush()
        target = {key: args[key] for key in ('user_id', 'subscription_id', 'account_generation')}
        return await core().resume_daily_from_balance(db, **await paid_args(db, target))

    monkeypatch.setattr(grace_fixture, 'renew_from_balance', manual_payment)
    async with postgres_session(postgres_database, TABLES) as db:
        if attempted:
            with pytest.raises(core().LedgerConflict, match='overlay attempt'):
                await grace_fixture.prepared(db, monkeypatch)
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 0
        else:
            reader, keys, remote, grace_id, paid = await grace_fixture.prepared(db, monkeypatch)
            assert paid.charged_kopeks == 100
            target = {key: keys[key] for key in ('user_id', 'subscription_id', 'account_generation')}
            overlay = remote['state']
            await core().set_daily_pause(db, **target, paused=True)
            await core().set_daily_pause(db, **target, paused=False)
            await db.commit()
            assert remote['state'] == overlay
            assert (await db.get(GraceAccessSessionModel, grace_id)).state == 'active'
            assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
            assert await advance_paid_renewal(reader, **keys) == 'verified'
            grace = await db.get(GraceAccessSessionModel, grace_id, populate_existing=True)
            assert grace.state == 'completed' and grace.completion_reason == 'paid'
            assert remote['state'].expire_at == paid.new_end_date
            reader._api.restore_split_user_once.assert_awaited_once()


@pytest.mark.parametrize('action', ['quote', 'pay'])
async def test_live_topology_change_blocks_daily_quote_and_payment(postgres_database, monkeypatch, action):
    from app.services.bs_traffic import daily_control_checkout as checkout

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _ = await prepared(db, monkeypatch)
        quote = await checkout.prepare_daily_resume(reader, user_id=owner['user_id'], subscription_id=sub.id)
        original = reader._api.get_internal_squad_topology.side_effect

        async def changed(squad):
            value = await original(squad)
            value['accessibleNodes'][0]['activeInbounds'] = ['changed-runtime-inbound']
            return value

        reader._api.get_internal_squad_topology.side_effect = changed
        with pytest.raises(core().LedgerConflict):
            if action == 'quote':
                await checkout.prepare_daily_resume(reader, user_id=owner['user_id'], subscription_id=sub.id)
            else:
                await checkout.checkout_daily_resume(
                    reader,
                    user_id=owner['user_id'],
                    subscription_id=sub.id,
                    operation_key=str(uuid4()),
                    expected_price_kopeks=quote.price_kopeks,
                    expected_terms_hash=quote.terms_hash,
                )
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_fully_discounted_explicit_resume_works_with_automatic_daily_disabled(postgres_database, monkeypatch):
    from app.config import settings
    from app.database.models import PromoGroup

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        monkeypatch.setattr(settings, 'DAILY_SUBSCRIPTIONS_ENABLED', False)
        group = PromoGroup(name='manual free daily group', period_discounts={'1': 100})
        db.add(group)
        await db.flush()
        (await db.get(User, sub.user_id)).promo_group_id = group.id
        await db.commit()
        with pytest.raises(core().DailyResumeConsentRequired):
            await core().set_daily_pause(db, **owner, paused=False)
        args = await paid_args(db, owner)
        assert args['expected_price_kopeks'] == 0
        paid = await core().resume_daily_from_balance(db, **args)
        assert paid.charged_kopeks == 0 and paid.new_end_date > datetime.now(UTC)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        assert (await core().resume_daily_from_balance(db, **args)).replayed


async def test_disabled_short_paid_window_is_extended_without_truncation(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _ = await prepared(db, monkeypatch)
        sub.status = 'disabled'
        sub.end_date = datetime.now(UTC) + timedelta(hours=6)
        before = sub.end_date
        await db.commit()
        paid = await core().resume_daily_from_balance(db, **await paid_args(db, owner))
        assert paid.old_end_date == before and paid.new_end_date > before
        assert sub.status == 'active'


async def test_historical_daily_lookup_rejects_mismatched_receipt_terms(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, _, _ = await prepared(db, monkeypatch)
        args = await paid_args(db, owner)
        paid = await core().resume_daily_from_balance(db, **args)
        row = await db.get(BsTrafficRenewalIntent, paid.intent_id)
        row.parameters = dict(row.parameters, period_days=2)
        await db.commit()
        with pytest.raises(core().LedgerConflict, match='receipt does not match'):
            await core().find_daily_resume_by_operation(
                db,
                user_id=owner['user_id'],
                subscription_id=owner['subscription_id'],
                operation_key=args['operation_key'],
            )
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
