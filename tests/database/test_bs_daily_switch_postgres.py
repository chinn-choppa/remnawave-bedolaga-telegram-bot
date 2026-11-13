"""Daily switch charges only the proven remaining window, on PostgreSQL."""

from uuid import uuid4

import pytest

from app.config import settings
from app.database.models import BsTrafficLedger, BsTrafficRenewalIntent, Tariff
from app.services.bs_traffic.daily_funding import resolve_daily_funding
from app.services.bs_traffic.daily_renewal import renew_daily_from_balance
from app.services.bs_traffic.evidence import collect_topology_evidence
from app.services.bs_traffic.tariff_switch import quote_tariff_switch, switch_tariff_from_balance
from tests.database.test_bs_daily_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def setup_daily(db, monkeypatch):
    reader, owner, sub, old = await prepared(db, monkeypatch)
    await renew_daily_from_balance(db, **owner, expected_due_at=sub.end_date)
    new = Tariff(
        name='Daily target',
        is_active=True,
        is_daily=True,
        daily_price_kopeks=150,
        traffic_limit_gb=old.traffic_limit_gb,
        device_limit=old.device_limit,
        allowed_squads=list(old.allowed_squads),
        traffic_reset_mode='NO_RESET',
        bs_traffic_settings=dict(old.bs_traffic_settings),
    )
    db.add(new)
    monkeypatch.setattr(settings, 'TARIFF_SWITCH_UPGRADE_ENABLED', True)
    monkeypatch.setattr(settings, 'TARIFF_SWITCH_DOWNGRADE_ENABLED', True)
    await db.commit()
    policy = (await db.get(BsTrafficLedger, sub.id)).access_policy
    evidence = await collect_topology_evidence(
        reader,
        panel_identity=reader.panel_scope,
        ordinary_squads=policy['topology']['ordinary_squads'],
        bs_squads=policy['topology']['bs_squads'],
    )
    return reader, owner, sub, old, new, evidence


async def test_daily_switch_chain_full_rate_and_no_extension(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, old, new, evidence = await setup_daily(db, monkeypatch)
        end, charge = sub.end_date, sub.last_daily_charge_at
        for tariff, expected_rate in [(new, 150), (old, 100), (new, 150)]:
            quote = await quote_tariff_switch(db, **owner, new_tariff_id=tariff.id, evidence=evidence)
            assert quote.billing_kind == 'daily'
            assert quote.target_daily_rate_kopeks == expected_rate
            assert quote.price_kopeks == (49 if expected_rate == 150 else 0)
            args = dict(
                **owner,
                new_tariff_id=tariff.id,
                evidence=evidence,
                expected_price_kopeks=quote.price_kopeks,
                expected_terms_hash=quote.terms_hash,
                operation_key=str(uuid4()),
            )
            result = await switch_tariff_from_balance(db, **args)
            await db.commit()
            assert sub.end_date == end and sub.last_daily_charge_at == charge
            source = await resolve_daily_funding(db, state=await db.get(BsTrafficLedger, sub.id), sub=sub)
            assert source.full_rate_kopeks == expected_rate
            receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
            assert receipt.parameters['daily_funding']['full_rate_kopeks'] == expected_rate
            replay = await switch_tariff_from_balance(db, **args)
            assert replay.replayed


@pytest.mark.parametrize('adapter_name', ['bot', 'cabinet'])
async def test_both_adapters_share_daily_quote_charge_and_offline_replay(postgres_database, monkeypatch, adapter_name):
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from app.cabinet.routes.subscription_modules import split_tariff_switch as cabinet
    from app.cabinet.schemas.subscription import TariffSwitchRequest
    from app.database.models import User
    from app.handlers.subscription import split_tariff_switch as bot
    from app.services.bs_traffic import tariff_switch_checkout as checkout

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _, new, _ = await setup_daily(db, monkeypatch)
        sub_id, tariff_id = sub.id, new.id
        end, charge = sub.end_date, sub.last_daily_charge_at
        user = await db.get(User, owner['user_id'])
        await db.commit()

        @asynccontextmanager
        async def bound(*args):
            yield reader

        monkeypatch.setattr(bot, 'purchase_reader', bound)
        monkeypatch.setattr(cabinet, '_reader', bound)
        monkeypatch.setattr(checkout, 'advance_paid_renewal', AsyncMock(return_value='verified'))
        monkeypatch.setattr(checkout, '_emit_effects', AsyncMock())
        data = {}

        async def update(**values):
            data.update(values)

        state = SimpleNamespace(get_data=AsyncMock(side_effect=lambda: data), update_data=update)
        callback = SimpleNamespace(answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock()))
        assert await bot.preview_split_tariff_switch(
            callback, user, db, state, subscription=sub, new_tariff_id=tariff_id
        )
        text = callback.message.edit_text.await_args.args[0]
        assert 'остаток оплаченных суток' in text and 'UTC' in text and '1.5' in text
        saved = data['split_tariff_switch_confirmation']
        api_quote = await cabinet.preview(db, user_id=owner['user_id'], subscription_id=sub_id, new_tariff_id=tariff_id)
        assert api_quote['billing_kind'] == 'daily'
        assert api_quote['source_daily_rate_kopeks'] == 100
        assert api_quote['target_daily_rate_kopeks'] == 150
        assert api_quote['upgrade_cost_kopeks'] == saved['price_kopeks'] == 49
        assert api_quote['expected_terms_hash'] == saved['terms_hash']
        if adapter_name == 'bot':
            callback.data = f'swt:{sub_id}:{saved["operation_key"]}'
            await bot.handle_split_tariff_switch_confirm(callback, user, db, state)
        else:
            request = TariffSwitchRequest(
                tariff_id=tariff_id,
                period_days=1,
                operation_id=saved['operation_key'],
                expected_price_kopeks=saved['price_kopeks'],
                expected_terms_hash=saved['terms_hash'],
            )
            result = await cabinet.execute(db, user_id=owner['user_id'], subscription_id=sub_id, request=request)
            assert result['charged_kopeks'] == 49
        from app.database.models import Subscription

        await db.rollback()
        sub = await db.get(Subscription, sub_id, populate_existing=True)
        assert sub.end_date == end and sub.last_daily_charge_at == charge
        assert sub.tariff_id == tariff_id
        reader._api.get_internal_squad_topology.side_effect = AssertionError('Offline replay requested panel')
        replay = await switch_tariff_from_balance(
            db,
            **owner,
            new_tariff_id=tariff_id,
            expected_price_kopeks=saved['price_kopeks'],
            expected_terms_hash=saved['terms_hash'],
            operation_key=saved['operation_key'],
            evidence=None,
        )
        assert replay.replayed


@pytest.mark.parametrize('fault', ['hash', 'payment', 'source_digest', 'root', 'cycle', 'window', 'device', 'legacy'])
async def test_corrupt_switch_source_rejects_before_next_debit(postgres_database, monkeypatch, fault):
    from copy import deepcopy

    from sqlalchemy import func, select

    from app.database.models import Transaction
    from app.services.bs_traffic.enrollment import terms_hash
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, old, new, evidence = await setup_daily(db, monkeypatch)
        quote = await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        result = await switch_tariff_from_balance(
            db,
            **owner,
            new_tariff_id=new.id,
            evidence=evidence,
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
            operation_key=str(uuid4()),
        )
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        values = deepcopy(receipt.parameters)
        if fault == 'hash':
            values['switch_terms_hash'] = 'a' * 64
        elif fault == 'payment':
            tx = await db.get(Transaction, result.transaction_id)
            tx.amount_kopeks -= 1
        elif fault == 'legacy':
            values.pop('daily_funding')
        elif fault == 'device':
            sub.device_limit += 1
        elif fault == 'window':
            from datetime import timedelta

            sub.end_date += timedelta(seconds=1)
        else:
            snapshot = values['daily_funding']
            if fault == 'source_digest':
                snapshot['source']['previous']['digest'] = 'a' * 64
                values['switch_terms']['daily']['source']['digest'] = 'a' * 64
            elif fault == 'root':
                snapshot['source']['root']['transaction_id'] += 1
                values['switch_terms']['daily']['root']['transaction_id'] += 1
            elif fault == 'cycle':
                snapshot['source']['ownership_cycle'] += 1
            values['daily_funding_hash'] = terms_hash(snapshot)
            values['switch_terms_hash'] = terms_hash(values['switch_terms'])
        receipt.parameters = values
        await db.commit()
        with pytest.raises(LedgerConflict):
            await quote_tariff_switch(db, **owner, new_tariff_id=old.id, evidence=evidence)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2


@pytest.mark.parametrize('version', [0, 1])
async def test_target_device_clamp_keeps_version_and_offer(postgres_database, monkeypatch, version):
    from sqlalchemy import select

    from app.database.models import User
    from app.services.bs_traffic.enrollment import terms_hash

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, old, new, evidence = await setup_daily(db, monkeypatch)
        sub.device_limit = 7
        sub.daily_devices_billing_version = version
        new.device_limit = 7
        new.max_device_limit = 5
        new.device_price_kopeks = 300
        receipt = await db.scalar(select(BsTrafficRenewalIntent).order_by(BsTrafficRenewalIntent.id.desc()))
        snapshot = dict(receipt.parameters['daily_funding'], device_limit=7, daily_devices_billing_version=version)
        receipt.parameters = dict(receipt.parameters, daily_funding=snapshot, daily_funding_hash=terms_hash(snapshot))
        user = await db.get(User, owner['user_id'])
        user.promo_offer_discount_percent = 80
        await db.commit()
        quote = await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        assert quote.device_limit == 5
        assert quote.target_daily_rate_kopeks == 150
        result = await switch_tariff_from_balance(
            db,
            **owner,
            new_tariff_id=new.id,
            evidence=evidence,
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
            operation_key=str(uuid4()),
        )
        await db.commit()
        assert sub.daily_devices_billing_version == version
        assert user.promo_offer_discount_percent == 80
        source = await resolve_daily_funding(db, state=await db.get(BsTrafficLedger, sub.id), sub=sub)
        assert source.full_rate_kopeks == quote.target_daily_rate_kopeks
        assert result.charged_kopeks < quote.target_daily_rate_kopeks


def test_proration_exact_integer_floor_across_dst():
    from datetime import UTC, datetime, timedelta
    from zoneinfo import ZoneInfo

    from app.services.bs_traffic.daily_funding import prorated_daily_charge
    from app.services.bs_traffic.ledger import LedgerConflict

    now = datetime(2026, 3, 29, 0, 30, tzinfo=UTC)
    end = now + timedelta(hours=6)
    assert prorated_daily_charge(100, 150, end, now) == 12
    assert prorated_daily_charge(100, 150, end.astimezone(ZoneInfo('Europe/Berlin')), now) == 12
    assert prorated_daily_charge(150, 100, end, now) == 0
    assert prorated_daily_charge(100, 101, end, now) == 0
    with pytest.raises(LedgerConflict):
        prorated_daily_charge(100, 150, end, end)


def fixed_clock(monkeypatch, db, now):
    original = db.scalar
    clock = [now]

    async def scalar(statement, *args, **kwargs):
        if str(statement).startswith('SELECT clock_timestamp()'):
            return clock[0]
        return await original(statement, *args, **kwargs)

    monkeypatch.setattr(db, 'scalar', scalar)
    return clock


async def test_hash_stable_for_same_kopeck_changed_price_requires_new_consent(postgres_database, monkeypatch):
    from datetime import timedelta

    from sqlalchemy import func, select

    from app.database.models import Transaction
    from app.services.bs_traffic.tariff_switch import SwitchQuoteChanged

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _, new, evidence = await setup_daily(db, monkeypatch)
        new.daily_price_kopeks = 86500
        await db.commit()
        clock = fixed_clock(monkeypatch, db, sub.last_daily_charge_at + timedelta(seconds=1, microseconds=100))
        first = await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        clock[0] += timedelta(microseconds=1)
        same = await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        assert same.terms_hash == first.terms_hash
        clock[0] += timedelta(seconds=2)
        changed = await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        assert changed.price_kopeks == first.price_kopeks - 2
        assert changed.terms_hash != first.terms_hash
        with pytest.raises(SwitchQuoteChanged):
            await switch_tariff_from_balance(
                db,
                **owner,
                new_tariff_id=new.id,
                evidence=evidence,
                expected_price_kopeks=first.price_kopeks,
                expected_terms_hash=first.terms_hash,
                operation_key=str(uuid4()),
            )
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('discount', [25, 100])
async def test_next_cycle_uses_full_discounted_target_once(postgres_database, monkeypatch, discount):
    from datetime import timedelta

    from app.database.models import PromoGroup, User

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _, new, evidence = await setup_daily(db, monkeypatch)
        end, charge = sub.end_date, sub.last_daily_charge_at
        group = PromoGroup(name='daily switch discount', period_discounts={'1': discount})
        db.add(group)
        await db.flush()
        user = await db.get(User, owner['user_id'])
        user.promo_group_id = group.id
        user.promo_offer_discount_percent = 90
        user.promo_offer_discount_expires_at = end + timedelta(days=1)
        await db.commit()
        quote = await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        rate = 150 - 150 * discount // 100
        assert quote.target_daily_rate_kopeks == rate
        result = await switch_tariff_from_balance(
            db,
            **owner,
            new_tariff_id=new.id,
            evidence=evidence,
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
            operation_key=str(uuid4()),
        )
        await db.commit()
        assert sub.end_date == end and sub.last_daily_charge_at == charge
        fixed_clock(monkeypatch, db, end + timedelta(microseconds=1))
        renewed = await renew_daily_from_balance(db, **owner, expected_due_at=end)
        await db.commit()
        assert renewed.charged_kopeks == rate
        assert sub.end_date == end + timedelta(days=1, microseconds=1)
        assert sub.tariff_id == new.id
        assert user.promo_offer_discount_percent == 90
        replay = await renew_daily_from_balance(db, **owner, expected_due_at=end)
        assert replay.replayed and replay.intent_id == renewed.intent_id
        assert (
            await resolve_daily_funding(db, state=await db.get(BsTrafficLedger, sub.id), sub=sub)
        ).full_rate_kopeks == rate
        assert result.charged_kopeks <= max(0, rate - 100)


@pytest.mark.parametrize('same_operation', [True, False])
async def test_concurrent_daily_switches_one_charge(postgres_database, monkeypatch, same_operation):
    import asyncio

    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.database.models import Transaction
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _, new, evidence = await setup_daily(db, monkeypatch)
        quote = await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        request = dict(
            **owner,
            new_tariff_id=new.id,
            evidence=evidence,
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
        )
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        key = str(uuid4())

        async def worker(operation):
            async with factory() as session:
                try:
                    result = await switch_tariff_from_balance(session, **request, operation_key=operation)
                    await session.commit()
                    return result
                except LedgerConflict:
                    await session.rollback()
                    return None

        results = await asyncio.gather(worker(key), worker(key if same_operation else str(uuid4())))
        if same_operation:
            assert sum(bool(x.replayed) for x in results) == 1
        else:
            assert sum(x is None for x in results) == 1
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2


@pytest.mark.parametrize('fault', ['paused', 'expired', 'raw_zero', 'old_missing', 'mixed'])
async def test_daily_guards_do_not_debit(postgres_database, monkeypatch, fault):
    from datetime import timedelta

    from sqlalchemy import func, select

    from app.database.models import Transaction
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _, new, evidence = await setup_daily(db, monkeypatch)
        if fault == 'paused':
            sub.is_daily_paused = True
        elif fault == 'expired':
            sub.end_date = sub.last_daily_charge_at - timedelta(seconds=1)
        elif fault == 'raw_zero':
            new.daily_price_kopeks = 0
        elif fault == 'mixed':
            new.is_daily = False
        else:
            receipt = await db.scalar(select(BsTrafficRenewalIntent).order_by(BsTrafficRenewalIntent.id.desc()))
            receipt.parameters = dict(receipt.parameters, kind='tariff_switch')
        await db.commit()
        with pytest.raises(LedgerConflict):
            await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_telegram_daily_legacy_routes_and_all_locales(postgres_database, monkeypatch):
    import inspect
    from contextlib import asynccontextmanager
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from sqlalchemy import func, select

    from app.database.models import Transaction, User
    from app.handlers.subscription import split_tariff_switch as adapter, tariff_purchase

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _, new, _ = await setup_daily(db, monkeypatch)
        user = await db.get(User, owner['user_id'])

        @asynccontextmanager
        async def bound(*args):
            yield reader

        monkeypatch.setattr(adapter, 'purchase_reader', bound)
        monkeypatch.setattr(tariff_purchase, '_resolve_switch_subscription', AsyncMock(return_value=(sub, sub.id)))
        debit = AsyncMock(side_effect=AssertionError('Legacy full-day debit'))
        monkeypatch.setattr(tariff_purchase, 'subtract_user_balance', debit)
        data = {}

        async def update(**values):
            data.update(values)

        state = SimpleNamespace(
            get_data=AsyncMock(side_effect=lambda: data), update_data=update, clear=AsyncMock(side_effect=data.clear)
        )
        callback = SimpleNamespace(answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock()))
        for language in ['ru', 'en', 'fa', 'ua', 'zh']:
            user.language = language
            await db.commit()
            callback.data = f'daily_tariff_switch_confirm:{new.id}'
            await inspect.unwrap(tariff_purchase.confirm_daily_tariff_switch)(callback, user, db, state)
            text = callback.message.edit_text.await_args.args[0]
            assert 'UTC' in text and '{' not in text and '1.5' in text
            assert data['split_tariff_switch_confirmation']['price_kopeks'] == 49
        for handler, callback_data in [
            (tariff_purchase.select_tariff_switch, f'tariff_sw_select:{new.id}'),
            (tariff_purchase.select_tariff_switch_period, f'tariff_sw_period:{new.id}:1'),
            (tariff_purchase.confirm_tariff_switch, f'tariff_sw_confirm:{new.id}:1'),
        ]:
            callback.data = callback_data
            await inspect.unwrap(handler)(callback, user, db, state)
            assert data['split_tariff_switch_confirmation']['price_kopeks'] == 49
        callback.data = 'instant_switch'
        await inspect.unwrap(tariff_purchase.show_instant_switch_list)(callback, user, db, state)
        keyboard = callback.message.edit_text.await_args.kwargs['reply_markup']
        assert keyboard.inline_keyboard[0][0].callback_data == f'instant_sw_preview:{new.id}'
        assert keyboard.inline_keyboard[0][0].text == new.name
        debit.assert_not_awaited()
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('groups', [('ordinary',), ('bs',), ('ordinary', 'bs')])
@pytest.mark.parametrize('reset', [False, True])
async def test_daily_calendar_mode_roundtrip_keeps_funded_window(postgres_database, monkeypatch, groups, reset):
    from sqlalchemy import func, select

    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.ledger import grant, open_usage, resolve_usage
    from tests.database.test_bs_tariff_switch_modes_postgres import mode_observation

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, old, new, evidence = await setup_daily(db, monkeypatch)
        end, charge = sub.end_date, sub.last_daily_charge_at
        ledger = await db.get(BsTrafficLedger, sub.id)
        new.traffic_reset_mode = 'DAY'
        if 'ordinary' in groups:
            new.traffic_limit_gb = 0
        if 'bs' in groups:
            new.bs_traffic_settings = dict(new.bs_traffic_settings, limit_gb=0, topup_enabled=False)
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', reset)
        topups = {}
        pending = {}
        now = await db.scalar(select(func.clock_timestamp()))
        for group in groups:
            topups[group] = await grant(
                db,
                **owner,
                group=group,
                operation_key=f'daily-topup:{group}',
                amount_bytes=987,
                starts_at=now,
                expires_at=end,
            )
            pending[group] = await open_usage(
                db,
                **owner,
                group=group,
                source_key=f'daily-pending:{group}',
                amount_bytes=123,
                interval_start=charge,
                interval_end=now,
            )
        await db.commit()
        for tariff in [new, old]:
            quote = await quote_tariff_switch(db, **owner, new_tariff_id=tariff.id, evidence=evidence)
            result = await switch_tariff_from_balance(
                db,
                **owner,
                new_tariff_id=tariff.id,
                evidence=evidence,
                expected_price_kopeks=quote.price_kopeks,
                expected_terms_hash=quote.terms_hash,
                operation_key=str(uuid4()),
            )
            await db.commit()
            assert sub.end_date == end and sub.last_daily_charge_at == charge
            assert (
                await resolve_daily_funding(db, state=ledger, sub=sub)
            ).full_rate_kopeks == tariff.daily_price_kopeks
            if tariff is new:
                assert not set(groups).intersection(result.grants)
                for group in groups:
                    now = await db.scalar(select(func.clock_timestamp()))
                    await mode_observation(db, ledger, ledger.access_policy['topology'][f'{group}_nodes'][0], now, 5000)
                await db.commit()
            else:
                assert set(result.grants) == {'ordinary', 'bs'}
        for group in groups:
            topup = await db.get(BsTrafficAllowance, topups[group])
            before = topup.remaining_bytes
            await resolve_usage(db, **owner, usage_id=pending[group], decision='charge')
            assert topup.remaining_bytes == before == 987 and topup.expires_at == end and topup.retired_at is None


async def test_enrollment_root_with_independent_charge_anchor_switches_twice(postgres_database, monkeypatch):
    from datetime import UTC, datetime, timedelta
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.daily_funding import enrollment_daily_funding
    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from app.services.bs_traffic.enrollment_delivery import _transfer, advance_enrollment
    from app.services.bs_traffic.enrollment_source import prepare_ordinary_source
    from tests.database.test_bs_enrollment_postgres import ORDINARY, bound_panel, paid
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
    from tests.database.test_bs_traffic_policy_postgres import NODE_A, NODE_B

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    monkeypatch.setattr(settings, 'TARIFF_SWITCH_UPGRADE_ENABLED', True)
    monkeypatch.setattr(settings, 'TARIFF_SWITCH_DOWNGRADE_ENABLED', True)
    async with postgres_session(postgres_database, TABLES) as db:
        _, old, sub, tx = await paid(db, monkeypatch)
        old.is_daily = True
        old.daily_price_kopeks = 9999  # Mutable current price is not the purchased rate.
        sub.end_date = datetime.now(UTC) + timedelta(days=1)
        sub.last_daily_charge_at = datetime.now(UTC) + timedelta(microseconds=100)
        end, charge = sub.end_date, sub.last_daily_charge_at
        snapshot = enrollment_daily_funding(sub=sub, transaction=tx)
        job = await stage_paid_enrollment(db, sub, transaction=tx, daily_funding=snapshot)
        reader, _ = await bound_panel(db, sub, monkeypatch)
        reader._api.get_system_configuration = AsyncMock(
            return_value={
                'service': {'disableUserUsageRecords': False, 'cleanUsageHistory': False},
                'misc': {'userUsageIgnoreBelowBytes': 0},
            }
        )
        reader._api.get_internal_squad_topology = AsyncMock(
            side_effect=lambda squad: {
                'squadUuid': squad,
                'accessibleNodes': [
                    {
                        'uuid': NODE_A if squad == ORDINARY else NODE_B,
                        'configProfileUuid': '66666666-6666-4666-8666-666666666666',
                        'activeInbounds': ['ordinary' if squad == ORDINARY else 'bs'],
                    }
                ],
            }
        )
        await prepare_ordinary_source(reader, job.id)
        await _transfer(reader, job.id)
        await db.refresh(job)
        owner = dict(user_id=sub.user_id, subscription_id=sub.id, account_generation=job.account_generation)
        assert await advance_enrollment(reader, enrollment_id=job.id) != 'verified'
        for node in (NODE_A, NODE_B):
            await observe(db, reader, owner, node=node, amount=0)
        await db.commit()
        assert await advance_enrollment(reader, enrollment_id=job.id) == 'verified'
        await db.refresh(sub)
        new = Tariff(
            id=2,
            name='Enrollment next daily',
            is_active=True,
            is_daily=True,
            daily_price_kopeks=2500,
            traffic_limit_gb=old.traffic_limit_gb,
            device_limit=old.device_limit,
            allowed_squads=list(old.allowed_squads),
            traffic_reset_mode='DAY',
            bs_traffic_settings=dict(old.bs_traffic_settings),
        )
        db.add(new)
        await db.commit()
        ledger = await db.get(BsTrafficLedger, sub.id)
        evidence = await collect_topology_evidence(
            reader,
            panel_identity=reader.panel_scope,
            ordinary_squads=ledger.access_policy['topology']['ordinary_squads'],
            bs_squads=ledger.access_policy['topology']['bs_squads'],
        )
        old.daily_price_kopeks = 2600
        await db.commit()
        for tariff, source_rate in [(new, 2000), (old, 2500)]:
            quote = await quote_tariff_switch(db, **owner, new_tariff_id=tariff.id, evidence=evidence)
            assert quote.source_daily_rate_kopeks == source_rate
            await switch_tariff_from_balance(
                db,
                **owner,
                new_tariff_id=tariff.id,
                evidence=evidence,
                expected_price_kopeks=quote.price_kopeks,
                expected_terms_hash=quote.terms_hash,
                operation_key=str(uuid4()),
            )
            await db.commit()
            assert sub.end_date == end and sub.last_daily_charge_at == charge
            proof = await resolve_daily_funding(db, state=ledger, sub=sub)
            assert proof.full_rate_kopeks == tariff.daily_price_kopeks
            assert proof.root_source['kind'] == 'enrollment'


@pytest.mark.parametrize('first', ['switch', 'renew'])
async def test_daily_switch_autocharge_boundary_serializes_real_sessions(postgres_database, monkeypatch, first):
    import asyncio
    from datetime import UTC, datetime, timedelta

    from sqlalchemy import func, select
    from sqlalchemy.ext.asyncio import async_sessionmaker

    from app.database.models import Subscription, Transaction
    from app.services.bs_traffic.enrollment import terms_hash
    from app.services.bs_traffic.ledger import LedgerConflict, _lock

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _, new, evidence = await setup_daily(db, monkeypatch)
        # An exactly funded day immediately at its boundary; no wall-clock sleep.
        end = datetime.now(UTC) + timedelta(seconds=1)
        start = end - timedelta(days=1)
        sub.end_date = end
        sub.last_daily_charge_at = start
        root = await db.scalar(select(BsTrafficRenewalIntent).order_by(BsTrafficRenewalIntent.id.desc()))
        root.intended_expires_at = end
        snapshot = dict(
            root.parameters['daily_funding'],
            window_start=start.isoformat(),
            window_end=end.isoformat(),
            last_daily_charge_at=start.isoformat(),
        )
        root.parameters = dict(
            root.parameters,
            daily_funding=snapshot,
            daily_funding_hash=terms_hash(snapshot),
            old_end_date=(start - timedelta(seconds=1)).isoformat(),
            expected_due_at=(start - timedelta(seconds=1)).isoformat(),
        )
        await db.commit()
        fixed_clock(monkeypatch, db, end - timedelta(microseconds=1))
        quote = await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        assert quote.price_kopeks == 0 and quote.is_upgrade
        request = dict(
            **owner,
            new_tariff_id=new.id,
            evidence=evidence,
            expected_price_kopeks=0,
            expected_terms_hash=quote.terms_hash,
            operation_key=str(uuid4()),
        )
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        locked = asyncio.Event()
        contender_started = asyncio.Event()

        async def worker(kind):
            async with factory() as session:
                fixed_clock(
                    monkeypatch,
                    session,
                    end + timedelta(microseconds=1)
                    if kind == 'renew'
                    else end + timedelta(microseconds=2)
                    if first == 'renew'
                    else end - timedelta(microseconds=1),
                )
                try:
                    if kind == first:
                        await _lock(session, owner['user_id'], owner['subscription_id'], owner['account_generation'])
                        locked.set()
                        await contender_started.wait()
                    else:
                        await locked.wait()
                        contender_started.set()
                    result = (
                        await switch_tariff_from_balance(session, **request)
                        if kind == 'switch'
                        else await renew_daily_from_balance(session, **owner, expected_due_at=end)
                    )
                    await session.commit()
                    return result
                except LedgerConflict:
                    await session.rollback()
                    return None

        switched, renewed = await asyncio.gather(worker('switch'), worker('renew'))
        assert renewed is not None
        assert renewed.charged_kopeks == (150 if first == 'switch' else 100)
        assert (switched is None) == (first == 'renew')
        actual = await db.get(Subscription, owner['subscription_id'], populate_existing=True)
        assert actual.end_date == end + timedelta(days=1, microseconds=1)
        assert actual.last_daily_charge_at == end + timedelta(microseconds=1)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == (3 if first == 'switch' else 2)


async def test_resume_funding_with_device_category_discount_then_switch(postgres_database, monkeypatch):
    from app.database.models import PromoGroup, User
    from app.services.bs_traffic.daily_control import quote_daily_resume, resume_daily_from_balance

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, old = await prepared(db, monkeypatch)
        old.device_limit, old.device_price_kopeks = 1, 300
        sub.device_limit, sub.daily_devices_billing_version = 3, 1
        sub.is_daily_paused = True
        group = PromoGroup(name='daily source categories', period_discounts={'1': 25}, device_discount_percent=50)
        db.add(group)
        await db.flush()
        user = await db.get(User, owner['user_id'])
        user.promo_group_id = group.id
        await db.commit()
        resume_quote = await quote_daily_resume(db, **owner)
        assert resume_quote.price_kopeks == 85
        await resume_daily_from_balance(
            db,
            **owner,
            expected_price_kopeks=85,
            expected_terms_hash=resume_quote.terms_hash,
            operation_key=str(uuid4()),
        )
        new = Tariff(
            name='Next daily discounted',
            is_active=True,
            is_daily=True,
            daily_price_kopeks=200,
            device_limit=2,
            traffic_limit_gb=old.traffic_limit_gb,
            allowed_squads=list(old.allowed_squads),
            traffic_reset_mode='NO_RESET',
            bs_traffic_settings=dict(old.bs_traffic_settings),
        )
        db.add(new)
        monkeypatch.setattr(settings, 'TARIFF_SWITCH_UPGRADE_ENABLED', True)
        await db.commit()
        ledger = await db.get(BsTrafficLedger, sub.id)
        evidence = await collect_topology_evidence(
            reader,
            panel_identity=reader.panel_scope,
            ordinary_squads=ledger.access_policy['topology']['ordinary_squads'],
            bs_squads=ledger.access_policy['topology']['bs_squads'],
        )
        quote = await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        assert quote.source_daily_rate_kopeks == 85
        assert quote.target_daily_rate_kopeks == 150
        assert quote.device_limit == 2
        await switch_tariff_from_balance(
            db,
            **owner,
            new_tariff_id=new.id,
            evidence=evidence,
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
            operation_key=str(uuid4()),
        )
        await db.commit()
        funding = await resolve_daily_funding(db, state=ledger, sub=sub)
        assert funding.full_rate_kopeks == 150 and funding.root_source['kind'] == 'daily_resume'


async def test_daily_checkout_rechecks_after_network_before_debit(postgres_database, monkeypatch):
    from sqlalchemy import func, select

    from app.database.models import Transaction
    from app.services.bs_traffic import tariff_switch_checkout as checkout
    from app.services.bs_traffic.tariff_switch import SwitchQuoteChanged

    async with postgres_session(postgres_database, TABLES) as db:
        reader, owner, sub, _, new, _ = await setup_daily(db, monkeypatch)
        tariff_id = new.id
        quote = await checkout.prepare_tariff_switch(
            reader, user_id=owner['user_id'], subscription_id=sub.id, new_tariff_id=tariff_id
        )
        await db.commit()
        collect = checkout.collect_mode_baselines

        async def change_price(*args, **kwargs):
            result = await collect(*args, **kwargs)
            async with reader._factory() as editing:
                target = await editing.get(Tariff, tariff_id)
                target.daily_price_kopeks = 200
                await editing.commit()
            return result

        monkeypatch.setattr(checkout, 'collect_mode_baselines', change_price)
        with pytest.raises(SwitchQuoteChanged):
            await checkout.checkout_tariff_switch(
                reader,
                user_id=owner['user_id'],
                subscription_id=sub.id,
                new_tariff_id=tariff_id,
                expected_price_kopeks=quote.price_kopeks,
                expected_terms_hash=quote.terms_hash,
                operation_key=str(uuid4()),
            )
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_daily_calendar_refresh_uses_switched_modes(postgres_database, monkeypatch):
    from datetime import datetime

    from app.database.models import BsTrafficAllowance
    from app.services.bs_traffic.calendar_renewal import refresh_calendar_base

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _, new, evidence = await setup_daily(db, monkeypatch)
        end = sub.end_date
        new.traffic_limit_gb = 0
        new.traffic_reset_mode = 'DAY'
        await db.commit()
        quote = await quote_tariff_switch(db, **owner, new_tariff_id=new.id, evidence=evidence)
        result = await switch_tariff_from_balance(
            db,
            **owner,
            new_tariff_id=new.id,
            evidence=evidence,
            expected_price_kopeks=quote.price_kopeks,
            expected_terms_hash=quote.terms_hash,
            operation_key=str(uuid4()),
        )
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        due = datetime.fromisoformat(receipt.parameters['next_refresh_at'])
        assert due < end
        refreshed = await refresh_calendar_base(db, **owner, now=due)
        assert set(refreshed.grants) == {'bs'}
        assert (await db.get(BsTrafficAllowance, refreshed.grants['bs'])).remaining_bytes == new.bs_traffic_settings[
            'limit_gb'
        ] * 1024**3
        assert (await refresh_calendar_base(db, **owner, now=due)).status == 'not_due'
        assert sub.end_date == end


@pytest.mark.parametrize('handler_name', ['confirm_daily_tariff_switch', 'confirm_tariff_switch'])
async def test_legacy_daily_callback_keeps_unknown_operation_after_mode_change(
    postgres_database, monkeypatch, handler_name
):
    import inspect
    from types import SimpleNamespace
    from unittest.mock import AsyncMock

    from sqlalchemy import func, select

    from app.database.models import Transaction, User
    from app.handlers.subscription import tariff_purchase

    async with postgres_session(postgres_database, TABLES) as db:
        _, owner, sub, _, new, _ = await setup_daily(db, monkeypatch)
        sub.traffic_accounting_mode = 'managed'
        new.period_prices = {'1': 150}
        user = await db.get(User, owner['user_id'])
        saved = dict(
            user_id=user.id,
            subscription_id=sub.id,
            new_tariff_id=new.id,
            price_kopeks=49,
            terms_hash='a' * 64,
            operation_key=str(uuid4()),
        )
        data = {'split_tariff_switch_confirmation': saved, 'split_tariff_switch_attempted': True}
        await db.commit()
        monkeypatch.setattr(tariff_purchase, '_resolve_switch_subscription', AsyncMock(return_value=(sub, sub.id)))
        debit = AsyncMock(side_effect=AssertionError('Unknown split attempt reached legacy debit'))
        monkeypatch.setattr(tariff_purchase, 'subtract_user_balance', debit)
        state = SimpleNamespace(get_data=AsyncMock(return_value=data), update_data=AsyncMock())
        callback = SimpleNamespace(
            data=f'confirm:{new.id}:1', answer=AsyncMock(), message=SimpleNamespace(edit_text=AsyncMock())
        )
        await inspect.unwrap(getattr(tariff_purchase, handler_name))(callback, user, db, state)
        assert 'Повторная оплата не нужна' in callback.message.edit_text.await_args.args[0]
        button = callback.message.edit_text.await_args.kwargs['reply_markup'].inline_keyboard[0][0]
        assert button.callback_data == f'swt:{sub.id}:{saved["operation_key"]}'
        debit.assert_not_awaited()
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
