"""A calendar switch replaces future funded cadence, not paid time or top-ups."""

from datetime import timedelta
from zoneinfo import ZoneInfo

import pytest
from sqlalchemy import func, select

from app.config import settings
from app.database.models import BsTrafficAllowance, BsTrafficRenewalIntent, Tariff, TrafficAccountingState, Transaction
from app.services.bs_traffic.calendar_renewal import current_interval, refresh_calendar_base
from app.services.bs_traffic.ledger import grant
from app.services.bs_traffic.renewal import renew_from_balance
from app.services.traffic_accounting_schedule import next_base_refresh
from tests.database.test_bs_tariff_switch_postgres import core, quote_and_args, setup_switch
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
STRATEGIES = ['DAY', 'WEEK', 'MONTH', 'MONTH_ROLLING', 'NO_RESET']


async def calendar_setup(db, monkeypatch, old_strategy, new_strategy):
    reader, target, sub, new, evidence = await setup_switch(db, monkeypatch)
    old = await db.get(Tariff, sub.tariff_id)
    old.traffic_reset_mode = old_strategy
    new.traffic_reset_mode = new_strategy
    await db.commit()
    seed = await renew_from_balance(
        db, **target, period_days=30, expected_price_kopeks=2000, operation_key='calendar-seed'
    )
    await db.commit()
    return reader, target, sub, new, evidence, seed


@pytest.mark.parametrize('reset', [False, True])
@pytest.mark.parametrize(
    'old_strategy,new_strategy', list(zip(STRATEGIES, STRATEGIES[1:] + STRATEGIES[:1], strict=True))
)
async def test_calendar_switch_preserves_paid_terms_and_replays(
    postgres_database, monkeypatch, old_strategy, new_strategy, reset
):
    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence, seed = await calendar_setup(db, monkeypatch, old_strategy, new_strategy)
        old_base = await db.get(BsTrafficAllowance, seed.grants['ordinary'])
        old_base.remaining_bytes = 2 * 1024**3
        source = await db.get(TrafficAccountingState, sub.id)
        source_terms = (source.calendar_anchor, source.calendar_timezone, sub.end_date)
        now = await db.scalar(select(func.clock_timestamp()))
        topup_id = await grant(
            db,
            **target,
            group='ordinary',
            operation_key='topup:calendar',
            amount_bytes=123,
            starts_at=now,
            expires_at=sub.end_date,
        )
        topup = await db.get(BsTrafficAllowance, topup_id)
        topup_terms = (topup.remaining_bytes, topup.starts_at, topup.expires_at, topup.retired_at)
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', reset)
        await db.commit()
        _, args = await quote_and_args(db, target, new, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        await db.commit()
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        assert receipt.parameters['calendar_strategy'] == new_strategy
        assert receipt.parameters['switch_terms']['source_calendar']['receipt_id'] == seed.intent_id
        base = await db.get(BsTrafficAllowance, result.grants['ordinary'])
        assert base.remaining_bytes == (20 if reset else 12) * 1024**3
        boundary = next_base_refresh(
            new_strategy, now=base.starts_at, anchor=source.calendar_anchor, timezone=ZoneInfo(source.calendar_timezone)
        )
        assert base.expires_at == (min(sub.end_date, boundary) if boundary else sub.end_date)
        assert (source.calendar_anchor, source.calendar_timezone, sub.end_date) == source_terms
        assert (topup.remaining_bytes, topup.starts_at, topup.expires_at, topup.retired_at) == topup_terms
        replay = await core().switch_tariff_from_balance(db, **args)
        assert replay.replayed and replay.intent_id == result.intent_id
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2


async def test_due_old_calendar_refreshes_before_no_reset_carry(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence, seed = await calendar_setup(db, monkeypatch, 'DAY', 'NO_RESET')
        source = await db.get(TrafficAccountingState, sub.id)
        now = await db.scalar(select(func.clock_timestamp()))
        due, _ = current_interval(
            'DAY', now=now, anchor=source.calendar_anchor, timezone=ZoneInfo(source.calendar_timezone)
        )
        receipt = await db.get(BsTrafficRenewalIntent, seed.intent_id)
        receipt.parameters = dict(receipt.parameters, next_refresh_at=due.isoformat())
        for base in await db.scalars(select(BsTrafficAllowance)):
            if base.retired_at is not None:
                base.retired_at = due - timedelta(hours=1)
            base.starts_at = due - timedelta(days=1)
            base.expires_at = due
            base.remaining_bytes = 0
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', False)
        await db.commit()
        _, args = await quote_and_args(db, target, new, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        await db.commit()
        assert (await db.get(BsTrafficAllowance, result.grants['ordinary'])).remaining_bytes == 20 * 1024**3
        assert receipt.parameters['calendar_last_boundary'] == due.isoformat()
        before = await db.scalar(select(func.count()).select_from(BsTrafficAllowance))
        refreshed = await refresh_calendar_base(db, **target, now=now + timedelta(days=2))
        assert refreshed.status == 'not_due'
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == before


@pytest.mark.parametrize('source_strategy', ['NO_RESET', 'MONTH'])
async def test_confirmed_excess_ends_at_first_target_calendar_reset(postgres_database, monkeypatch, source_strategy):
    from datetime import datetime

    from app.services.bs_traffic.ledger import open_usage, resolve_usage

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence, seed = await calendar_setup(db, monkeypatch, source_strategy, 'DAY')
        old_base = await db.get(BsTrafficAllowance, seed.grants['ordinary'])
        old_base.remaining_bytes = 0
        new.traffic_limit_gb = 5
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', False)
        # Pending old usage keeps the old eligible IDs; it never follows calendar replacement.
        pending = await open_usage(
            db,
            **target,
            group='ordinary',
            source_key='calendar:pending',
            amount_bytes=100,
            interval_start=old_base.starts_at,
            interval_end=old_base.starts_at + timedelta(microseconds=1),
        )
        await db.commit()
        _, args = await quote_and_args(db, target, new, evidence)
        switched = await core().switch_tariff_from_balance(db, **args)
        assert (await db.get(BsTrafficAllowance, switched.grants['ordinary'])).remaining_bytes == 0
        receipt = await db.get(BsTrafficRenewalIntent, switched.intent_id)
        due = datetime.fromisoformat(receipt.parameters['next_refresh_at'])
        fresh = await refresh_calendar_base(db, **target, now=due)
        assert (await db.get(BsTrafficAllowance, fresh.grants['ordinary'])).remaining_bytes == 5 * 1024**3
        await resolve_usage(db, **target, usage_id=pending, decision='charge')
        assert (await db.get(BsTrafficAllowance, fresh.grants['ordinary'])).remaining_bytes == 5 * 1024**3
        again = await refresh_calendar_base(db, **target, now=due)
        assert again.status == 'not_due'


@pytest.mark.parametrize('fault', ['mode', 'source_due', 'source_payment', 'stopped_bs'])
async def test_calendar_change_rejects_stale_or_combined_terms_before_money(postgres_database, monkeypatch, fault):
    from app.database.models import BsTrafficLedger
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence, seed = await calendar_setup(db, monkeypatch, 'DAY', 'WEEK')
        _, args = await quote_and_args(db, target, new, evidence)
        source = await db.get(BsTrafficRenewalIntent, seed.intent_id)
        if fault == 'mode':
            new.traffic_limit_gb = 0
        elif fault == 'source_due':
            source.parameters = dict(source.parameters, next_refresh_at=sub.end_date.isoformat())
        elif fault == 'source_payment':
            (await db.get(Transaction, seed.transaction_id)).is_completed = False
        else:
            state = await db.get(BsTrafficLedger, sub.id)
            state.access_policy = dict(state.access_policy, bs_purchase_enabled=False)
        await db.commit()
        with pytest.raises(LedgerConflict):
            await core().switch_tariff_from_balance(db, **args)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_concurrent_calendar_confirmation_has_one_replacement(postgres_database, monkeypatch):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, _, new, evidence, _ = await calendar_setup(db, monkeypatch, 'DAY', 'WEEK')
        _, args = await quote_and_args(db, target, new, evidence)
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)

        async def confirm():
            async with factory() as session, session.begin():
                return await core().switch_tariff_from_balance(session, **args)

        results = await asyncio.gather(confirm(), confirm())
        assert results[0].intent_id == results[1].intent_id
        assert sorted(item.replayed for item in results) == [False, True]
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2


async def test_initial_paid_enrollment_calendar_switch_without_renewal_receipt(postgres_database, monkeypatch):
    from unittest.mock import AsyncMock

    from app.services.bs_traffic.enrollment import stage_paid_enrollment
    from app.services.bs_traffic.enrollment_delivery import _transfer, advance_enrollment
    from app.services.bs_traffic.enrollment_source import prepare_ordinary_source
    from app.services.bs_traffic.evidence import collect_topology_evidence
    from tests.database.test_bs_enrollment_postgres import BS, ORDINARY, bound_panel, paid
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
    from tests.database.test_bs_traffic_policy_postgres import NODE_A, NODE_B

    async with postgres_session(postgres_database, TABLES) as db:
        user, old, sub, tx = await paid(db, monkeypatch)
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
        job = await stage_paid_enrollment(db, sub, transaction=tx)
        reader, _ = await bound_panel(db, sub, monkeypatch, used=0)
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
        target = dict(user_id=user.id, subscription_id=sub.id, account_generation=job.account_generation)
        for node in (NODE_A, NODE_B):
            await observe(db, reader, target, node=node, amount=0)
        await db.commit()
        assert await advance_enrollment(reader, enrollment_id=job.id) == 'verified'
        await db.refresh(sub)
        new = Tariff(
            id=2,
            name='Calendar only',
            is_active=True,
            period_prices={'30': 4000},
            traffic_limit_gb=20,
            device_limit=2,
            allowed_squads=[ORDINARY],
            traffic_reset_mode='WEEK',
            bs_traffic_settings=dict(old.bs_traffic_settings),
        )
        db.add(new)
        monkeypatch.setattr(settings, 'TARIFF_SWITCH_UPGRADE_ENABLED', True)
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', True)
        await db.commit()
        evidence = await collect_topology_evidence(
            reader, panel_identity=reader.panel_scope, ordinary_squads=[ORDINARY], bs_squads=[BS]
        )
        assert await db.scalar(select(func.count()).select_from(BsTrafficRenewalIntent)) == 0
        _, args = await quote_and_args(db, target, new, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        assert {item['kind'] for item in receipt.parameters['switch_terms']['source_calendar']['sources']} == {
            'transfer',
            'enrollment',
        }
        assert set(result.grants) == {'ordinary', 'bs'}


@pytest.mark.parametrize('strategy', ['DAY', 'MONTH_ROLLING'])
async def test_target_calendar_keeps_timezone_and_month_end_anchor(postgres_database, monkeypatch, strategy):
    from datetime import UTC, datetime

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        source = await db.get(TrafficAccountingState, sub.id)
        source.calendar_anchor = datetime(2026, 1, 31, 12, tzinfo=UTC)
        source.calendar_timezone = 'Europe/Berlin'
        from app.database.models import User

        (await db.get(User, sub.user_id)).balance_kopeks = 1_000_000
        sub.end_date = datetime(2028, 1, 1, tzinfo=UTC)
        new.traffic_reset_mode = strategy
        await db.commit()
        await renew_from_balance(
            db, **target, period_days=30, expected_price_kopeks=2000, operation_key='timezone-seed'
        )
        await db.commit()
        _, args = await quote_and_args(db, target, new, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        assert receipt.parameters['calendar_anchor'] == source.calendar_anchor.isoformat()
        assert receipt.parameters['calendar_timezone'] == 'Europe/Berlin'
        timezone = ZoneInfo('Europe/Berlin')
        if strategy == 'DAY':
            before = datetime(2027, 3, 28, 0, 5, tzinfo=timezone).astimezone(UTC)
            first = await refresh_calendar_base(db, **target, now=before)
            assert first.starts_at == before
            assert first.next_refresh_at - before == timedelta(hours=23)
        else:
            before = datetime(2027, 2, 28, 0, 10, tzinfo=timezone).astimezone(UTC)
            first = await refresh_calendar_base(db, **target, now=before)
            assert first.starts_at == before
            assert first.next_refresh_at == datetime(2027, 3, 31, 0, 10, tzinfo=timezone).astimezone(UTC)


async def test_paid_strategy_outlives_mutable_old_tariff_configuration(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence, seed = await calendar_setup(db, monkeypatch, 'DAY', 'MONTH')
        (await db.get(Tariff, sub.tariff_id)).traffic_reset_mode = 'MONTH'
        await db.commit()
        _, args = await quote_and_args(db, target, new, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        assert receipt.parameters['switch_terms']['source_calendar']['receipt_id'] == seed.intent_id
        assert receipt.parameters['switch_terms']['source_calendar']['sources'][0]['strategy'] == 'DAY'


async def test_fleet_replaces_cached_old_calendar_due_after_switch(postgres_database, monkeypatch):
    from datetime import datetime
    from types import SimpleNamespace

    from app.services.bs_traffic.fleet_settlement import _calendar

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence, seed = await calendar_setup(db, monkeypatch, 'MONTH', 'DAY')
        old = await db.get(BsTrafficRenewalIntent, seed.intent_id)
        work = SimpleNamespace(
            calendar_receipt_id=seed.intent_id,
            calendar_due_at=datetime.fromisoformat(old.parameters['next_refresh_at']),
        )
        _, args = await quote_and_args(db, target, new, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        now = await db.scalar(select(func.clock_timestamp()))
        await _calendar(db, work, target, now, sub)
        assert work.calendar_receipt_id == result.intent_id
        assert work.calendar_due_at == datetime.fromisoformat(receipt.parameters['next_refresh_at'])


async def test_no_calendar_receipt_cannot_use_mutable_tariff_as_source(postgres_database, monkeypatch):
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        source = await db.get(TrafficAccountingState, sub.id)
        assert source.business_strategy == 'NO_RESET'
        from app.database.models import BsTrafficManagedTransfer

        (await db.get(BsTrafficManagedTransfer, sub.id)).calendar_terms = None
        (await db.get(Tariff, sub.tariff_id)).traffic_reset_mode = 'MONTH'
        new.traffic_reset_mode = 'MONTH'
        await db.commit()
        with pytest.raises(LedgerConflict):
            await quote_and_args(db, target, new, evidence)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


@pytest.mark.parametrize('fault', ['identity', 'revision', 'phase', 'strategy'])
async def test_unreceipted_source_provenance_change_rejects_switch(postgres_database, monkeypatch, fault):
    from app.database.models import BsTrafficManagedTransfer
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, sub, new, evidence = await setup_switch(db, monkeypatch)
        (await db.get(BsTrafficManagedTransfer, sub.id)).calendar_terms = None
        source = await db.get(TrafficAccountingState, sub.id)
        if fault == 'identity':
            source.panel_identity = 'changed-account'
        elif fault == 'revision':
            source.revision += 1
        elif fault == 'phase':
            source.phase = 'ready'
        else:
            source.business_strategy = 'invalid'
        await db.commit()
        with pytest.raises(LedgerConflict):
            await quote_and_args(db, target, new, evidence)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 0


async def test_malformed_funded_calendar_rejects_same_strategy_switch(postgres_database, monkeypatch):
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, _, new, evidence, seed = await calendar_setup(db, monkeypatch, 'DAY', 'DAY')
        (await db.get(BsTrafficRenewalIntent, seed.intent_id)).parameters = []
        await db.commit()
        with pytest.raises(LedgerConflict):
            await quote_and_args(db, target, new, evidence)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('fault', ['terms', 'generation', 'tariff', 'funding'])
async def test_same_calendar_rejects_invalid_funded_authority(postgres_database, monkeypatch, fault):
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        _, target, _, new, evidence, seed = await calendar_setup(db, monkeypatch, 'DAY', 'DAY')
        receipt = await db.get(BsTrafficRenewalIntent, seed.intent_id)
        if fault == 'terms':
            receipt.parameters = {'calendar_strategy': 'DAY'}
        elif fault == 'generation':
            receipt.account_generation = 'b' * 64
        elif fault == 'tariff':
            receipt.parameters = dict(receipt.parameters, tariff_id=999)
        else:
            (await db.get(Transaction, seed.transaction_id)).is_completed = False
        await db.commit()
        with pytest.raises(LedgerConflict):
            await quote_and_args(db, target, new, evidence)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
