"""Calendar and mode transitions share the old funded boundary atomically."""

from datetime import datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.config import settings
from app.database.models import (
    BsTrafficAllowance,
    BsTrafficLedger,
    BsTrafficRenewalIntent,
    Tariff,
    Transaction,
    User,
)
from app.services.bs_traffic.calendar_renewal import refresh_calendar_base
from app.services.bs_traffic.ledger import LedgerConflict, grant, open_usage, resolve_usage
from tests.database.test_bs_tariff_switch_calendar_postgres import calendar_setup
from tests.database.test_bs_tariff_switch_modes_postgres import mode_observation
from tests.database.test_bs_tariff_switch_postgres import core, quote_and_args
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def combined_setup(db, monkeypatch, groups):
    _, target, sub, new, evidence, seed = await calendar_setup(db, monkeypatch, 'DAY', 'WEEK')
    if 'ordinary' in groups:
        new.traffic_limit_gb = 0
    if 'bs' in groups:
        new.bs_traffic_settings = dict(new.bs_traffic_settings, limit_gb=0, topup_enabled=False)
    await db.commit()
    return target, sub, new, evidence, seed


@pytest.mark.parametrize('groups', [('ordinary',), ('bs',), ('ordinary', 'bs')])
@pytest.mark.parametrize('reset', [False, True])
async def test_combined_roundtrip_preserves_money_time_topups_and_pending(
    postgres_database, monkeypatch, groups, reset
):
    async with postgres_session(postgres_database, TABLES) as db:
        target, sub, new, evidence, seed = await combined_setup(db, monkeypatch, groups)
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', reset)
        old = await db.get(Tariff, sub.tariff_id)
        ledger = await db.get(BsTrafficLedger, sub.id)
        identity = (ledger.account_generation, ledger.ownership_cycle, sub.end_date)
        user = await db.get(User, target['user_id'])
        starting_balance = user.balance_kopeks
        pending, topups = {}, {}
        now = await db.scalar(select(func.clock_timestamp()))
        for group in groups:
            base = await db.get(BsTrafficAllowance, seed.grants[group])
            base.remaining_bytes -= 1024**3
            pending[group] = await open_usage(
                db,
                **target,
                group=group,
                source_key=f'combined:pending:{group}',
                amount_bytes=123,
                interval_start=base.starts_at,
                interval_end=base.starts_at + timedelta(microseconds=1),
            )
            topups[group] = await grant(
                db,
                **target,
                group=group,
                operation_key=f'combined:topup:{group}',
                amount_bytes=987,
                starts_at=now,
                expires_at=sub.end_date,
            )
        await db.commit()
        _, args = await quote_and_args(db, target, new, evidence)
        outgoing = await core().switch_tariff_from_balance(db, **args)
        assert not set(groups).intersection(outgoing.grants)
        first = await db.get(BsTrafficRenewalIntent, outgoing.intent_id)
        assert set(first.parameters['amounts']) == {'ordinary', 'bs'} - set(groups)
        assert first.parameters['switch_terms']['source_calendar']['receipt_id'] == seed.intent_id
        await db.commit()
        assert (await core().switch_tariff_from_balance(db, **args)).replayed
        for group in groups:
            now = await db.scalar(select(func.clock_timestamp()))
            await mode_observation(db, ledger, ledger.access_policy['topology'][f'{group}_nodes'][0], now, 5000)
        _, reverse_args = await quote_and_args(db, target, old, evidence)
        incoming = await core().switch_tariff_from_balance(db, **reverse_args)
        receipt = await db.get(BsTrafficRenewalIntent, incoming.intent_id)
        for group in groups:
            assert receipt.parameters['mode_boundary'][group]['nodes'][0]['baseline_bytes'] == 5000
            base = await db.get(BsTrafficAllowance, incoming.grants[group])
            expected = (10 if group == 'ordinary' else 5) * 1024**3 - (0 if reset else 1024**3)
            assert base.remaining_bytes == expected
            await resolve_usage(db, **target, usage_id=pending[group], decision='charge')
            assert base.remaining_bytes == expected
            topup = await db.get(BsTrafficAllowance, topups[group])
            assert topup.remaining_bytes == 987 and topup.retired_at is None and topup.expires_at == identity[2]
        assert (ledger.account_generation, ledger.ownership_cycle, sub.end_date) == identity
        await db.commit()
        assert (await core().switch_tariff_from_balance(db, **reverse_args)).replayed
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 3
        assert user.balance_kopeks == starting_balance - first.charged_kopeks - receipt.charged_kopeks
        due = datetime.fromisoformat(receipt.parameters['next_refresh_at'])
        refreshed = await refresh_calendar_base(db, **target, now=due)
        assert set(refreshed.grants) == {'ordinary', 'bs'}
        for group, amount in [('ordinary', 10), ('bs', 5)]:
            assert (await db.get(BsTrafficAllowance, refreshed.grants[group])).remaining_bytes == amount * 1024**3
        assert (await refresh_calendar_base(db, **target, now=due)).status == 'not_due'


@pytest.mark.parametrize('groups', [('ordinary',), ('bs',), ('ordinary', 'bs')])
async def test_combined_next_refresh_does_not_restore_unlimited_group(postgres_database, monkeypatch, groups):
    async with postgres_session(postgres_database, TABLES) as db:
        target, _, new, evidence, _ = await combined_setup(db, monkeypatch, groups)
        _, args = await quote_and_args(db, target, new, evidence)
        switched = await core().switch_tariff_from_balance(db, **args)
        receipt = await db.get(BsTrafficRenewalIntent, switched.intent_id)
        due = datetime.fromisoformat(receipt.parameters['next_refresh_at'])
        refreshed = await refresh_calendar_base(db, **target, now=due)
        assert set(refreshed.grants) == {'ordinary', 'bs'} - set(groups)


async def test_combined_missing_baseline_rolls_back_due_calendar(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        target, sub, new, evidence, _ = await combined_setup(db, monkeypatch, ('bs',))
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', False)
        old = await db.get(Tariff, sub.tariff_id)
        _, args = await quote_and_args(db, target, new, evidence)
        first = await core().switch_tariff_from_balance(db, **args)
        await db.commit()
        row = await db.get(BsTrafficRenewalIntent, first.intent_id)
        # Force an already due old calendar; baseline failure must undo its replacement too.
        from zoneinfo import ZoneInfo

        from app.services.bs_traffic.calendar_renewal import current_interval

        now = await db.scalar(select(func.clock_timestamp()))
        due, _ = current_interval(
            'WEEK',
            now=now,
            anchor=datetime.fromisoformat(row.parameters['calendar_anchor']),
            timezone=ZoneInfo(row.parameters['calendar_timezone']),
        )
        row.parameters = dict(row.parameters, next_refresh_at=due.isoformat())
        for base in await db.scalars(select(BsTrafficAllowance)):
            if base.retired_at is not None:
                base.retired_at = due - timedelta(hours=1)
            base.starts_at = due - timedelta(days=7)
            base.expires_at = due
        await db.commit()
        user = await db.get(User, target['user_id'])
        ledger = await db.get(BsTrafficLedger, sub.id)
        before = (user.balance_kopeks, sub.tariff_id, dict(ledger.access_policy), dict(row.parameters))
        count = await db.scalar(select(func.count()).select_from(BsTrafficAllowance))
        _, args = await quote_and_args(db, target, old, evidence)
        with pytest.raises(LedgerConflict, match='baseline missing'):
            await core().switch_tariff_from_balance(db, **args)
        for item in (user, sub, ledger, row):
            await db.refresh(item)
        assert (user.balance_kopeks, sub.tariff_id, ledger.access_policy, row.parameters) == before
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == count
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2


@pytest.mark.parametrize('fault', ['payment', 'generation', 'incomplete', 'old_mode'])
async def test_combined_rejects_invalid_source_before_payment(postgres_database, monkeypatch, fault):
    async with postgres_session(postgres_database, TABLES) as db:
        target, _, new, evidence, seed = await combined_setup(db, monkeypatch, ('ordinary', 'bs'))
        row = await db.get(BsTrafficRenewalIntent, seed.intent_id)
        if fault == 'payment':
            (await db.get(Transaction, seed.transaction_id)).is_completed = False
        elif fault == 'generation':
            row.account_generation = 'f' * 64
        elif fault == 'incomplete':
            row.parameters = dict(row.parameters, amounts={'ordinary': 10 * 1024**3})
        else:
            ledger = await db.get(BsTrafficLedger, target['subscription_id'])
            ledger.access_policy = dict(ledger.access_policy, bs_mode='unlimited')
        await db.commit()
        with pytest.raises(LedgerConflict):
            await quote_and_args(db, target, new, evidence)
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


@pytest.mark.parametrize('group', ['ordinary', 'bs'])
async def test_combined_due_calendar_uses_old_modes_before_replacement(postgres_database, monkeypatch, group):
    from zoneinfo import ZoneInfo

    from app.services.bs_traffic.calendar_renewal import current_interval

    async with postgres_session(postgres_database, TABLES) as db:
        target, _, new, evidence, seed = await combined_setup(db, monkeypatch, (group,))
        monkeypatch.setattr(settings, 'RESET_TRAFFIC_ON_TARIFF_SWITCH', False)
        row = await db.get(BsTrafficRenewalIntent, seed.intent_id)
        now = await db.scalar(select(func.clock_timestamp()))
        due, _ = current_interval(
            'DAY',
            now=now,
            anchor=datetime.fromisoformat(row.parameters['calendar_anchor']),
            timezone=ZoneInfo(row.parameters['calendar_timezone']),
        )
        row.parameters = dict(row.parameters, next_refresh_at=due.isoformat())
        for base in await db.scalars(select(BsTrafficAllowance)):
            if base.retired_at is not None:
                base.retired_at = due - timedelta(hours=1)
            base.starts_at = due - timedelta(days=1)
            base.expires_at = due
            base.remaining_bytes = 0
        await db.commit()
        _, args = await quote_and_args(db, target, new, evidence)
        switched = await core().switch_tariff_from_balance(db, **args)
        assert row.parameters['calendar_last_boundary'] == due.isoformat()
        other = 'bs' if group == 'ordinary' else 'ordinary'
        assert set(switched.grants) == {other}
        assert (await db.get(BsTrafficAllowance, switched.grants[other])).remaining_bytes == (
            10 if other == 'bs' else 20
        ) * 1024**3


async def test_combined_opposite_group_directions_share_calendar_boundary(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        target, sub, new, evidence, _ = await combined_setup(db, monkeypatch, ('bs',))
        old = await db.get(Tariff, sub.tariff_id)
        _, args = await quote_and_args(db, target, new, evidence)
        await core().switch_tariff_from_balance(db, **args)
        old.traffic_limit_gb = 0
        ledger = await db.get(BsTrafficLedger, sub.id)
        now = await db.scalar(select(func.clock_timestamp()))
        await mode_observation(db, ledger, ledger.access_policy['topology']['bs_nodes'][0], now, 5000)
        await db.commit()
        _, args = await quote_and_args(db, target, old, evidence)
        result = await core().switch_tariff_from_balance(db, **args)
        assert set(result.grants) == {'bs'}
        assert ledger.access_policy['ordinary_mode'] == 'unlimited'
        assert ledger.access_policy['bs_mode'] == 'finite'
        receipt = await db.get(BsTrafficRenewalIntent, result.intent_id)
        assert set(receipt.parameters['mode_boundary']) == {'bs'}
        assert receipt.parameters['calendar_strategy'] == 'DAY'
        due = datetime.fromisoformat(receipt.parameters['next_refresh_at'])
        refreshed = await refresh_calendar_base(db, **target, now=due)
        assert set(refreshed.grants) == {'bs'}
        assert (await db.get(BsTrafficAllowance, refreshed.grants['bs'])).remaining_bytes == 5 * 1024**3
