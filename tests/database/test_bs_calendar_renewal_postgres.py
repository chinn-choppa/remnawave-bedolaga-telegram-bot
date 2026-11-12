from datetime import datetime, timedelta

import pytest
from sqlalchemy import func, select

from app.database.models import BsTrafficAllowance, BsTrafficRenewalIntent, Subscription, Tariff, Transaction, User
from app.services.bs_traffic.calendar_renewal import refresh_calendar_base
from app.services.bs_traffic.ledger import LedgerConflict, open_usage, resolve_usage
from app.services.bs_traffic.renewal import renew_from_balance
from tests.database.test_bs_paid_renewal_postgres import prepared
from tests.database.test_bs_traffic_purchase_postgres import TABLES
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def setup(db, monkeypatch):
    reader, target, sub = await prepared(db, monkeypatch)
    (await db.get(Tariff, sub.tariff_id)).traffic_reset_mode = 'DAY'
    await db.commit()
    paid = await renew_from_balance(
        db, **target, period_days=30, expected_price_kopeks=2000, operation_key='calendar-paid'
    )
    await db.commit()
    receipt = await db.get(BsTrafficRenewalIntent, paid.intent_id)
    due = datetime.fromisoformat(receipt.parameters['next_refresh_at'])
    return target, paid, due, reader


async def test_calendar_catchup_grants_only_current_interval_once_without_money(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        target, paid, due, _ = await setup(db, monkeypatch)
        before = (await db.get(User, target['user_id'])).balance_kopeks
        now = due + timedelta(days=3, hours=2)
        result = await refresh_calendar_base(db, **target, now=now)
        assert result.status == 'refreshed'
        assert result.starts_at == due + timedelta(days=3)
        assert len(result.grants) == 2
        await db.commit()
        again = await refresh_calendar_base(db, **target, now=now)
        assert again.status == 'not_due'
        assert (await db.get(User, target['user_id'])).balance_kopeks == before
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1
        # Known missing-period usage and a mixed old/new interval cannot debit
        # the newly funded current calendar bucket.
        fresh = await db.get(BsTrafficAllowance, result.grants['bs'])
        remaining = fresh.remaining_bytes
        for key, start, end in [
            ('missed', due + timedelta(hours=1), due + timedelta(hours=2)),
            ('mixed', result.starts_at - timedelta(seconds=1), result.starts_at + timedelta(seconds=1)),
        ]:
            usage = await open_usage(
                db, **target, group='bs', source_key=key, amount_bytes=70, interval_start=start, interval_end=end
            )
            await resolve_usage(db, **target, usage_id=usage, decision='charge')
        assert fresh.remaining_bytes == remaining


async def test_calendar_wrong_owner_and_changed_tariff_roll_back(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        target, _, due, _ = await setup(db, monkeypatch)
        with pytest.raises(LedgerConflict):
            await refresh_calendar_base(db, **(target | {'user_id': 2}), now=due)
        replacement = Tariff(name='Different subscription tariff')
        db.add(replacement)
        await db.flush()
        sub = await db.get(Subscription, target['subscription_id'])
        sub.tariff_id = replacement.id
        await db.commit()
        before = await db.scalar(select(func.count()).select_from(BsTrafficAllowance))
        with pytest.raises(LedgerConflict):
            await refresh_calendar_base(db, **target, now=due)
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == before


@pytest.mark.parametrize('write_off', [False, True])
async def test_observed_delta_across_calendar_boundary_stays_raw_not_new_debt(
    postgres_database, monkeypatch, write_off
):
    from app.config import settings
    from app.database.models import BsTrafficUsage
    from app.services.bs_traffic.intake import attach_cursor, settle_reported_observation
    from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
    from tests.database.test_bs_traffic_policy_postgres import NODE_B

    async with postgres_session(postgres_database, TABLES) as db:
        target, _, due, reader = await setup(db, monkeypatch)
        monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', write_off)
        boundary = due + timedelta(days=3)
        poll, day = await observe(db, reader, target, NODE_B, 100, day=boundary.date())
        poll.created_at = boundary - timedelta(seconds=1)
        await db.flush()
        await attach_cursor(db, **target, group='bs', node_uuid=NODE_B, day=day)
        poll, _ = await observe(db, reader, target, NODE_B, 170, day=day)
        poll.created_at = boundary + timedelta(seconds=1)
        await db.flush()
        result = await refresh_calendar_base(db, **target, now=boundary + timedelta(hours=2))
        fresh = await db.get(BsTrafficAllowance, result.grants['bs'])
        before = fresh.remaining_bytes
        event = await settle_reported_observation(db, **target, node_uuid=NODE_B, day=day)
        usage = await db.get(BsTrafficUsage, event)
        assert usage.evidence_kind == 'raw_pending'
        assert usage.state == ('written_off' if write_off else 'pending')
        assert usage.charged_bytes == 0 and usage.eligible_grants == []
        assert fresh.remaining_bytes == before
        with pytest.raises(LedgerConflict):
            await resolve_usage(db, **target, usage_id=event, decision='charge')


async def test_calendar_disable_bs_preserves_topup_and_stops_at_paid_end(postgres_database, monkeypatch):
    from app.database.crud.tariff import update_tariff
    from app.database.models import Subscription
    from app.services.bs_traffic.ledger import grant

    async with postgres_session(postgres_database, TABLES) as db:
        target, _, due, _ = await setup(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        topup = await grant(
            db,
            **target,
            group='bs',
            operation_key='purchase:independent',
            amount_bytes=500,
            starts_at=due - timedelta(hours=1),
            expires_at=sub.end_date + timedelta(days=20),
        )
        original_end = (await db.get(BsTrafficAllowance, topup)).expires_at
        await update_tariff(db, await db.get(Tariff, sub.tariff_id), bs_traffic_settings=None)
        result = await refresh_calendar_base(db, **target, now=due)
        assert set(result.grants) == {'ordinary', 'bs'}
        assert (await db.get(BsTrafficAllowance, topup)).remaining_bytes == 500
        assert (await db.get(BsTrafficAllowance, topup)).expires_at == original_end
        assert (await refresh_calendar_base(db, **target, now=sub.end_date)).status == 'expired'


async def test_concurrent_calendar_workers_grant_one_interval(postgres_database, monkeypatch):
    import asyncio

    async with postgres_session(postgres_database, TABLES) as db:
        target, _, due, reader = await setup(db, monkeypatch)
        await db.rollback()

        async def refresh():
            async with reader._factory() as other, other.begin():
                return await refresh_calendar_base(other, **target, now=due)

        results = await asyncio.gather(refresh(), refresh())
        assert sorted(result.status for result in results) == ['not_due', 'refreshed']
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 1


async def test_ordinary_renewal_does_not_cancel_or_extend_paid_bs_calendar(postgres_database, monkeypatch):
    from app.database.crud.tariff import update_tariff
    from app.database.models import Subscription

    async with postgres_session(postgres_database, TABLES) as db:
        target, first, due, _ = await setup(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        bs_paid_end = sub.end_date
        await update_tariff(db, await db.get(Tariff, sub.tariff_id), bs_traffic_settings=None)
        second = await renew_from_balance(
            db, **target, period_days=30, expected_price_kopeks=2000, operation_key='ordinary-after-disable'
        )
        await db.commit()
        assert set(second.grants) == {'ordinary'}
        result = await refresh_calendar_base(db, **target, now=due)
        assert set(result.grants) == {'ordinary', 'bs'}
        bs_grant = await db.get(BsTrafficAllowance, result.grants['bs'])
        assert bs_grant.expires_at <= bs_paid_end
        await db.commit()
        assert (await refresh_calendar_base(db, **target, now=due)).status == 'not_due'
        at_end = await refresh_calendar_base(db, **target, now=bs_paid_end + timedelta(seconds=1))
        assert set(at_end.grants) == {'ordinary'}
        assert await db.scalar(select(func.count()).select_from(Transaction)) == 2


async def test_new_no_reset_ordinary_terms_keep_earlier_bs_calendar_due(postgres_database, monkeypatch):
    from types import SimpleNamespace

    from app.database.crud.tariff import update_tariff
    from app.database.models import Subscription
    from app.services.bs_traffic.fleet_settlement import _calendar

    async with postgres_session(postgres_database, TABLES) as db:
        target, _, due, _ = await setup(db, monkeypatch)
        sub = await db.get(Subscription, target['subscription_id'])
        await update_tariff(db, await db.get(Tariff, sub.tariff_id), bs_traffic_settings=None)
        (await db.get(Tariff, sub.tariff_id)).traffic_reset_mode = 'NO_RESET'
        await db.commit()
        second = await renew_from_balance(
            db, **target, period_days=30, expected_price_kopeks=2000, operation_key='new-no-reset-ordinary'
        )
        await db.commit()
        work = SimpleNamespace(calendar_receipt_id=None, calendar_due_at=None)
        await _calendar(db, work, target, due, sub)
        assert work.calendar_receipt_id == second.intent_id
        assert work.calendar_due_at == due + timedelta(days=1)
        first_count = await db.scalar(select(func.count()).select_from(BsTrafficAllowance))
        await _calendar(db, work, target, due + timedelta(seconds=1), sub)
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == first_count
        await _calendar(db, work, target, due + timedelta(days=1), sub)
        assert await db.scalar(select(func.count()).select_from(BsTrafficAllowance)) == first_count + 1


async def test_funded_calendar_survives_mutable_tariff_modes_and_squads(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        target, _, due, _ = await setup(db, monkeypatch)
        tariff = await db.scalar(select(Tariff))
        tariff.allowed_squads = []
        tariff.traffic_limit_gb = 0
        tariff.bs_traffic_settings = dict(tariff.bs_traffic_settings, limit_gb=0)
        await db.commit()
        result = await refresh_calendar_base(db, **target, now=due)
        assert set(result.grants) == {'ordinary', 'bs'}
        assert (await db.get(BsTrafficAllowance, result.grants['ordinary'])).granted_bytes == 10 * 1024**3
        assert (await db.get(BsTrafficAllowance, result.grants['bs'])).granted_bytes == 5 * 1024**3
