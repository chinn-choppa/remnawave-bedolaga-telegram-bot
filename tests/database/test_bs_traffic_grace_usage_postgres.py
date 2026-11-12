from datetime import timedelta

import pytest

from app.config import settings
from app.database.models import BsTrafficUsage, GraceAccessSessionModel, Subscription
from app.services.bs_traffic.intake import settle_reported_observation
from tests.database.test_bs_traffic_intake_postgres import KEY, TABLES
from tests.database.test_bs_traffic_ledger_postgres import NOW, remaining
from tests.database.test_bs_traffic_reported_postgres import sample, setup
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


def grace(*, start, end=None, subscription_id=1):
    return GraceAccessSessionModel(
        id='22222222-2222-4222-8222-222222222222',
        subscription_id=subscription_id,
        remnawave_id=12,
        reason='expired',
        incident_key='synthetic',
        state='completed' if end else 'active',
        billing_before={},
        panel_before={},
        overlay={},
        started_at=start,
        grace_until=start + timedelta(hours=1),
        updated_at=end or start,
        completed_at=end,
        completion_reason='paid' if end else None,
    )


@pytest.mark.parametrize('kind', ['active', 'completed', 'marker'])
@pytest.mark.parametrize('write_off', [False, True])
async def test_grace_day_usage_never_charges_paid_package(postgres_database, monkeypatch, kind, write_off):
    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', write_off)
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        if kind == 'marker':
            (await db.get(Subscription, 1)).grace_session_open = True
        else:
            db.add(
                grace(
                    start=NOW + timedelta(minutes=5), end=NOW + timedelta(minutes=10) if kind == 'completed' else None
                )
            )
        await sample(db, 560)
        event_id = await settle_reported_observation(db, **KEY)
        row = await db.get(BsTrafficUsage, event_id)
        assert row.evidence_kind == 'raw_pending'
        assert row.state == ('written_off' if write_off else 'pending')
        assert row.charged_bytes == 0 and row.eligible_grants == []
        assert await remaining(db) == 100
        assert await settle_reported_observation(db, **KEY) is None


@pytest.mark.parametrize('unrelated', ['previous_day', 'other_subscription'])
async def test_unrelated_completed_grace_does_not_block_reported_usage(postgres_database, unrelated):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        if unrelated == 'previous_day':
            db.add(grace(start=NOW - timedelta(hours=2), end=NOW))
        else:
            db.add(grace(start=NOW, end=NOW + timedelta(minutes=10), subscription_id=2))
        await sample(db, 560)
        event_id = await settle_reported_observation(db, **KEY)
        assert (await db.get(BsTrafficUsage, event_id)).state == 'charged'
        assert await remaining(db) == 40


async def test_late_delta_after_restore_does_not_spend_new_grant(postgres_database, monkeypatch):
    from sqlalchemy import select

    from app.database.models import BsTrafficAllowance, BsTrafficPoll
    from app.services.bs_traffic.intake import grant_with_reported_settlement
    from app.services.bs_traffic.ledger import summary

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', False)
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        session = grace(start=NOW)
        db.add(session)
        await sample(db, 560)
        await settle_reported_observation(db, **KEY)
        session.state = 'completed'
        session.completed_at = NOW + timedelta(hours=1, minutes=5)
        session.completion_reason = 'paid'
        await sample(db, 580)
        grant_id = await grant_with_reported_settlement(
            db,
            user_id=KEY['user_id'],
            subscription_id=KEY['subscription_id'],
            account_generation=KEY['account_generation'],
            group='bs',
            operation_key='after-grace',
            amount_bytes=100,
            starts_at=NOW + timedelta(hours=1, minutes=5),
            expires_at=NOW + timedelta(days=30),
        )
        await sample(db, 610)
        poll = await db.scalar(select(BsTrafficPoll).order_by(BsTrafficPoll.id.desc()).limit(1))
        poll.created_at = NOW + timedelta(hours=2)
        await db.flush()
        event_id = await settle_reported_observation(db, **KEY)
        event = await db.get(BsTrafficUsage, event_id)
        assert event.amount_bytes == 30 and event.state == 'pending' and event.eligible_grants == []
        assert (await db.get(BsTrafficAllowance, grant_id)).remaining_bytes == 100
        balances = await summary(
            db,
            **{key: KEY[key] for key in ('user_id', 'subscription_id', 'account_generation')},
            now=NOW + timedelta(hours=2),
        )
        assert balances['bs']['remaining_bytes'] == 200
