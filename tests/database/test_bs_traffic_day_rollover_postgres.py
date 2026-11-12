from datetime import timedelta

import pytest
from sqlalchemy import delete, func, select

from app.database.models import BsTrafficUsage, BsTrafficUsageCursor
from app.services.bs_traffic.day_rollover import stage_day_rollover
from app.services.bs_traffic.ledger import LedgerConflict
from tests.database.test_bs_traffic_cutover_baseline_postgres import observe
from tests.database.test_bs_traffic_policy_postgres import NODE_B
from tests.database.test_bs_traffic_purchase_delivery_postgres import prepared_purchase
from tests.database.test_bs_traffic_purchase_postgres import TABLES, totals
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def rollover_fixture(db, monkeypatch, amount=60, write_off=False):
    args, reader, target, _ = await prepared_purchase(db, monkeypatch)
    from app.config import settings

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', write_off)
    now = await db.scalar(select(func.clock_timestamp()))
    day = now.date()
    await db.execute(delete(BsTrafficUsageCursor).where(BsTrafficUsageCursor.subscription_id == 1))
    db.add(
        BsTrafficUsageCursor(
            subscription_id=1,
            node_uuid=NODE_B,
            day=day - timedelta(days=1),
            account_generation=target['account_generation'],
            traffic_group='bs',
            baseline_bytes=100,
            processed_bytes=200,
            needs_review=False,
        )
    )
    await observe(db, reader, target, NODE_B, amount, day)
    await db.flush()
    return args, reader, target, day


async def test_first_daily_counter_preserved_without_charging_new_package(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args, reader, target, day = await rollover_fixture(db, monkeypatch)
        before = await totals(db)
        event_id = await stage_day_rollover(db, **target, node_uuid=NODE_B, day=day, evidence=args['evidence'])
        row = await db.get(BsTrafficUsage, event_id)
        assert (row.amount_bytes, row.evidence_kind, row.state) == (60, 'raw_pending', 'pending')
        assert row.eligible_grants == []
        cursor = await db.get(BsTrafficUsageCursor, (1, NODE_B, day))
        assert (cursor.baseline_bytes, cursor.processed_bytes) == (0, 60)
        yesterday = await db.get(BsTrafficUsageCursor, (1, NODE_B, day - timedelta(days=1)))
        assert yesterday.processed_bytes == 200
        assert await totals(db) == before
        assert await stage_day_rollover(db, **target, node_uuid=NODE_B, day=day, evidence=args['evidence']) is None
        reader._api.update_internal_squads_once.assert_not_awaited()


async def test_rollover_requires_explicit_previous_day(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, day = await rollover_fixture(db, monkeypatch)
        await db.execute(delete(BsTrafficUsageCursor))
        with pytest.raises(LedgerConflict):
            await stage_day_rollover(db, **target, node_uuid=NODE_B, day=day, evidence=args['evidence'])
        assert await db.get(BsTrafficUsageCursor, (1, NODE_B, day)) is None


async def test_rollover_and_raw_usage_rollback_together(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, day = await rollover_fixture(db, monkeypatch)
        await db.commit()
        await stage_day_rollover(db, **target, node_uuid=NODE_B, day=day, evidence=args['evidence'])
        await db.rollback()
        assert await db.get(BsTrafficUsageCursor, (1, NODE_B, day)) is None
        assert not (await db.scalars(select(BsTrafficUsage))).all()


@pytest.mark.parametrize(
    'change',
    [
        'generation',
        'review',
        'group',
        'missing_raw',
        'future',
        'stale_evidence',
        'foreign_owner',
        'unactivated',
        'bad_poll',
        'raw_review',
    ],
)
async def test_unsafe_rollover_leaves_no_new_cursor(postgres_database, monkeypatch, change):
    from app.database.models import BsTrafficObservation, BsTrafficPoll, Subscription

    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, day = await rollover_fixture(db, monkeypatch)
        previous = await db.get(BsTrafficUsageCursor, (1, NODE_B, day - timedelta(days=1)))
        if change == 'generation':
            previous.account_generation = 'b' * 64
        elif change == 'review':
            previous.needs_review = True
        elif change == 'group':
            previous.traffic_group = 'ordinary'
        elif change == 'stale_evidence':
            from dataclasses import replace

            args['evidence'] = replace(args['evidence'], valid_until=args['evidence'].observed_at)
        elif change == 'foreign_owner':
            target = target | {'user_id': 2}
        elif change == 'unactivated':
            (await db.get(Subscription, 1)).traffic_accounting_mode = 'legacy'
        elif change in ('bad_poll', 'raw_review'):
            raw = await db.scalar(select(BsTrafficObservation).where(BsTrafficObservation.node_uuid == NODE_B))
            if change == 'raw_review':
                raw.needs_review = True
            else:
                (await db.get(BsTrafficPoll, raw.latest_poll_id)).response_hash = None
        elif change == 'missing_raw':
            await db.execute(delete(BsTrafficObservation).where(BsTrafficObservation.node_uuid == NODE_B))
        else:
            day += timedelta(days=1)
        await db.flush()
        with pytest.raises(LedgerConflict):
            await stage_day_rollover(db, **target, node_uuid=NODE_B, day=day, evidence=args['evidence'])
        assert await db.get(BsTrafficUsageCursor, (1, NODE_B, day)) is None


async def test_writeoff_requires_explicit_existing_operator_setting(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, day = await rollover_fixture(db, monkeypatch, write_off=True)
        before = await totals(db)
        event_id = await stage_day_rollover(db, **target, node_uuid=NODE_B, day=day, evidence=args['evidence'])
        event = await db.get(BsTrafficUsage, event_id)
        assert event.state == 'written_off' and event.charged_bytes == 0 and event.unallocated_bytes == 60
        assert await totals(db) == before


async def test_repeated_enrollment_does_not_consume_later_delta(postgres_database, monkeypatch):
    from app.services.bs_traffic.intake import stage_observation

    async with postgres_session(postgres_database, TABLES) as db:
        args, reader, target, day = await rollover_fixture(db, monkeypatch)
        await stage_day_rollover(db, **target, node_uuid=NODE_B, day=day, evidence=args['evidence'])
        await observe(db, reader, target, NODE_B, 90, day)
        assert await stage_day_rollover(db, **target, node_uuid=NODE_B, day=day, evidence=args['evidence']) is None
        cursor = await db.get(BsTrafficUsageCursor, (1, NODE_B, day))
        assert cursor.processed_bytes == 60
        next_id = await stage_observation(db, **target, node_uuid=NODE_B, day=day)
        assert (await db.get(BsTrafficUsage, next_id)).amount_bytes == 30


async def test_explicit_zero_counter_creates_cursor_without_usage(postgres_database, monkeypatch):
    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, day = await rollover_fixture(db, monkeypatch, amount=0)
        assert await stage_day_rollover(db, **target, node_uuid=NODE_B, day=day, evidence=args['evidence']) is None
        cursor = await db.get(BsTrafficUsageCursor, (1, NODE_B, day))
        assert cursor.processed_bytes == 0
        assert not (await db.scalars(select(BsTrafficUsage))).all()


async def test_grace_bytes_are_retained_without_charging_paid_allowances(postgres_database, monkeypatch):
    from app.database.models import Subscription

    async with postgres_session(postgres_database, TABLES) as db:
        args, _, target, day = await rollover_fixture(db, monkeypatch)
        (await db.get(Subscription, 1)).grace_session_open = True
        await db.flush()
        before = await totals(db)
        event_id = await stage_day_rollover(db, **target, node_uuid=NODE_B, day=day, evidence=args['evidence'])
        assert (await db.get(BsTrafficUsage, event_id)).state == 'pending'
        assert await totals(db) == before
