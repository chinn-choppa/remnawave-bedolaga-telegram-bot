"""One raw node/day delta enters pending accounting once, never as paid usage."""

from datetime import timedelta

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficObservation, BsTrafficPoll, BsTrafficUsage, User
from app.services.bs_traffic.intake import attach_cursor, stage_observation
from app.services.bs_traffic.ledger import LedgerConflict, summary
from app.services.bs_traffic.observations import begin_poll, record_poll
from tests.database.test_bs_traffic_ledger_postgres import (
    GENERATION,
    NOW,
    SCOPE,
    TARGET,
    issue,
    remaining,
    resolve,
    seed,
)
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
NODE = '22222222-2222-4222-8222-222222222222'
KEY = TARGET | {'node_uuid': NODE, 'day': NOW.date()}
TABLES = [User.__table__, BsTrafficPoll.__table__, BsTrafficObservation.__table__]


async def observe(db, amount):
    poll = await begin_poll(db, panel_scope=SCOPE, day=NOW.date(), node_uuids=(NODE,), identities={12: GENERATION})
    poll.created_at = NOW
    await db.flush()
    await record_poll(db, poll.id, {'nodes': [{'uuid': NODE, 'users': [{'id': 12, 'totalBytes': amount}]}]})


async def test_existing_history_is_baseline_and_new_delta_stays_pending(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await seed(db)
        await issue(db)
        await observe(db, 500)
        await attach_cursor(db, **KEY, group='bs')
        assert await stage_observation(db, **KEY) is None
        await observe(db, 560)
        event_id = await stage_observation(db, **KEY)
        assert await stage_observation(db, **KEY) is None
        event = await db.get(BsTrafficUsage, event_id)
        assert event.amount_bytes == 60 and event.evidence_kind == 'raw_pending'
        assert event.state == 'pending' and await remaining(db) == 100
        with pytest.raises(LedgerConflict):
            await resolve(db, event_id)
        await resolve(db, event_id, 'write_off', allow_write_off=True)
        assert await remaining(db) == 100


async def test_replayed_attach_does_not_skip_unprocessed_delta(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await seed(db)
        await observe(db, 100)
        await attach_cursor(db, **KEY, group='bs')
        await observe(db, 200)
        await attach_cursor(db, **KEY, group='bs')
        event_id = await stage_observation(db, **KEY)
        assert (await db.get(BsTrafficUsage, event_id)).amount_bytes == 100


async def test_missing_observation_cannot_be_invented_as_zero_baseline(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await seed(db)
        with pytest.raises(LedgerConflict):
            await attach_cursor(db, **KEY, group='bs')


async def test_group_change_cannot_relabel_existing_counter(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await seed(db)
        await observe(db, 100)
        await attach_cursor(db, **KEY, group='bs')
        with pytest.raises(LedgerConflict):
            await attach_cursor(db, **KEY, group='ordinary')


async def test_cursor_and_pending_usage_rollback_together(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await seed(db)
        await observe(db, 100)
        await attach_cursor(db, **KEY, group='bs')
        await observe(db, 200)
        await db.commit()
        await stage_observation(db, **KEY)
        await db.rollback()
        assert not (await db.scalars(select(BsTrafficUsage))).all()
        event_id = await stage_observation(db, **KEY)
        assert (await db.get(BsTrafficUsage, event_id)).amount_bytes == 100


async def test_next_utc_day_does_not_reuse_previous_day_baseline(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await seed(db)
        await observe(db, 100)
        await attach_cursor(db, **KEY, group='bs')
        with pytest.raises(LedgerConflict):
            await stage_observation(db, **(KEY | {'day': NOW.date() + timedelta(days=1)}))


async def test_counter_regression_is_visible_even_without_a_positive_pending_delta(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await seed(db)
        await observe(db, 100)
        await attach_cursor(db, **KEY, group='bs')
        await observe(db, 90)
        assert await stage_observation(db, **KEY) is None
        status = (await summary(db, **TARGET, now=NOW))['bs']
        assert status['needs_review'] is True and status['pending_bytes'] == 0


async def test_parallel_intake_creates_one_event_for_one_delta(postgres_database):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(postgres_database, TABLES) as db:
        await seed(db)
        await observe(db, 100)
        await attach_cursor(db, **KEY, group='bs')
        await observe(db, 200)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        barrier = asyncio.Barrier(2)

        async def worker():
            async with maker() as conn, conn.begin():
                await barrier.wait()
                return await stage_observation(conn, **KEY)

        result = await asyncio.wait_for(asyncio.gather(worker(), worker()), 10)
        assert sum(item is not None for item in result) == 1
        events = (await db.scalars(select(BsTrafficUsage))).all()
        assert len(events) == 1 and events[0].amount_bytes == 100


async def test_counter_regression_invalidates_access_revision_exactly_once(postgres_database):
    from app.database.models import BsTrafficLedger

    async with postgres_session(postgres_database, TABLES) as db:
        await seed(db)
        await observe(db, 100)
        await attach_cursor(db, **KEY, group='bs')
        ledger = await db.get(BsTrafficLedger, 1)
        before = ledger.revision
        await observe(db, 90)
        assert await stage_observation(db, **KEY) is None
        assert ledger.revision == before + 1
        assert await stage_observation(db, **KEY) is None
        assert ledger.revision == before + 1
