"""Reported usage charges once and never creates debt for later purchases."""

from datetime import timedelta

import pytest
from sqlalchemy import select

from app.database.models import BsTrafficAllowance, BsTrafficPoll, BsTrafficUsage
from app.services.bs_traffic.intake import attach_cursor, settle_reported_observation
from tests.database.test_bs_traffic_intake_postgres import KEY, TABLES, observe
from tests.database.test_bs_traffic_ledger_postgres import NOW, issue, remaining, seed
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def setup(db):
    await seed(db)
    await issue(db)
    await observe(db, 500)
    await attach_cursor(db, **KEY, group='bs')


async def sample(db, amount):
    await observe(db, amount)
    poll = await db.scalar(select(BsTrafficPoll).order_by(BsTrafficPoll.id.desc()).limit(1))
    poll.created_at = NOW + timedelta(hours=1)
    await db.flush()


async def test_reported_delta_is_charged_once(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 560)
        event_id = await settle_reported_observation(db, **KEY)
        assert await remaining(db) == 40
        row = await db.get(BsTrafficUsage, event_id)
        assert row.evidence_kind == 'reported' and row.state == 'charged'
        assert await settle_reported_observation(db, **KEY) is None
        assert await remaining(db) == 40


async def test_overflow_does_not_become_future_debt(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 650)
        event_id = await settle_reported_observation(db, **KEY)
        row = await db.get(BsTrafficUsage, event_id)
        assert row.charged_bytes == 100 and row.unallocated_bytes == 50
        await issue(db, operation_key='next', amount_bytes=100)
        assert await settle_reported_observation(db, **KEY) is None
        assert await remaining(db) == 100


async def test_poll_before_purchase_cannot_charge_new_grant(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 650)
        grant_id = await issue(db, operation_key='next', amount_bytes=100, starts_at=NOW + timedelta(hours=2))
        await settle_reported_observation(db, **KEY)
        assert (await db.get(BsTrafficAllowance, grant_id)).remaining_bytes == 100


async def test_regressed_counter_cannot_charge(postgres_database):
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 450)
        with pytest.raises(LedgerConflict):
            await settle_reported_observation(db, **KEY)
        assert await remaining(db) == 100


async def test_settlement_rollback_restores_cursor_and_grants(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 560)
        await db.commit()
        await settle_reported_observation(db, **KEY)
        await db.rollback()
        assert await remaining(db) == 100
        await settle_reported_observation(db, **KEY)
        assert await remaining(db) == 40


async def test_concurrent_consumers_charge_once(postgres_database):
    import asyncio

    from sqlalchemy.ext.asyncio import async_sessionmaker

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 560)
        await db.commit()
        maker = async_sessionmaker(db.bind)

        async def consume():
            async with maker() as session, session.begin():
                return await settle_reported_observation(session, **KEY)

        results = await asyncio.wait_for(asyncio.gather(consume(), consume()), 10)
        assert sum(value is not None for value in results) == 1
        assert await remaining(db) == 40


async def test_known_late_day_is_not_charged_to_current_period(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 560)
        poll = await db.scalar(select(BsTrafficPoll).order_by(BsTrafficPoll.id.desc()).limit(1))
        poll.created_at = NOW + timedelta(days=1)
        await db.flush()
        event_id = await settle_reported_observation(db, **KEY)
        assert (await db.get(BsTrafficUsage, event_id)).evidence_kind == 'raw_pending'
        assert await remaining(db) == 100


async def test_reported_migration_preserves_records_and_refuses_lossy_downgrade(postgres_database):
    import importlib

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module('migrations.alembic.versions.0151_bs_reported_usage')

    async with postgres_session(postgres_database, TABLES) as db:
        connection = await db.connection()

        def apply(conn, operation):
            with Operations.context(MigrationContext.configure(conn)):
                operation()

        await connection.run_sync(apply, migration.downgrade)
        await connection.run_sync(apply, migration.upgrade)
        await connection.run_sync(apply, migration.upgrade)
        await setup(db)
        await sample(db, 560)
        event_id = await settle_reported_observation(db, **KEY)
        with pytest.raises(RuntimeError, match='retention'):
            await connection.run_sync(apply, migration.downgrade)
        assert (await db.get(BsTrafficUsage, event_id)).charged_bytes == 60


@pytest.mark.parametrize('write_off', [False, True])
async def test_historical_delta_is_preserved_without_spending_new_package(postgres_database, monkeypatch, write_off):
    from app.config import settings

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', write_off)
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        fresh_id = await issue(db, operation_key='new-period', starts_at=NOW + timedelta(days=1))
        await sample(db, 650)
        poll = await db.scalar(select(BsTrafficPoll).order_by(BsTrafficPoll.id.desc()).limit(1))
        poll.created_at = NOW + timedelta(days=1, hours=1)
        await db.flush()
        event_id = await settle_reported_observation(db, **KEY)
        event = await db.get(BsTrafficUsage, event_id)
        assert event.evidence_kind == 'raw_pending'
        assert event.amount_bytes == 150 and event.charged_bytes == 0
        assert event.state == ('written_off' if write_off else 'pending')
        assert event.eligible_grants == []
        assert (await db.get(BsTrafficAllowance, fresh_id)).remaining_bytes == 100
        assert await settle_reported_observation(db, **KEY) is None
        assert len((await db.scalars(select(BsTrafficUsage))).all()) == 1


async def test_historical_intake_rollback_preserves_cursor(postgres_database, monkeypatch):
    from app.config import settings
    from app.database.models import BsTrafficUsageCursor
    from tests.database.test_bs_traffic_intake_postgres import NODE

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 650)
        poll = await db.scalar(select(BsTrafficPoll).order_by(BsTrafficPoll.id.desc()).limit(1))
        poll.created_at = NOW + timedelta(days=2)
        await db.commit()
        await settle_reported_observation(db, **KEY)
        await db.rollback()
        assert not (await db.scalars(select(BsTrafficUsage))).all()
        assert (await db.get(BsTrafficUsageCursor, (1, NODE, NOW.date()))).processed_bytes == 500
        event_id = await settle_reported_observation(db, **KEY)
        assert (await db.get(BsTrafficUsage, event_id)).unallocated_bytes == 150
        assert await remaining(db) == 100


async def test_later_historical_increment_does_not_recount_previous_bytes(postgres_database, monkeypatch):
    from app.config import settings

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        for count in (650, 700):
            await sample(db, count)
            poll = await db.scalar(select(BsTrafficPoll).order_by(BsTrafficPoll.id.desc()).limit(1))
            poll.created_at = NOW + timedelta(days=3)
            await db.flush()
            await settle_reported_observation(db, **KEY)
        events = (await db.scalars(select(BsTrafficUsage).order_by(BsTrafficUsage.id))).all()
        assert [event.amount_bytes for event in events] == [150, 50]
        assert all(event.state == 'written_off' and event.charged_bytes == 0 for event in events)
        assert await remaining(db) == 100


async def test_future_day_is_not_treated_as_historical_loss(postgres_database, monkeypatch):
    from app.config import settings
    from app.services.bs_traffic.ledger import LedgerConflict

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 650)
        poll = await db.scalar(select(BsTrafficPoll).order_by(BsTrafficPoll.id.desc()).limit(1))
        poll.created_at = NOW - timedelta(days=1)
        await db.flush()
        with pytest.raises(LedgerConflict, match='Future-day'):
            await settle_reported_observation(db, **KEY)
        assert not (await db.scalars(select(BsTrafficUsage))).all()
        assert await remaining(db) == 100
