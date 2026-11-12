"""A known purchase between snapshots does not turn old bytes into new debt."""

from datetime import timedelta

import pytest
from sqlalchemy import select

from app.config import settings
from app.database.models import BsTrafficAllowance, BsTrafficUsage
from app.services.bs_traffic.intake import settle_reported_observation
from tests.database.test_bs_traffic_intake_postgres import KEY, TABLES
from tests.database.test_bs_traffic_ledger_postgres import NOW, issue
from tests.database.test_bs_traffic_reported_postgres import sample, setup
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


@pytest.mark.parametrize('write_off', [False, True])
async def test_purchase_between_samples_does_not_fund_uncertain_delta(postgres_database, monkeypatch, write_off):
    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', write_off)
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        fresh = await issue(db, operation_key='fresh', starts_at=NOW + timedelta(minutes=30))
        await sample(db, 650)
        usage_id = await settle_reported_observation(db, **KEY)
        usage = await db.get(BsTrafficUsage, usage_id)
        assert usage.evidence_kind == 'raw_pending'
        assert usage.charged_bytes == 0 and usage.amount_bytes == 150
        assert usage.state == ('written_off' if write_off else 'pending')
        assert (await db.get(BsTrafficAllowance, fresh)).remaining_bytes == 100
        assert await settle_reported_observation(db, **KEY) is None
        assert len((await db.scalars(select(BsTrafficUsage))).all()) == 1


async def test_other_group_purchase_does_not_contaminate_bs(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await issue(db, group='ordinary', operation_key='ordinary', starts_at=NOW + timedelta(minutes=30))
        await sample(db, 560)
        usage_id = await settle_reported_observation(db, **KEY)
        assert (await db.get(BsTrafficUsage, usage_id)).charged_bytes == 60


async def test_zero_sample_advances_boundary_without_creating_usage(postgres_database):
    from app.database.models import BsTrafficUsageCursor

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 500)
        assert await settle_reported_observation(db, **KEY) is None
        cursor = await db.get(BsTrafficUsageCursor, (1, KEY['node_uuid'], NOW.date()))
        assert cursor.last_poll_started_at == NOW + timedelta(hours=1)
        assert not (await db.scalars(select(BsTrafficUsage))).all()


async def test_unknown_migrated_boundary_cannot_charge(postgres_database):
    from app.database.models import BsTrafficUsageCursor

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        cursor = await db.get(BsTrafficUsageCursor, (1, KEY['node_uuid'], NOW.date()))
        cursor.last_poll_started_at = None
        await db.flush()
        await sample(db, 560)
        usage_id = await settle_reported_observation(db, **KEY)
        assert (await db.get(BsTrafficUsage, usage_id)).evidence_kind == 'raw_pending'
        assert cursor.last_poll_started_at == NOW + timedelta(hours=1)


async def test_regressed_source_poll_cannot_move_boundary(postgres_database):
    from app.database.models import BsTrafficPoll, BsTrafficUsageCursor
    from app.services.bs_traffic.ledger import LedgerConflict

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await sample(db, 500)
        await settle_reported_observation(db, **KEY)
        await sample(db, 560)
        poll = await db.scalar(select(BsTrafficPoll).order_by(BsTrafficPoll.id.desc()).limit(1))
        poll.created_at = NOW + timedelta(minutes=30)
        await db.flush()
        with pytest.raises(LedgerConflict, match='Source poll moved'):
            await settle_reported_observation(db, **KEY)
        cursor = await db.get(BsTrafficUsageCursor, (1, KEY['node_uuid'], NOW.date()))
        assert cursor.processed_bytes == 500
        assert cursor.last_poll_started_at == NOW + timedelta(hours=1)


async def test_boundary_and_uncertain_delta_rollback_together(postgres_database):
    from app.database.models import BsTrafficUsageCursor

    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        fresh = await issue(db, operation_key='fresh', starts_at=NOW + timedelta(minutes=30))
        await sample(db, 650)
        await db.commit()
        await settle_reported_observation(db, **KEY)
        await db.rollback()
        cursor = await db.get(BsTrafficUsageCursor, (1, KEY['node_uuid'], NOW.date()))
        assert cursor.processed_bytes == 500 and cursor.last_poll_started_at == NOW
        assert not (await db.scalars(select(BsTrafficUsage))).all()
        assert (await db.get(BsTrafficAllowance, fresh)).remaining_bytes == 100
        event_id = await settle_reported_observation(db, **KEY)
        assert (await db.get(BsTrafficUsage, event_id)).amount_bytes == 150


async def test_migration_keeps_legacy_cursor_unknown_and_refuses_loss(postgres_database):
    import importlib

    from alembic.migration import MigrationContext
    from alembic.operations import Operations
    from sqlalchemy import text

    migration = importlib.import_module('migrations.alembic.versions.0152_bs_cursor_poll_boundary')
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        await db.execute(text('UPDATE bs_traffic_usage_cursors SET last_poll_started_at = NULL'))
        connection = await db.connection()

        def apply(conn, operation):
            with Operations.context(MigrationContext.configure(conn)):
                operation()

        await connection.run_sync(apply, migration.downgrade)
        await connection.run_sync(apply, migration.upgrade)
        await connection.run_sync(apply, migration.upgrade)
        assert await db.scalar(text('SELECT last_poll_started_at FROM bs_traffic_usage_cursors')) is None
        assert await db.scalar(text('SELECT processed_bytes FROM bs_traffic_usage_cursors')) == 500
        await db.execute(text('UPDATE bs_traffic_usage_cursors SET last_poll_started_at = CURRENT_TIMESTAMP'))
        with pytest.raises(RuntimeError, match='retention'):
            await connection.run_sync(apply, migration.downgrade)


async def test_next_delta_uses_reported_policy_after_explicit_boundary_writeoff(postgres_database, monkeypatch):
    """Only the known transition is fenced; this is not proof of a provider flush."""
    from app.database.models import BsTrafficPoll

    monkeypatch.setattr(settings, 'BS_TRAFFIC_UNCERTAIN_WRITE_OFF_ENABLED', True)
    async with postgres_session(postgres_database, TABLES) as db:
        await setup(db)
        fresh = await issue(db, operation_key='fresh', starts_at=NOW + timedelta(minutes=30))
        await sample(db, 650)
        first = await settle_reported_observation(db, **KEY)
        assert (await db.get(BsTrafficUsage, first)).state == 'written_off'
        await sample(db, 660)
        poll = await db.scalar(select(BsTrafficPoll).order_by(BsTrafficPoll.id.desc()).limit(1))
        poll.created_at = NOW + timedelta(hours=2)
        await db.flush()
        second = await settle_reported_observation(db, **KEY)
        event = await db.get(BsTrafficUsage, second)
        assert event.evidence_kind == 'reported' and event.charged_bytes == 10
        assert (await db.get(BsTrafficAllowance, fresh)).remaining_bytes == 100
        assert await settle_reported_observation(db, **KEY) is None
