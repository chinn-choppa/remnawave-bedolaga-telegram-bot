"""Real PostgreSQL ordering/locking checks for raw evidence, not billing."""

import asyncio
import importlib

import pytest
from alembic.migration import MigrationContext
from alembic.operations import Operations
from sqlalchemy import select
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionJob, BsTrafficFleetNode, BsTrafficObservation, BsTrafficPoll
from app.services.bs_traffic.observations import record_poll
from tests.database.test_bs_traffic_observation_store_postgres import TABLES, poll, response, state
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres


async def test_overlapping_polls_and_replays_have_one_consistent_high_water(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        older, newer = await poll(db), await poll(db)
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        barrier = asyncio.Barrier(4)

        async def save(poll_id, value):
            async with factory() as worker:
                await barrier.wait()
                await record_poll(worker, poll_id, response(value))
                await worker.commit()

        await asyncio.wait_for(
            asyncio.gather(save(older.id, 100), save(older.id, 100), save(newer.id, 200), save(newer.id, 200)),
            timeout=10,
        )
        saved = await state(db)
        assert (saved.high_water_bytes, saved.latest_bytes, saved.latest_poll_id) == (200, 200, newer.id)
        assert not saved.needs_review
        assert len((await db.scalars(select(BsTrafficObservation))).all()) == 1


async def test_competing_responses_for_same_poll_cannot_both_commit(postgres_database):
    async with postgres_session(postgres_database, TABLES) as db:
        pending = await poll(db)
        await db.commit()
        factory = async_sessionmaker(db.bind, expire_on_commit=False)
        barrier = asyncio.Barrier(2)

        async def save(value):
            async with factory() as worker:
                await barrier.wait()
                try:
                    await record_poll(worker, pending.id, response(value))
                    await worker.commit()
                    return True
                except ValueError:
                    await worker.rollback()
                    return False

        results = await asyncio.wait_for(asyncio.gather(save(100), save(200)), timeout=10)
        assert sorted(results) == [False, True]
        assert (await state(db)).high_water_bytes in (100, 200)


async def test_additive_migration_creates_usable_storage_and_downgrade_keeps_evidence(postgres_database):
    migration = importlib.import_module('migrations.alembic.versions.0142_bs_traffic_observations')
    omitted = importlib.import_module('migrations.alembic.versions.0165_bs_omitted_baseline')
    async with postgres_session(postgres_database, TABLES) as db:
        connection = await db.connection()

        def migrate(sync):
            with Operations.context(MigrationContext.configure(sync)):
                omitted.downgrade()
            # The test starts from current metadata; remove the later dependent
            # table before recreating the original observation-era schema.
            BsTrafficFleetNode.__table__.drop(sync)
            BsTrafficCollectionJob.__table__.drop(sync)
            BsTrafficObservation.__table__.drop(sync)
            BsTrafficPoll.__table__.drop(sync)
            with Operations.context(MigrationContext.configure(sync)):
                migration.upgrade()
                omitted.upgrade()
            BsTrafficCollectionJob.__table__.create(sync)
            BsTrafficFleetNode.__table__.create(sync)

        await connection.run_sync(migrate)
        pending = await poll(db)
        await record_poll(db, pending.id, response(100))
        await db.commit()
        connection = await db.connection()

        def downgrade(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.downgrade()
                migration.upgrade()

        await connection.run_sync(downgrade)
        assert (await state(db)).high_water_bytes == 100


async def test_multirow_upsert_retains_each_accounts_counter(postgres_database):
    from app.services.bs_traffic.observations import begin_poll
    from tests.database.test_bs_traffic_observation_store_postgres import DAY, NODE, SCOPE

    async with postgres_session(postgres_database, TABLES) as db:
        for increase in (0, 100):
            pending = await begin_poll(
                db,
                panel_scope=SCOPE,
                day=DAY,
                node_uuids=(NODE,),
                identities={value: f'generation-{value}' for value in range(1, 502)},
            )
            await db.commit()
            await record_poll(
                db,
                pending.id,
                {
                    'nodes': [
                        {
                            'uuid': NODE,
                            'users': [{'id': value, 'totalBytes': value + increase} for value in range(1, 502)],
                        }
                    ]
                },
            )
            await db.commit()
        rows = (await db.scalars(select(BsTrafficObservation))).all()
        assert len(rows) == 501
        assert all(row.high_water_bytes == row.panel_user_id + 100 and not row.needs_review for row in rows)
