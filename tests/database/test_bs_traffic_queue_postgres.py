"""Durable collection jobs: claims, recovery and obsolete responses on real PG."""

import asyncio
from unittest.mock import AsyncMock

import pytest
from sqlalchemy import select, text, update
from sqlalchemy.ext.asyncio import async_sessionmaker

from app.database.models import BsTrafficCollectionBudget, BsTrafficCollectionJob, BsTrafficObservation, BsTrafficPoll
from app.services.bs_traffic.queue import cancel_job, claim_job, enqueue_job, finish_job, run_once
from tests.database.test_bs_traffic_observation_store_postgres import DAY, NODE, SCOPE, TABLES, response
from tests.fixtures.postgres_db import postgres_session


pytestmark = pytest.mark.postgres
JOB_TABLES = [*TABLES, BsTrafficCollectionJob.__table__, BsTrafficCollectionBudget.__table__]


async def enqueue(db, key='test-operation'):
    return await enqueue_job(
        db, operation_key=key, panel_scope=SCOPE, day=DAY, node_uuids=(NODE,), identities={7: 'generation-a'}
    )


async def test_enqueue_is_idempotent_but_never_overwrites_identity(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        first = await enqueue(db)
        assert await enqueue(db) == first
        with pytest.raises(ValueError):
            await enqueue_job(
                db,
                operation_key='test-operation',
                panel_scope=SCOPE,
                day=DAY,
                node_uuids=(NODE,),
                identities={7: 'different-generation'},
            )
        assert not (await db.scalars(select(BsTrafficPoll))).all()


async def test_two_workers_claim_only_one_job(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        await enqueue(db)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        barrier = asyncio.Barrier(2)

        async def claim():
            async with maker() as worker, worker.begin():
                await barrier.wait()
                return await claim_job(worker, SCOPE)

        results = await asyncio.wait_for(asyncio.gather(claim(), claim()), 10)
        assert sum(value is not None for value in results) == 1
        assert len((await db.scalars(select(BsTrafficPoll))).all()) == 1


async def test_expired_lease_gets_new_poll_and_old_result_is_discarded(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        old = await claim_job(db, SCOPE)
        await db.commit()
        await db.execute(
            update(BsTrafficCollectionJob)
            .where(BsTrafficCollectionJob.id == job_id)
            .values(lease_until=text("clock_timestamp() - interval '1 second'"))
        )
        await db.execute(
            update(BsTrafficCollectionBudget)
            .where(BsTrafficCollectionBudget.panel_scope == SCOPE)
            .values(slot_until=text("clock_timestamp() - interval '1 second'"))
        )
        new = await claim_job(db, SCOPE)
        assert new.poll_id > old.poll_id and new.token != old.token
        assert not await finish_job(db, old, response(999))
        assert await finish_job(db, new, response(100))
        assert (await db.scalars(select(BsTrafficObservation))).one().high_water_bytes == 100
        assert (await db.get(BsTrafficPoll, old.poll_id)).response_hash is None


async def test_cancel_during_http_cannot_publish_late_response(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        claim = await claim_job(db, SCOPE)
        assert await cancel_job(db, job_id, panel_scope=SCOPE)
        assert not await finish_job(db, claim, response(100))
        assert not (await db.scalars(select(BsTrafficObservation))).all()


async def test_success_is_committed_and_completed_job_cannot_run_again(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(return_value=response(100))
        assert await run_once(maker, api, SCOPE)
        assert not await run_once(maker, api, SCOPE)
        api.get_daily_nodes_usage.assert_awaited_once()
        job = await db.get(BsTrafficCollectionJob, job_id, populate_existing=True)
        assert job.state == 'complete' and job.attempts == 1
        assert job.lease_token is None


async def test_timeout_requeues_with_backoff_without_writing_zero(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(side_effect=TimeoutError)
        assert await run_once(maker, api, SCOPE)
        assert not await run_once(maker, api, SCOPE)
        job = await db.get(BsTrafficCollectionJob, job_id, populate_existing=True)
        assert job.state == 'pending' and job.last_error == 'fetch_timeout'
        assert not (await db.scalars(select(BsTrafficObservation))).all()


async def test_invalid_response_requires_attention_not_infinite_retries(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(return_value={'nodes': 'broken'})
        assert await run_once(maker, api, SCOPE)
        job = await db.get(BsTrafficCollectionJob, job_id, populate_existing=True)
        assert job.state == 'attention' and job.last_error == 'invalid_response'
        assert not (await db.scalars(select(BsTrafficObservation))).all()


async def test_wrong_panel_scope_cannot_claim_or_cancel(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        assert await claim_job(db, NODE) is None
        assert not await cancel_job(db, job_id, panel_scope=NODE)
        assert await claim_job(db, SCOPE) is not None


async def test_cancelled_worker_leaves_recoverable_lease(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(side_effect=asyncio.CancelledError)
        with pytest.raises(asyncio.CancelledError):
            await run_once(maker, api, SCOPE)
        job = await db.get(BsTrafficCollectionJob, job_id, populate_existing=True)
        assert job.state == 'leased' and job.lease_token
        assert not (await db.scalars(select(BsTrafficObservation))).all()


async def test_repeated_crashes_eventually_require_attention(postgres_database):
    from app.services.bs_traffic.queue import MAX_ATTEMPTS

    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        await db.execute(
            update(BsTrafficCollectionJob).where(BsTrafficCollectionJob.id == job_id).values(attempts=MAX_ATTEMPTS)
        )
        assert await claim_job(db, SCOPE) is None
        job = await db.get(BsTrafficCollectionJob, job_id, populate_existing=True)
        assert job.state == 'attention' and job.last_error == 'attempts_exhausted'
        assert not (await db.scalars(select(BsTrafficPoll))).all()


async def test_expired_token_cannot_finish_even_before_another_worker_claims(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        claim = await claim_job(db, SCOPE)
        await db.execute(
            update(BsTrafficCollectionJob)
            .where(BsTrafficCollectionJob.id == job_id)
            .values(lease_until=text("clock_timestamp() - interval '1 second'"))
        )
        await db.execute(
            update(BsTrafficCollectionBudget)
            .where(BsTrafficCollectionBudget.panel_scope == SCOPE)
            .values(slot_until=text("clock_timestamp() - interval '1 second'"))
        )
        assert not await finish_job(db, claim, response(100))
        assert not (await db.scalars(select(BsTrafficObservation))).all()


async def test_worker_drains_jobs_and_stops_cleanly(postgres_database):
    from app.services.bs_traffic.queue import run_worker

    async with postgres_session(postgres_database, JOB_TABLES) as db:
        await enqueue(db, 'a')
        await enqueue(db, 'b')
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        stop = asyncio.Event()
        count = 0

        async def fetch(*args, **kwargs):
            nonlocal count
            count += 1
            if count == 2:
                stop.set()
            return response(count * 100)

        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(side_effect=fetch)
        await asyncio.wait_for(run_worker(maker, api, SCOPE, stop), timeout=5)
        jobs = (await db.scalars(select(BsTrafficCollectionJob))).all()
        assert all(job.state == 'complete' for job in jobs)
        assert (await db.scalars(select(BsTrafficObservation))).one().high_water_bytes == 200


async def test_queue_migration_preserves_unfinished_work_on_downgrade(postgres_database):
    import importlib

    from alembic.migration import MigrationContext
    from alembic.operations import Operations

    migration = importlib.import_module('migrations.alembic.versions.0143_bs_traffic_collection_queue')
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        connection = await db.connection()

        def upgrade(sync):
            BsTrafficCollectionJob.__table__.drop(sync)
            BsTrafficCollectionBudget.__table__.drop(sync)
            with Operations.context(MigrationContext.configure(sync)):
                migration.upgrade()

        await connection.run_sync(upgrade)
        job_id = await enqueue(db)
        await db.commit()
        connection = await db.connection()

        def downgrade(sync):
            with Operations.context(MigrationContext.configure(sync)):
                migration.downgrade()
                migration.upgrade()

        await connection.run_sync(downgrade)
        assert (await db.get(BsTrafficCollectionJob, job_id)).state == 'pending'


@pytest.mark.parametrize(
    'status,state', [(401, 'attention'), (403, 'attention'), (429, 'pending'), (500, 'pending'), (503, 'pending')]
)
async def test_api_failure_classification(postgres_database, status, state):
    from app.external.remnawave_api import RemnaWaveAPIError

    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(side_effect=RemnaWaveAPIError('synthetic', status))
        assert await run_once(maker, api, SCOPE)
        job = await db.get(BsTrafficCollectionJob, job_id, populate_existing=True)
        assert job.state == state
        assert job.last_error == ('panel_rejected' if state == 'attention' else 'panel_unavailable')


@pytest.mark.parametrize('account_count', [10000, 50000])
async def test_large_account_batch_is_collected_as_one_job(postgres_database, account_count):
    import time

    from sqlalchemy import event

    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue_job(
            db,
            operation_key=f'batch-{account_count}',
            panel_scope=SCOPE,
            day=DAY,
            node_uuids=(NODE,),
            identities={i: f'account-{i}' for i in range(1, account_count + 1)},
        )
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(
            return_value={
                'nodes': [
                    {'uuid': NODE, 'users': [{'id': i, 'totalBytes': i * 100} for i in range(1, account_count + 1)]}
                ]
            }
        )
        writes = 0

        def count(connection, cursor, statement, parameters, context, executemany):
            nonlocal writes
            if statement.startswith('INSERT INTO bs_traffic_observations'):
                writes += 1

        event.listen(db.bind.sync_engine, 'before_cursor_execute', count)
        started = time.monotonic()
        try:
            assert await run_once(maker, api, SCOPE)
        finally:
            event.remove(db.bind.sync_engine, 'before_cursor_execute', count)
        elapsed = time.monotonic() - started
        rows = (await db.scalars(select(BsTrafficObservation))).all()
        assert len(rows) == account_count and all(row.high_water_bytes == row.panel_user_id * 100 for row in rows)
        assert writes == account_count // 250
        assert (await db.get(BsTrafficCollectionJob, job_id)).state == 'complete'
        api.get_daily_nodes_usage.assert_awaited_once()
        print(
            f'{account_count} accounts, 1 mocked HTTP, {writes} SQL upserts, {elapsed:.3f}s (local DB, not panel SLO)'
        )


async def test_distinct_jobs_share_one_panel_slot(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        await enqueue(db, 'a')
        await enqueue(db, 'b')
        first = await claim_job(db, SCOPE)
        assert first is not None
        assert await claim_job(db, SCOPE) is None
        assert await finish_job(db, first, response(100))
        assert await claim_job(db, SCOPE) is not None


async def test_retry_after_blocks_other_jobs_and_survives_new_worker(postgres_database):
    from sqlalchemy import func

    from app.external.remnawave_api import RemnaWaveTransientError

    async with postgres_session(postgres_database, JOB_TABLES) as db:
        await enqueue(db, 'limited')
        await enqueue(db, 'another')
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        error = RemnaWaveTransientError('synthetic', 429)
        error.retry_after_seconds = 600
        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(side_effect=error)
        assert await run_once(maker, api, SCOPE)
        # A different session/process must honor persisted provider cooldown.
        assert not await run_once(maker, api, SCOPE)
        api.get_daily_nodes_usage.assert_awaited_once()
        budget = await db.get(BsTrafficCollectionBudget, SCOPE)
        now = await db.scalar(select(func.clock_timestamp()))
        assert (budget.cooldown_until - now).total_seconds() > 590


async def test_health_exposes_recovery_and_cooldown_without_account_data(postgres_database):
    from app.services.bs_traffic.queue import queue_health

    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        await claim_job(db, SCOPE)
        await db.execute(
            update(BsTrafficCollectionJob)
            .where(BsTrafficCollectionJob.id == job_id)
            .values(lease_until=text("clock_timestamp() - interval '1 second'"))
        )
        await db.execute(
            update(BsTrafficCollectionBudget)
            .where(BsTrafficCollectionBudget.panel_scope == SCOPE)
            .values(slot_until=text("clock_timestamp() - interval '1 second'"))
        )
        result = await queue_health(db, SCOPE)
        assert result['states'] == {'leased': 1}
        assert result['expired_leases'] == 1
        assert 'identities' not in result and 'lease_token' not in result
        other = await queue_health(db, NODE)
        assert other['states'] == {} and other['expired_leases'] == 0


async def test_completed_jobs_release_duplicate_account_maps_but_keep_replay_protection(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db)
        claim = await claim_job(db, SCOPE)
        assert await finish_job(db, claim, response(100))
        job = await db.get(BsTrafficCollectionJob, job_id, populate_existing=True)
        poll = await db.get(BsTrafficPoll, claim.poll_id, populate_existing=True)
        assert job.identities == {} and poll.identities == {}
        assert poll.selected_accounts == 1 and poll.observed_pairs == 1
        assert await enqueue(db) == job_id
        with pytest.raises(ValueError):
            await enqueue_job(
                db,
                operation_key='test-operation',
                panel_scope=SCOPE,
                day=DAY,
                node_uuids=(NODE,),
                identities={7: 'another-generation'},
            )


async def test_two_workers_cannot_claim_distinct_jobs_of_same_panel_together(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        await enqueue(db, 'a')
        await enqueue(db, 'b')
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)
        barrier = asyncio.Barrier(2)

        async def claim():
            async with maker() as session, session.begin():
                await barrier.wait()
                return await claim_job(session, SCOPE)

        values = await asyncio.wait_for(asyncio.gather(claim(), claim()), 10)
        assert sum(value is not None for value in values) == 1


async def test_cancel_keeps_panel_slot_until_the_old_request_finishes(postgres_database):
    async with postgres_session(postgres_database, JOB_TABLES) as db:
        first_id = await enqueue(db, 'a')
        await enqueue(db, 'b')
        old = await claim_job(db, SCOPE)
        assert await cancel_job(db, first_id, panel_scope=SCOPE)
        assert await claim_job(db, SCOPE) is None
        assert not await finish_job(db, old, response(100))
        assert await claim_job(db, SCOPE) is not None


@pytest.mark.parametrize('status', [429, 503])
async def test_cancelled_http_failure_still_sets_shared_cooldown(postgres_database, status):
    from app.external.remnawave_api import RemnaWaveTransientError

    async with postgres_session(postgres_database, JOB_TABLES) as db:
        job_id = await enqueue(db, 'cancelled')
        await enqueue(db, 'next')
        await db.commit()
        maker = async_sessionmaker(db.bind, expire_on_commit=True)

        async def fetch(*args, **kwargs):
            async with maker() as control, control.begin():
                await cancel_job(control, job_id, panel_scope=SCOPE)
            error = RemnaWaveTransientError('synthetic', status)
            error.retry_after_seconds = 600
            raise error

        api = type('API', (), {})()
        api.get_daily_nodes_usage = AsyncMock(side_effect=fetch)
        assert await run_once(maker, api, SCOPE)
        assert not await run_once(maker, api, SCOPE)
        job = await db.get(BsTrafficCollectionJob, job_id, populate_existing=True)
        assert job.state == 'cancelled'
        assert not (await db.scalars(select(BsTrafficObservation))).all()
